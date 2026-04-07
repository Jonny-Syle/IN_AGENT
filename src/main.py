import os
import time
import json
import pyodbc
import logging
import requests
import unicodedata
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
from logging.handlers import RotatingFileHandler

# --- 1. CONFIGURACIÓN DE LOGS ---
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

file_handler = RotatingFileHandler(
    "inagent_sync.log", 
    maxBytes=5*1024*1024, 
    backupCount=5,
    encoding='utf-8'
)
file_handler.setFormatter(formatter)

console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)

logger.addHandler(file_handler)
logger.addHandler(console_handler)

# --- 2. CARGA DE ENTORNO ---
load_dotenv()

REQUIRED_ENV_VARS = ["INAGENT_API_KEY", "INAGENT_URL", "INAGENT_CREW_ID", "DB_SERVER", "BD"]
missing_vars = [var for var in REQUIRED_ENV_VARS if not os.getenv(var)]

if missing_vars:
    logger.critical(f"Faltan variables en el .env: {', '.join(missing_vars)}")
    raise EnvironmentError("Configuración incompleta.")

API_KEY = os.getenv("INAGENT_API_KEY")
ENDPOINT = os.getenv("INAGENT_URL")

CREW_MAPPING = {
    os.getenv("INAGENT_CREW_ID"): "MEDICA"
}
crew_id2 = os.getenv("INAGENT_CREW_ID2")
if crew_id2:
    CREW_MAPPING[crew_id2] = "OMV"
    
CREW_IDS = list(CREW_MAPPING.keys())

DB_SERVER = os.getenv('DB_SERVER')
DB_PORT = os.getenv('DB_PORT', '1433')
DB_NAME = os.getenv('BD')
DB_USER = os.getenv('DB_USER')
DB_PASS = os.getenv('DB_PASS')

TABLE_NAME = "dbo.inagent"

logger.info("Entorno cargado y sistema de logs listo.")

# --- 3. WHITELISTS EXACTAS DE LAB.IPYNB ---
NATIVE_WHITELIST = [
    'Id', 'Id Externo', 'Id Canal', 'Canal', 'Timestamp', 'Inicio', 'Fin', 
    'Duración (s)', 'Análisis Sentimental', 'Tema general de la conversación', 
    'Fue resuelta', 'Fue solo agradecimiento', 'Herramientas Usadas', 
    'Es saliente', 'Fue abandonada', 'Contexto', 'Origen'
]

CONTEXT_WHITELIST = [
    'toolLogs', 'tarjeta', 
    'proxyData_sip_attributes_sip_trunkPhoneNumber',
    'proxyData_sip_attributes_sip_h_x-tarjeta-id'
]

TOOL_WHITELIST = [
    'URL_fetch', 'body_fetch', 'return_fetch', 'tool', 'status', 
    'timestamp', 'code_fetch','return_fetch.msg',
    'return_fetch.message','return_fetch.data.client_complete_name',
    'return_fetch.data.policy_number','return_fetch.data.program_name',
    'return_fetch.data.program_status','return_fetch.data.client_account',
    'return_fetch.data.client_telefono','return_fetch.success',
    'return_fetch.data.client_card','body_fetch.cas','body_fetch.cancelMotive',
    'return_fetch.response.cas_folio','body_fetch.scheduleDate',
    'body_fetch.specialty','return_fetch.response.nameDoctor',
    'return_fetch.response.name_service','return_fetch.response.creationDate',
    'return_fetch.response.title','return_fetch.response.provider',
    'return_fetch.response.Kinship','return_fetch.response.category'
]

PARA_SQL = [
    'Id', 'Id_Externo', 'Id_Canal', 'Canal', 'Timestamp', 'Inicio', 'Fin', 
    'Duracion_s', 'Analisis_Sentimental', 'Tema_general_de_la_conversacion', 
    'Fue_resuelta', 'Fue_solo_agradecimiento', 'Herramientas_Usadas', 
    'Es_saliente', 'Fue_abandonada', 'Origen', 'ctx_tarjeta', 
    'ctx_proxyData_sip_attributes_sip_trunkPhoneNumber', 
    'ctx_proxyData_sip_attributes_sip_h_x_tarjeta_id', 
    'tool_URL_fetch', 'tool_body_fetch', 'tool_return_fetch', 'tool_tool', 
    'tool_status', 'tool_timestamp', 'tool_code_fetch', 'tool_return_fetch_msg', 
    'tool_return_fetch_message', 'tool_return_fetch_data_client_complete_name', 
    'tool_return_fetch_data_policy_number', 'tool_return_fetch_data_program_name', 
    'tool_return_fetch_data_program_status', 'tool_return_fetch_data_client_account', 
    'tool_return_fetch_data_client_telefono', 'tool_return_fetch_success', 
    'tool_return_fetch_data_client_card', 'tool_body_fetch_cas', 
    'tool_body_fetch_cancelMotive', 'tool_return_fetch_response_cas_folio', 
    'tool_body_fetch_scheduleDate', 'tool_body_fetch_specialty', 
    'tool_return_fetch_response_nameDoctor', 'tool_return_fetch_response_name_service', 
    'tool_return_fetch_response_provider', 'tool_return_fetch_response_Kinship', 
    'tool_return_fetch_response_category', 'tool_timestamp_dt', 
    'interaccion_unica', 'Estatus_Final', 'id_registro'
]

# --- 4. FUNCIONES AUXILIARES ---
def to_unix_ms(iso_date):
    dt = datetime.fromisoformat(iso_date).replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)

def safe_json_parse(val):
    try:
        if isinstance(val, (dict, list)): return val
        return json.loads(val) if (val and val != 'null') else {}
    except:
        return {}

def limpiar_nombres(txt):
    if not isinstance(txt, str): return txt
    txt = "".join(c for c in unicodedata.normalize('NFD', txt) if unicodedata.category(c) != 'Mn')
    return txt.replace(" ", "_").replace("(", "").replace(")", "").replace(".", "_")

# --- 5. EXTRACCIÓN CON BACKOFF Y MULTI-CREW ---
def extract_inagent_data(start_iso, end_iso):
    all_data = []
    page_size = 100
    max_retries = 5
    cols = []

    logger.info(f"Iniciando extracción desde {start_iso} hasta {end_iso}")

    for crew in CREW_IDS:
        page = 0
        label_origen = CREW_MAPPING.get(crew, "DESCONOCIDO")
        logger.info(f"Extrayendo datos corporativos de {label_origen} (ID: {crew})")
        
        while True:
            params = {
                "crew_id": crew,
                "start_ts": to_unix_ms(start_iso),
                "end_ts": to_unix_ms(end_iso),
                "page": page,
                "pageSize": page_size
            }
            headers = {"apikey": API_KEY}
            
            retry_count = 0
            success = False
            while retry_count < max_retries:
                try:
                    res = requests.get(ENDPOINT, headers=headers, params=params, timeout=30)
                    if res.status_code == 200:
                        success = True
                        break
                    elif res.status_code == 429:
                        retry_count += 1
                        wait_time = 2 ** retry_count
                        logger.warning(f"Too many requests ({label_origen} - Página {page}). Reintentando en {wait_time}s...")
                        time.sleep(wait_time)
                    else:
                        logger.error(f"Error {res.status_code} en {label_origen} pág {page}: {res.text}")
                        break
                except Exception as e:
                    logger.error(f"Error de conexión en {label_origen} pág {page}: {e}")
                    break
                    
            if not success:
                logger.error(f"Agotados reintentos para {label_origen} pág {page}. Se detiene extracción de esta crew.")
                break
                
            payload = res.json().get("data", {})
            rows = payload.get("rows", [])
            
            if page == 0 and crew == CREW_IDS[0]:
                cols = payload.get("dataSchema", {}).get("columnNames", [])
                if "Origen" not in cols:
                    cols.append("Origen")

            if not rows:
                break
            
            for row in rows:
                row.append(label_origen)

            all_data.extend(rows)
            logger.info(f"Página {page} de {label_origen} procesada. Total acumulado: {len(all_data)}")

            if len(rows) < page_size:
                break
            
            page += 1
            time.sleep(0.5) 

    return pd.DataFrame(all_data, columns=cols if cols else None)

# --- 6. TRANSFORMACIÓN Y LIMPIEZA MODO LAB ---
def aplicar_ancla_maestra(df): 
    df['tool_timestamp_dt'] = pd.to_datetime(df['tool_timestamp'], errors='coerce')
    df = df.sort_values(by=['Id', 'tool_timestamp_dt'], ascending=[True, False])
    
    es_el_ancla = ~df.duplicated(subset=['Id'], keep='first')
    
    df['interaccion_unica'] = np.where(es_el_ancla, 1, 0)
    df['Estatus_Final'] = np.where(es_el_ancla, df['tool_tool'], None)
    
    return df

def crear_id_compuesto_pro(df):
    logger.info(" Generando identificadores únicos (id_registro)...")
    df['tool_tool'] = df['tool_tool'].fillna('SIN_HERRAMIENTA')
    tool = df['tool_timestamp'].astype(str)
    
    df['id_registro'] = (
        df['Id'].astype(str) + "_" + 
        df['tool_tool'].astype(str) + "_" + 
        tool
    )
    return df

def pipeline_maestro_final(df, whitelist):
    df_sql = df.copy()
    df_sql.columns = [limpiar_nombres(c) for c in df_sql.columns]
    whitelist_limpia = [limpiar_nombres(c) for c in whitelist]

    if 'Origen' in df_sql.columns:
        df_sql['Id_Canal'] = df_sql['Origen']
        df_sql['Id_Externo'] = df_sql['id_registro'] # PK técnica de fila

        # Mapeos especiales solicitados en Lab
        if 'interaccion_unica' in df_sql.columns:
            df_sql['tool_return_fetch_response_costPIFDoctor'] = df_sql['interaccion_unica']
        
        if 'Estatus_Final' in df_sql.columns:
            df_sql['tool_return_fetch_response_typeOfService'] = df_sql['Estatus_Final']

    df_sql = df_sql[[c for c in whitelist_limpia if c in df_sql.columns]]

    cols_num = [
        'Duracion_s', 'Herramientas_Usadas', 'tool_code_fetch',
        'tool_return_fetch_httpCode', 'tool_return_fetch_response_costPIFDoctor',
        'tool_return_fetch_response_selected_dentist'
    ]
    cols_date = ['Inicio', 'Fin', 'tool_timestamp_dt']

    for col in df_sql.columns:
        if col in cols_date:
            df_sql[col] = pd.to_datetime(df_sql[col], errors='coerce')
            df_sql[col] = df_sql[col].astype(object).where(pd.notnull(df_sql[col]), None)
        elif col in cols_num:
            df_sql[col] = pd.to_numeric(df_sql[col], errors='coerce')
            df_sql[col] = df_sql[col].astype(object).where(pd.notnull(df_sql[col]), None)
        elif any(x in col for x in ['Fue_', 'Es_', 'Cerrada_']):
            df_sql[col] = pd.to_numeric(df_sql[col], errors='coerce').fillna(0).astype(int)
        else:
            df_sql[col] = df_sql[col].astype(str).replace(['n.n','nan', 'None', 'NaN', 'null'], None)
            df_sql[col] = df_sql[col].where(df_sql[col].notnull(), None)

    return df_sql

def transform_data(df_raw):
    if df_raw.empty: return pd.DataFrame()
    logger.info("Iniciando transformaciones de datos...")

    # A. Extraer context base
    df_native = df_raw[[c for c in NATIVE_WHITELIST if c in df_raw.columns]].copy()
    
    ctx_raw = pd.json_normalize(df_raw['Contexto'].apply(safe_json_parse))
    ctx_raw.columns = [c.replace(".", "_") for c in ctx_raw.columns]
    ctx_selected = ctx_raw[[c for c in CONTEXT_WHITELIST if c in ctx_raw.columns]].add_prefix('ctx_')
    
    df_base = pd.concat([df_native, ctx_selected], axis=1)

    # B. Explotar ToolLogs
    col_logs = 'ctx_toolLogs'
    if col_logs in df_base.columns:
        df_tools_exploded = df_base[['Id', col_logs]].dropna(subset=[col_logs]).explode(col_logs)
        tool_rows = df_tools_exploded[col_logs].apply(lambda x: x if isinstance(x, (dict, list)) else safe_json_parse(x)).tolist()
        df_tools_flat = pd.json_normalize(tool_rows)
        
        cols_t = [c for c in TOOL_WHITELIST if c in df_tools_flat.columns]
        df_tools_final = df_tools_flat[cols_t].copy()
        df_tools_final.columns = [f"tool_{c.replace('.', '_')}" for c in df_tools_final.columns]
        df_tools_final.index = df_tools_exploded.index
        df_tools_merged = pd.concat([df_tools_exploded[['Id']], df_tools_final], axis=1)
        
        df_final = df_base.merge(df_tools_merged, on='Id', how='left')
    else:
        df_final = df_base
        
    df_final.columns = [c.replace(" ", "_").replace("(", "").replace(")", "").replace(".", "_") for c in df_final.columns]
    
    # C. Limpieza, Anclas y Renombramiento Final
    df_preparado = aplicar_ancla_maestra(df_final)
    df_preparado = crear_id_compuesto_pro(df_preparado)
    df_listo = pipeline_maestro_final(df_preparado, PARA_SQL)
    
    if 'ctx_proxyData_sip_attributes_sip_h_x-tarjeta-id' in df_listo.columns:
        # Arreglo manual del guión como en el lab
        df_listo.rename(columns={
            'ctx_proxyData_sip_attributes_sip_h_x-tarjeta-id': 'ctx_proxyData_sip_attributes_sip_h_x_tarjeta_id'
        }, inplace=True)
        
    df_produccion = df_listo[[c for c in PARA_SQL if c in df_listo.columns]].copy()
    df_produccion = df_produccion.loc[:, ~df_produccion.columns.duplicated()].copy()
    
    return df_produccion

# --- 7. CARGA A BASE DE DATOS SQL (CON EVASIÓN DE DUPLICADOS) ---
def load_to_sql(df_para_sql):
    if df_para_sql.empty:
        logger.info("No hay datos extraídos de la API para procesar.")
        return

    conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={DB_SERVER},{DB_PORT};DATABASE={DB_NAME};UID={DB_USER};PWD={DB_PASS}"
    
    try:
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        
        # --- VERIFICACIÓN DE NUEVOS REGISTROS ---
        ids_externos_lote = tuple(df_para_sql['Id_Externo'].dropna().unique().tolist())
        existentes = set()
        
        if len(ids_externos_lote) > 0:
            logger.info("Consultando registros pre-existentes en SQL para evitar ejecución innecesaria...")
            chunk_size = 1000
            for i in range(0, len(ids_externos_lote), chunk_size):
                chunk = ids_externos_lote[i:i+chunk_size]
                placeholders = ",".join(["?"] * len(chunk))
                query = f"SELECT Id_Externo FROM {TABLE_NAME} WHERE Id_Externo IN ({placeholders})"
                cursor.execute(query, chunk)
                existentes.update([row[0] for row in cursor.fetchall()])
        
        # Filtramos dejando solo lo que no está en la Base de Datos
        df_nuevos = df_para_sql[~df_para_sql['Id_Externo'].isin(existentes)].copy()
        
        if df_nuevos.empty:
            logger.info(f"Todos los registros de este periodo ya están procesados en SQL ({len(df_para_sql)} filas). No hay nuevos registros por insertar.")
            return
            
        logger.info(f"De {len(df_para_sql)} filas extraídas, {len(df_nuevos)} son estrictamente nuevas (Las demás fueron ignoradas).")
        
        # --- INGESTA SQL ---
        cursor.fast_executemany = True
        cursor.execute(f"IF OBJECT_ID('tempdb..#stg_inagent') IS NOT NULL DROP TABLE #stg_inagent")
        
        cols = df_nuevos.columns.tolist()
        col_names_bracketed = ", ".join(f"[{c}]" for c in cols)
        cursor.execute(f"SELECT TOP 0 {col_names_bracketed} INTO #stg_inagent FROM {TABLE_NAME}")

        plcholders = ", ".join("?" for _ in cols)
        sql_insert = f"INSERT INTO #stg_inagent ({col_names_bracketed}) VALUES ({plcholders})"
        
        data_to_load = [tuple(x) for x in df_nuevos.values]
        
        logger.info(f"Subiendo {len(data_to_load)} registros a Staging DB...")
        cursor.executemany(sql_insert, data_to_load)

        sql_merge = f"""
        MERGE {TABLE_NAME} AS target
        USING #stg_inagent AS source
        ON (target.Id_Externo = source.Id_Externo)
        WHEN MATCHED THEN
            UPDATE SET 
                target.Analisis_Sentimental = source.Analisis_Sentimental,
                target.Tema_general_de_la_conversacion = source.Tema_general_de_la_conversacion,
                target.tool_status = source.tool_status,
                target.tool_return_fetch_message = source.tool_return_fetch_message
        WHEN NOT MATCHED THEN
            INSERT ({col_names_bracketed})
            VALUES ({', '.join(f'source.[{c}]' for c in cols)});
        """
        
        logger.info("Ejecutando MERGE en tabla definitiva...")
        cursor.execute(sql_merge)
        conn.commit()
        logger.info(f"ÉXITO: {len(df_nuevos)} registros sincronizados correctamente en {TABLE_NAME}.")

    except Exception as e:
        if 'conn' in locals(): conn.rollback()
        logger.error(f"Error en SQL: {e}")
    finally:
        if 'cursor' in locals(): cursor.close()
        if 'conn' in locals(): conn.close()


# --- 8. EJECUCIÓN DEL PIPELINE (MAIN) ---
if __name__ == "__main__":
    env_start = os.getenv("START_DATE")
    env_end = os.getenv("END_DATE")
    
    hoy = datetime.now()
    ayer = hoy - timedelta(days=1)
    
    if env_start:
        start_date = env_start
    else:
        start_date = ayer.strftime("%Y-%m-%dT00:00:00")
        
    if env_end:
        end_date = env_end
    else:
        end_date = ayer.strftime("%Y-%m-%dT23:59:59")
        
    logger.info("="*50)
    logger.info("Iniciando pipeline dinámico de InAgent (Precisión Refactorizada)...")
    logger.info(f"Fechas configuradas: Desde {start_date} hasta {end_date}")

    # Paso 1: Extracción
    df_raw = extract_inagent_data(start_date, end_date)
    
    if not df_raw.empty:
        # Paso 2: Transformación (Equivalentes a `Lab.ipynb`)
        df_prod = transform_data(df_raw)
        
        # Paso 3: Carga en base de datos con chequeo de nuevos registros
        load_to_sql(df_prod)
        
        # Paso 4: Backup de seguridad
        backup_date = start_date[:10].replace("-", "") if env_start else ayer.strftime('%Y%m%d')
        df_prod.to_csv(f"notebooks/backup_inagent_{backup_date}.csv", index=False)
        logger.info(f"Respaldo generado en notebooks/backup_inagent_{backup_date}.csv")
    else:
        logger.info("El sistema de INAGENT devolvió 0 interacciones para el periodo.")