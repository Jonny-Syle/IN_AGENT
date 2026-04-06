import os
import time
import json
import pyodbc
import logging
import requests
import unicodedata
import pandas as pd
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
# Si existe un CREW_ID2 lo agregamos dinámicamente
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

# --- 3. WHITELISTS ---
NATIVE_WHITELIST = [
    'Id', 'Id Externo', 'Id Canal', 'Canal', 'Timestamp', 'Inicio', 'Fin', 
    'Duración (s)', 'Análisis Sentimental', 'Tema general de la conversación', 
    'Fue resuelta', 'Fue solo agradecimiento', 'Herramientas Usadas', 
    'Es saliente', 'Fue abandonada', 'Contexto', 'Origen'
]

CONTEXT_WHITELIST = ['toolLogs']

TOOL_WHITELIST = [
    'URL_fetch', 'body_fetch', 'return_fetch', 'tool', 'status', 
    'timestamp', 'code_fetch', 'return_fetch.message', 'return_fetch.status', 
    'return_fetch.success', 'return_fetch.data.policy_number', 'return_fetch.data.program_name',
    'return_fetch.data.client_card', 'body_fetch.cas', 'body_fetch.cancelMotive',
    'body_fetch.idBeneficiary', 'body_fetch.scheduleDate', 'body_fetch.specialty',
    'return_fetch.response.selected_dentist', 'return_fetch.response.idDoctor',
    'return_fetch.response.Kinship', 'return_fetch.response.nameDoctor', 
    'return_fetch.response.costPIFDoctor', 'return_fetch.response.typeOfService', 
    'return_fetch.response.name_service'
]

PARA_SQL = [
    'Id', 'tool_timestamp', 'Id_Externo', 'Id_Canal', 'Canal', 'Timestamp', 
    'Inicio', 'Fin', 'Duracion_s', 'Analisis_Sentimental', 
    'Tema_general_de_la_conversacion', 'Fue_resuelta', 
    'Fue_solo_agradecimiento', 'Herramientas_Usadas', 
    'Es_saliente', 'Fue_abandonada', 'tool_URL_fetch', 'tool_body_fetch', 
    'tool_return_fetch', 'tool_tool', 'tool_status', 
    'tool_code_fetch', 'tool_return_fetch_message', 
    'tool_return_fetch_status', 'tool_return_fetch_success',
    'tool_return_fetch_data_policy_number', 
    'tool_return_fetch_data_program_name', 
    'tool_return_fetch_data_client_card',
    'tool_body_fetch_cas', 'tool_body_fetch_cancelMotive', 
    'tool_body_fetch_idBeneficiary', 'tool_body_fetch_scheduleDate', 
    'tool_body_fetch_specialty', 'tool_return_fetch_response_selected_dentist',
    'tool_return_fetch_response_idDoctor', 'tool_return_fetch_response_Kinship',
    'tool_return_fetch_response_nameDoctor', 'tool_return_fetch_response_costPIFDoctor',
    'tool_return_fetch_response_typeOfService', 'tool_return_fetch_response_name_service'
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
            time.sleep(0.5) # Pausa entre páginas

    return pd.DataFrame(all_data, columns=cols if cols else None)

# --- 6. TRANSFORMACIÓN Y LIMPIEZA ---
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

    # C. Generar Primary Key robusta y limpia
    logger.info("Generando identificadores únicos...")
    df_final['Origen'] = df_final['Origen'].fillna('DESCONOCIDO')
    df_final['Inicio'] = df_final['Inicio'].fillna('0000-00-00')
    if 'tool_tool' in df_final.columns:
        df_final['tool_tool'] = df_final['tool_tool'].fillna('SIN_HERRAMIENTA')
    else:
        df_final['tool_tool'] = 'SIN_HERRAMIENTA'

    fecha_id = df_final['Inicio'].astype(str).str[:10].replace('-', '', regex=True)
    df_final['id_registro'] = (
        df_final['Id'].astype(str) + "_" + 
        df_final['Origen'].astype(str) + "_" + 
        df_final['tool_tool'].astype(str) + "_" + 
        fecha_id
    )

    # D. Renombramientos y Pipeline SQL
    df_sql = df_final.copy()
    df_sql.columns = [limpiar_nombres(c) for c in df_sql.columns]
    whitelist_limpia = [limpiar_nombres(c) for c in PARA_SQL]

    if 'Origen' in df_sql.columns:
        df_sql['Id_Canal'] = df_sql['Origen']
        df_sql['Id'] = df_sql['id_registro']

    df_sql = df_sql[[c for c in whitelist_limpia if c in df_sql.columns]]
    total_antes = len(df_sql)
    
    # E. Drop duplicates usando la super pk
    df_sql = df_sql.drop_duplicates(subset=['Id'], keep='first') # En este punto Id = id_registro
    logger.info(f"Limpieza PK (id_registro): {total_antes} -> {len(df_sql)} filas.")

    cols_bit = ['Fue_resuelta', 'Fue_solo_agradecimiento', 'Es_saliente', 'Fue_abandonada']
    cols_num = ['Duracion_s', 'Herramientas_Usadas', 'tool_code_fetch',
                'tool_return_fetch_httpCode', 'tool_return_fetch_response_selected_dentist',
                'tool_return_fetch_response_costPIFDoctor']

    for col in df_sql.columns:
        if col in cols_bit:
            df_sql[col] = pd.to_numeric(df_sql[col], errors='coerce').fillna(0).astype(int)
        elif col in cols_num:
            df_sql[col] = pd.to_numeric(df_sql[col], errors='coerce')
            df_sql[col] = df_sql[col].astype(object).where(pd.notnull(df_sql[col]), None)
        else:
            df_sql[col] = df_sql[col].astype(str).replace(['nan', 'None', 'NaN', 'null'], None)
            df_sql[col] = df_sql[col].where(df_sql[col].notnull(), None)

    return df_sql

# --- 7. CARGA A BASE DE DATOS SQL ---
def load_to_sql(df_para_sql):
    if df_para_sql.empty:
        logger.info("No hay datos para cargar a SQL.")
        return

    conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={DB_SERVER},{DB_PORT};DATABASE={DB_NAME};UID={DB_USER};PWD={DB_PASS}"
    
    try:
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        cursor.fast_executemany = True

        cursor.execute(f"IF OBJECT_ID('tempdb..#stg_inagent') IS NOT NULL DROP TABLE #stg_inagent")
        
        cols = df_para_sql.columns.tolist()
        col_names_bracketed = ", ".join(f"[{c}]" for c in cols)
        cursor.execute(f"SELECT TOP 0 {col_names_bracketed} INTO #stg_inagent FROM {TABLE_NAME}")

        placeholders = ", ".join("?" for _ in cols)
        sql_insert = f"INSERT INTO #stg_inagent ({col_names_bracketed}) VALUES ({placeholders})"
        
        data_to_load = [tuple(x) for x in df_para_sql.values]
        
        logger.info(f"Subiendo {len(data_to_load)} registros limpios a Staging SQL...")
        cursor.executemany(sql_insert, data_to_load)

        sql_merge = f"""
        MERGE {TABLE_NAME} AS target
        USING #stg_inagent AS source
        ON (target.Id = source.Id)
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
        logger.info(f"ÉXITO TOTAL: {len(df_para_sql)} registros sincronizados correctamente en {TABLE_NAME}.")

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
    logger.info("Iniciando pipeline dinámico de InAgent (Multi-Crew)...")
    logger.info(f"Fechas configuradas: Desde {start_date} hasta {end_date}")

    # Paso 1: Extracción
    df_raw = extract_inagent_data(start_date, end_date)
    
    if not df_raw.empty:
        # Paso 2: Transformación y Limpieza
        df_prod = transform_data(df_raw)
        
        # Paso 3: Carga en SQL
        load_to_sql(df_prod)
        
        # Paso 4: Respaldo en histórico local (opcional/seguridad)
        backup_date = start_date[:10].replace("-", "") if env_start else ayer.strftime('%Y%m%d')
        df_prod.to_csv(f"notebooks/backup_inagent_{backup_date}.csv", index=False)
        logger.info(f"Respaldo generado: backup_inagent_{backup_date}.csv")
    else:
        logger.info("No se encontraron registros nuevos para procesar.")