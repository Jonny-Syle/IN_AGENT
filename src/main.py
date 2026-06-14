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
from zoneinfo import ZoneInfo
from dotenv import load_dotenv
from logging.handlers import RotatingFileHandler

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

load_dotenv()

REQUIRED_ENV_VARS = ["INAGENT_API_KEY", "INAGENT_URL", "INAGENT_CREW_MAPPING", "DB_SERVER", "BD"]
missing_vars = [var for var in REQUIRED_ENV_VARS if not os.getenv(var)]

if missing_vars:
    logger.critical(f"Faltan variables en el .env: {', '.join(missing_vars)}")
    raise EnvironmentError("Configuración incompleta.")

API_KEY = os.getenv("INAGENT_API_KEY")
ENDPOINT = os.getenv("INAGENT_URL")

try:
    CREW_MAPPING = json.loads(os.getenv("INAGENT_CREW_MAPPING"))
except json.JSONDecodeError:
    logger.critical("La variable INAGENT_CREW_MAPPING no tiene un formato JSON válido.")
    raise ValueError("Configuración de CREW_MAPPING inválida.")

CREW_IDS = list(CREW_MAPPING.keys())

DB_SERVER = os.getenv('DB_SERVER')
DB_PORT = os.getenv('DB_PORT', '1433')
DB_NAME = os.getenv('BD')
DB_USER = os.getenv('DB_USER')
DB_PASS = os.getenv('DB_PASS')

TABLE_NAME = "dbo.inagent"

logger.info("Entorno cargado y sistema de logs listo.")

NATIVE_WHITELIST = [
    'Id', 'Id Externo', 'Id Canal', 'Canal', 'Timestamp', 'Inicio', 'Fin', 
    'Duración (s)', 'Análisis Sentimental', 'Tema general de la conversación', 
    'Fue resuelta', 'Fue solo agradecimiento', 'Herramientas Usadas', 
    'Es saliente', 'Fue abandonada', 'Atención del Agente Virtual (s)', 'Contexto', 
    'Resumen', 'Motivo de Cierre', 'Origen'
]

CONTEXT_WHITELIST = [
    'toolLogs', 'tarjeta', 
    'proxyData_sip_attributes_sip_trunkPhoneNumber',
    'proxyData_sip_attributes_sip_h_x-tarjeta-id',
    'comb_resume'
]

TOOL_WHITELIST = [
    'URL_fetch', 'body_fetch', 'return_fetch', 'tool', 'status', 
    'timestamp', 'code_fetch','return_fetch.msg',
    'return_fetch.message','return_fetch.data.client_complete_name',
    'return_fetch.data.policy_number','return_fetch.data.program_name',
    'return_fetch.data.program_status','return_fetch.data.client_account',
    'return_fetch.data.client_telefono','return_fetch.success',
    'return_fetch.data.client_card','body_fetch.cas','body_fetch.cancelMotive',
    'return_fetch.response.cas_folio','return_fetch.cas_folio','body_fetch.scheduleDate',
    'body_fetch.specialty','return_fetch.response.nameDoctor',
    'return_fetch.response.name_service','return_fetch.response.creationDate',
    'return_fetch.response.title','return_fetch.response.provider',
    'return_fetch.response.Kinship','return_fetch.response.category',
    'return_fetch_response.cas_folio'
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
    'interaccion_unica', 'Estatus_Final',
    'tool_return_fetch_response_costPIFDoctor', 'tool_return_fetch_response_typeOfService',
    'cas', 'ctx_comb_resume'
]

cdmx_tz = ZoneInfo("America/Mexico_City")

def to_unix_ms(iso_date):
    dt = datetime.fromisoformat(iso_date.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=cdmx_tz)
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

def cas_inteligente_maestro(df_base, col_logs='ctx_toolLogs'):
    df = df_base.copy()

    cols_prioridad = [
        'tool_return_fetch_cas_folio', 
        'tool_body_fetch_cas', 
        'tool_return_fetch_response_cas_folio'
    ]
    
    for col in cols_prioridad:
        if col not in df.columns:
            df[col] = np.nan
        else:
            df[col] = df[col].astype(str).replace(['n.n', 'nan', 'None', 'NaN', 'null', ''], np.nan)

    condiciones_base = [
        df['tool_return_fetch_cas_folio'].astype(str).str.contains('cas', case=False, na=False),
        df['tool_body_fetch_cas'].notna(),
        df['tool_return_fetch_response_cas_folio'].notna(),
        df['tool_return_fetch_cas_folio'].notna()
    ]
    
    valores_base = [
        df['tool_return_fetch_cas_folio'],
        df['tool_body_fetch_cas'],
        df['tool_return_fetch_response_cas_folio'],
        df['tool_return_fetch_cas_folio']
    ]

    df['cas_fase1'] = np.select(condiciones_base, valores_base, default=np.nan)
    df['cas_fase1'] = df['cas_fase1'].astype(str).str.upper().replace(['NAN', 'NONE', 'NULL', ''], np.nan)

    mask_huerfanos = df['cas_fase1'].isna()
    df['cas_fase2'] = np.nan # Inicializamos
    
    if mask_huerfanos.any() and col_logs in df.columns:
        df_rescate = df.loc[mask_huerfanos, ['Id', col_logs]].dropna(subset=[col_logs])
        
        if not df_rescate.empty:
            df_tools_exploded = df_rescate.explode(col_logs)
            
            tool_rows = df_tools_exploded[col_logs].apply(
                lambda x: x if isinstance(x, dict) else safe_json_parse(x)
            ).tolist()
            
            df_tools_flat = pd.json_normalize(tool_rows)
            df_tools_flat.index = df_tools_exploded.index
            df_tools_flat['Id'] = df_tools_exploded['Id']

            if 'tool' in df_tools_flat.columns:
                tools_objetivo = ['set_checkup', 'set_checkup_OMV', 'cancelar_cita', 'cancelar_cita_OMV']
                df_target = df_tools_flat[df_tools_flat['tool'].isin(tools_objetivo)].copy()
                
                rutas_cas = [
                    'return_fetch.cas_folio',
                    'body_fetch.cas',
                    'return_fetch.response.cas_folio',
                    'return_fetch.data.cas_folio'
                ]

                cols_existentes = [c for c in rutas_cas if c in df_target.columns]
                
                if cols_existentes:
                    df_target['cas_rescatado'] = df_target[cols_existentes].bfill(axis=1).iloc[:, 0]
                    df_target['cas_rescatado'] = df_target['cas_rescatado'].astype(str).str.upper().replace(['NAN', 'NONE', 'NULL', ''], np.nan)
                    
                    df_validos = df_target.dropna(subset=['cas_rescatado'])
                    
                    if not df_validos.empty:
                        df_cas_unico = df_validos.drop_duplicates(subset=['Id'], keep='last')[['Id', 'cas_rescatado']]
                        
                        df = df.merge(df_cas_unico, on='Id', how='left')
                        df['cas_fase2'] = df['cas_rescatado']
                        df = df.drop(columns=['cas_rescatado'])

    df['cas_final'] = df['cas_fase1'].fillna(df['cas_fase2'])
    df['cas'] = df['cas_final'].fillna('SIN FOLIO CAS')
    df = df.drop(columns=['cas_fase1', 'cas_fase2', 'cas_final'])

    return df

def aplicar_ancla_maestra(df):
    df = df.copy()
    df['tool_timestamp_dt'] = pd.to_datetime(
        df['tool_timestamp'], 
        format='mixed', 
        errors='coerce'
    )
    df = df.sort_values(by=['Id', 'tool_timestamp_dt'], ascending=[True, False])
    es_el_ancla = ~df.duplicated(subset=['Id'], keep='first')
    df['interaccion_unica'] = es_el_ancla.astype(np.int8)
    df['Estatus_Final'] = df['tool_tool'].where(es_el_ancla, pd.NA)
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

def aplicar_taxonomia_eventos(df, columna):
    mapa_regex = {
        r'(?i)transferencia|comunicaci[oó]n asesor': 'Transferencia a Asesor',
        r'(?i)cancelaci[oó]n|cancelar': 'Cancelación',
        r'(?i)reagenda|cambio|correcci[oó]n|apellido error': 'Modificación / Reagenda',
        r'(?i)check[- ]?up|chequeo': 'Consulta Checkup',
        r'(?i)agend|cita|appointment|horario|confirmaci[oó]n': 'Agendamiento / Cita',
        r'(?i)seguro|p[oó]liza|beneficio|privilegio|descuento|programa|peep|pif': 'Seguros y Beneficios',
        r'(?i)validaci[oó]n|verificaci[oó]n|folio|orden|correo|tarjeta|qr|identidad|vigencia': 'Gestión Administrativa',
        r'(?i)dental|m[eé]dic|nutrici[oó]n|psicolog|laboratorio|ambulancia|salud|gr[uú]a|mec[áa]nica': 'Derivación Especialidad / Proveedor',
        r'(?i)asistencia|atenci[oó]n|servicio|orientaci[oó]n': 'Asistencia General'
    }
    condiciones = [df[columna].str.contains(patron, na=False) for patron in mapa_regex.keys()]
    resultados = list(mapa_regex.values())
    df['Evento_Normalizado'] = np.select(condiciones, resultados, default='Sin Clasificar')
    return df


def pipeline_maestro_final(df, whitelist):
    df_sql = df.copy()
    
    def limpiar_nombres(txt):
        if not isinstance(txt, str): return txt
        txt = "".join(c for c in unicodedata.normalize('NFD', txt) if unicodedata.category(c) != 'Mn')
        return txt.replace(" ", "_").replace("(", "").replace(")", "").replace(".", "_")

    df_sql.columns = [limpiar_nombres(c) for c in df_sql.columns]
    whitelist_limpia = [limpiar_nombres(c) for c in whitelist]        
    
    if 'cas' not in whitelist_limpia:
        whitelist_limpia.append('cas')

    df_sql = df_sql[[c for c in whitelist_limpia if c in df_sql.columns]]

    cols_num = [
        'Timestamp', 'tool_timestamp', 'Duracion_s', 'Herramientas_Usadas', 'tool_code_fetch',
        'tool_return_fetch_httpCode', 'tool_return_fetch_response_costPIFDoctor',
        'tool_return_fetch_response_selected_dentist','tool_body_fetch_scheduleDate'
    ]
    
    cols_date = ['Inicio', 'Fin', 'tool_timestamp_dt']

    for col in df_sql.columns:
        if col in cols_date:
            df_sql[col] = pd.to_datetime(df_sql[col], errors='coerce')
            df_sql[col] = df_sql[col].astype(object).where(pd.notnull(df_sql[col]), None)
            
        elif col in cols_num:
            df_sql[col] = pd.to_numeric(df_sql[col], errors='coerce')
            if col in ['Timestamp', 'tool_timestamp']:
                df_sql[col] = df_sql[col].fillna(0)
            df_sql[col] = df_sql[col].astype(object).where(pd.notnull(df_sql[col]), None)
            
        elif any(x in col for x in ['Fue_', 'Es_', 'Cerrada_']):
            df_sql[col] = pd.to_numeric(df_sql[col], errors='coerce').fillna(0).astype(int)
            if col in ['Timestamp', 'tool_timestamp']:
                df_sql[col] = df_sql[col].fillna(0)
            
        else:
            df_sql[col] = df_sql[col].astype(str).replace(['n.n','nan', 'None', 'NaN', 'null', ''], None)
            df_sql[col] = df_sql[col].where(df_sql[col].notnull(), None)
            
            if col in ['tool_return_fetch_response_nameDoctor', 'tool_return_fetch_response_name_service', 'tool_return_fetch_data_client_complete_name', 'ctx_proxyData_roomName', 'tool_body_fetch', 'tool_return_fetch', 'ctx_comb_resume', 'tool_URL_fetch']:
                df_sql[col] = df_sql[col].apply(lambda x: x[:255] if isinstance(x, str) else x)
            elif col in ['tool_return_fetch_data_program_name']:
                df_sql[col] = df_sql[col].apply(lambda x: x[:150] if isinstance(x, str) else x)
            elif col in ['Estatus_Final', 'Id_Externo', 'Id', 'Id_Canal', 'ctx_tarjeta', 'tool_tool', 'cas',
            'tool_return_fetch_data_program_status', 'tool_return_fetch_data_client_account',
            'tool_return_fetch_data_client_card',
            'tool_body_fetch_cas', 'tool_return_fetch_data_policy_number',
            'tool_body_fetch_scheduleDate', 'tool_body_fetch_specialty',
            'tool_return_fetch_response_Kinship', 'tool_return_fetch_response_typeOfService',
            'ctx_proxyData_sip_attributes_sip_trunkPhoneNumber', 'ctx_proxyData_sip_attributes_sip_h_x_tarjeta_id',
            'tool_return_fetch_response_cas_folio', 'tool_return_fetch_response_provider', 'tool_return_fetch_response_category']:
                df_sql[col] = df_sql[col].apply(lambda x: x[:100] if isinstance(x, str) else x)
            elif col in ['Origen', 'Canal', 'tool_status', 'tool_return_fetch_status', 'tool_return_fetch_data_client_telefono']:
                df_sql[col] = df_sql[col].apply(lambda x: x[:50] if isinstance(x, str) else x)
            elif col in ['tool_return_fetch_success']:
                df_sql[col] = df_sql[col].apply(lambda x: x[:20] if isinstance(x, str) else x)

    return df_sql

def transform_data(df_raw):
    if df_raw.empty: return pd.DataFrame()
    logger.info("Iniciando transformaciones de datos...")

    df_native = df_raw[[c for c in NATIVE_WHITELIST if c in df_raw.columns]].copy()
    
    ctx_raw = pd.json_normalize(df_raw['Contexto'].apply(safe_json_parse))
    ctx_raw.columns = [c.replace(".", "_") for c in ctx_raw.columns]
    
    ctx_selected = ctx_raw[[c for c in CONTEXT_WHITELIST if c in ctx_raw.columns]].add_prefix('ctx_')
    
    df_base = pd.concat([df_native, ctx_selected], axis=1)

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
    
    df_preparado = aplicar_ancla_maestra(df_final)
    df_preparado = crear_id_compuesto_pro(df_preparado)
    
    # CAS inteligente maestro
    df_listo = cas_inteligente_maestro(df_preparado, col_logs='ctx_toolLogs')
    
    # Evento categorization
    condiciones = [
        df_listo['tool_tool'].str.contains(r'(?i)^cerrar', na=False),
        df_listo['tool_tool'].str.contains(r'(?i)^transferencia', na=False),
        df_listo['tool_tool'].str.contains(r'(?i)whatsapp', na=False),
        df_listo['tool_tool'].str.contains(r'(?i)^get_', na=False),
        df_listo['tool_tool'].str.contains(r'(?i)^set_', na=False),
        df_listo['tool_tool'].str.contains(r'(?i)^upsert_', na=False),
        df_listo['tool_tool'].str.contains(r'(?i)cancelar', na=False),
        df_listo['tool_tool'].str.contains(r'(?i)sesi[oó]n', na=False),
        df_listo['tool_tool'].str.contains(r'(?i)herramienta', na=False),
    ]

    resultados = [
        'Cerrar sesion', 
        'Transferencia a Asesor',
        'Interacción WhatsApp',
        'Consulta de Datos',
        "Programar cita",
        "Modificar datos",
        'Cancelación',
        'Gestión de Sesión',
        'Sin evento'
    ]

    df_listo['Evento'] = np.select(condiciones, resultados, default='Sin evento')

    df_listo = aplicar_taxonomia_eventos(df_listo, 'Tema_general_de_la_conversación')
    
    # Mapeo personalizado / Asignaciones
    df_listo['Id_Canal'] = df_listo['Origen']
    df_listo['Id_Externo'] = df_listo['id_registro'] 
    df_listo['ctx_comb_resume'] = df_listo['tool_return_fetch_message']
    df_listo['tool_return_fetch_response_nameDoctor'] = df_listo['tool_body_fetch_cas']
    df_listo['tool_return_fetch_response_title'] = df_listo['tool_body_fetch_cancelMotive']
    df_listo['tool_return_fetch_response_typeOfService'] = df_listo['tool_body_fetch']
    df_listo['tool_body_fetch_scheduleDate'] = df_listo['tool_return_fetch_cas_folio']
    df_listo['Tema_general_de_la_conversacion'] = df_listo['cas']
    df_listo['tool_return_fetch_response_costPIFDoctor'] = df_listo['interaccion_unica']

    df_listo = pipeline_maestro_final(df_listo, PARA_SQL)

    if 'tool_timestamp' in df_listo.columns and 'Timestamp' in df_listo.columns:
        df_listo['tool_timestamp'] = df_listo['tool_timestamp'].fillna(df_listo['Timestamp'])
    
    if 'ctx_proxyData_sip_attributes_sip_h_x-tarjeta-id' in df_listo.columns:
        df_listo.rename(columns={
            'ctx_proxyData_sip_attributes_sip_h_x-tarjeta-id': 'ctx_proxyData_sip_attributes_sip_h_x_tarjeta_id'
        }, inplace=True)
        
    df_produccion = df_listo[[c for c in PARA_SQL if c in df_listo.columns]].copy()
    df_produccion = df_produccion.loc[:, ~df_produccion.columns.duplicated()].copy()
    
    return df_produccion

def load_to_sql(df_para_sql):
    if df_para_sql.empty:
        logger.info("No hay datos extraídos de la API para procesar.")
        return

    conn_str = f"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={DB_SERVER},{DB_PORT};DATABASE={DB_NAME};UID={DB_USER};PWD={DB_PASS};TrustServerCertificate=yes"
    
    try:
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        cursor.fast_executemany = True

        # 1. Asegurar desduplicación de columnas en el DataFrame de carga
        df_para_sql = df_para_sql.loc[:, ~df_para_sql.columns.duplicated()].copy()
        if 'id_registro' in df_para_sql.columns:
            df_para_sql = df_para_sql.drop(columns=['id_registro'])

        # 2. Eliminar duplicados físicos en Id_Externo
        df_para_sql = df_para_sql.drop_duplicates(subset=['Id_Externo']).copy()
        
        # 3. Truncar columnas problemáticas de texto para ajustarlas al esquema SQL Server
        cols_to_trim_255 = ['tool_return_fetch_response_nameDoctor', 'tool_return_fetch_response_name_service', 
                            'tool_return_fetch_data_client_complete_name', 'ctx_proxyData_roomName', 
                            'tool_body_fetch', 'tool_return_fetch', 'ctx_comb_resume', 'tool_URL_fetch']
        for c in cols_to_trim_255:
            if c in df_para_sql.columns:
                df_para_sql[c] = df_para_sql[c].astype(str).str.slice(0, 255)
                df_para_sql[c] = df_para_sql[c].replace(['None', 'nan', 'NaN', 'null'], None)

        # 4. Crear tabla temporal de Staging
        cols = df_para_sql.columns.tolist()
        col_names_bracketed = ", ".join(f"[{c}]" for c in cols)
        
        cursor.execute(f"IF OBJECT_ID('tempdb..#stg_inagent') IS NOT NULL DROP TABLE #stg_inagent")
        cursor.execute(f"SELECT TOP 0 {col_names_bracketed} INTO #stg_inagent FROM {TABLE_NAME}") 

        placeholders = ", ".join("?" for _ in cols)
        sql_insert = f"INSERT INTO #stg_inagent ({col_names_bracketed}) VALUES ({placeholders})"

        # 5. Obtener los tipos de columnas del esquema de SQL Server para setinputsizes
        sql_types = {}
        try:
            cursor.execute(f"""
                SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_NAME = '{TABLE_NAME.replace('dbo.', '')}'
            """)
            sql_types = {row[0]: {'type': row[1], 'char_len': row[2], 'prec': row[3], 'scale': row[4]} for row in cursor.fetchall()}

            for col in cols:
                if col in sql_types:
                    sql_type = sql_types[col]['type'].lower()
                    if sql_type in ('int', 'smallint', 'tinyint', 'bigint'):
                        for idx, row in enumerate(df_para_sql.values):
                            val = row[cols.index(col)]
                            if val is not None and isinstance(val, str) and val.isdigit():
                                val_int = int(val)
                                if sql_type == 'int' and val_int > 2147483647:
                                    logger.warning(f"COLUMNA '{col}' VALOR FUERA DE RANGO INT en fila {idx}: {val}")
                                elif sql_type == 'bigint' and val_int > 9223372036854775807:
                                    logger.warning(f"COLUMNA '{col}' VALOR FUERA DE RANGO BIGINT en fila {idx}: {val}")
        except Exception as type_e:
            logger.warning(f"No se pudo auditar tipos SQL: {type_e}")

        # 6. Definir inputsizes para evitar error 'String data, right truncation' de pyodbc
        inputsizes = []
        for col in cols:
            db_type_info = sql_types.get(col, {})
            db_type = db_type_info.get('type', '').lower()
            char_len = db_type_info.get('char_len', None)
            
            if 'char' in db_type or 'text' in db_type:
                non_null_strings = df_para_sql[col].dropna().astype(str)
                max_str_len = non_null_strings.str.len().max() if not non_null_strings.empty else 0
                if char_len is None or char_len == -1:
                    bind_len = max(max_str_len, 4000)
                else:
                    bind_len = max(max_str_len, char_len)
                inputsizes.append((pyodbc.SQL_WVARCHAR, bind_len, 0))
            else:
                inputsizes.append(None)
                
        cursor.setinputsizes(inputsizes)

        # 7. Convertir DataFrame a tuplas limpias de Python (Traducción de NaT, enteras y floats de numpy)
        def clean_sql_value(v):
            if pd.isna(v):
                return None
            if isinstance(v, np.integer):
                return int(v)
            if isinstance(v, np.floating):
                return float(v) if not np.isnan(v) else None
            return v

        data_to_load = [tuple(clean_sql_value(v) for v in row) for row in df_para_sql.values]

        logger.info(f"Subiendo {len(data_to_load)} registros a Staging DB...")

        try:
            cursor.executemany(sql_insert, data_to_load)
        except Exception as e:
            logger.error(f"Fallo masivo en executemany. Inyectando auditoría de aislamiento... Error original: {e}")

            for i, row in enumerate(data_to_load):
                try:
                    cursor.execute(sql_insert, row)
                except Exception as inner_e:
                    logger.error(f"FILA CORRUPTA AISLADA EN EL ÍNDICE {i}")
                    for j, (columna, valor) in enumerate(zip(cols, row)):
                        sql_type = sql_types.get(columna, {}).get('type', 'UNKNOWN')
                        logger.error(f"  [{j}] Columna: {columna} | SQL_Type: {sql_type} | Valor: {valor} | Tipo_Python: {type(valor)}")
                        try:
                            single_sql = f"INSERT INTO #stg_inagent ([{columna}]) VALUES (?)"
                            cursor.execute(single_sql, (valor,))
                            cursor.execute("DELETE FROM #stg_inagent WHERE 1=0")
                        except Exception as col_e:
                            logger.error(f"  >>>>> COLUMNA CULPABLE IDENTIFICADA: {columna} | Valor: {valor} | Error: {col_e}")
                    break

            raise

        sql_merge = f"""
        MERGE {TABLE_NAME} AS target
        USING #stg_inagent AS source
        ON (target.Id_Externo = source.Id_Externo)
        WHEN MATCHED THEN
            UPDATE SET 
                target.Analisis_Sentimental = source.Analisis_Sentimental,
                target.Tema_general_de_la_conversacion = source.Tema_general_de_la_conversacion,
                target.tool_status = source.tool_status,
                target.tool_return_fetch_message = source.tool_return_fetch_message,
                target.cas = source.cas,
                target.ctx_comb_resume = source.ctx_comb_resume
        WHEN NOT MATCHED THEN
            INSERT ({col_names_bracketed})
            VALUES ({', '.join(f'source.[{c}]' for c in cols)});
        """
        
        logger.info("Ejecutando MERGE en tabla definitiva...")
        cursor.execute(sql_merge)
        conn.commit()
        logger.info(f"ÉXITO: {len(df_para_sql)} registros sincronizados correctamente en {TABLE_NAME}.")

    except Exception as e:
        if 'conn' in locals(): conn.rollback()
        logger.error(f"Error en SQL: {e}")
    finally:
        if 'cursor' in locals(): cursor.close()
        if 'conn' in locals(): conn.close()


if __name__ == "__main__":
    hoy = datetime.now(cdmx_tz)
    hace_una_semana = hoy - timedelta(days=7)

    start_date = hace_una_semana.strftime("%Y-%m-%dT%H:%M:%S")
    end_date = hoy.strftime("%Y-%m-%dT%H:%M:%S")

    logger.info("="*50)
    logger.info("Iniciando pipeline dinámico de InAgent (Precisión Refactorizada)...")
    logger.info(f"Fechas configuradas: Desde {start_date} hasta {end_date}")

    df_raw = extract_inagent_data(start_date, end_date)

    if not df_raw.empty:
        df_prod = transform_data(df_raw)
        load_to_sql(df_prod)
    else:
        logger.info("El sistema de INAGENT devolvió 0 interacciones para el periodo.")