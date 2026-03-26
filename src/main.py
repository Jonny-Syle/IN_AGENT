import os
import json
import pyodbc
import logging
import requests
import pandas as pd
from datetime import datetime, timedelta, timezone
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

REQUIRED_ENV_VARS = ["INAGENT_API_KEY", "INAGENT_URL", "INAGENT_CREW_ID", "DB_SERVER", "BD"]
missing_vars = [var for var in REQUIRED_ENV_VARS if not os.getenv(var)]

if missing_vars:
    logger.critical(f"Faltan variables en el .env: {', '.join(missing_vars)}")
    raise EnvironmentError("Configuración incompleta.")

API_KEY = os.getenv("INAGENT_API_KEY")
ENDPOINT = os.getenv("INAGENT_URL")
CREW_ID = os.getenv("INAGENT_CREW_ID")

DB_SERVER = os.getenv('DB_SERVER')
DB_PORT = os.getenv('DB_PORT', '1433')
DB_NAME = os.getenv('BD')
DB_USER = os.getenv('DB_USER')
DB_PASS = os.getenv('DB_PASS')

logger.info("Entorno cargado y sistema de logs listo.")

TABLE_NAME = "dbo.inagent"

SQL_WHITELIST = [
    'Id', 'tool_timestamp', 'Id_Externo', 'Id_Canal', 'Canal', 'Timestamp', 
    'Inicio', 'Fin', 'Duración_s', 'Análisis_Sentimental', 
    'Tema_general_de_la_conversación', 'Resumen', 'Fue_resuelta', 
    'Fue_solo_agradecimiento', 'Es_saliente', 'Cerrada_por_inactividad', 
    'Fue_abandonada', 'Mensajes_del_asistente', 'Mensajes_del_usuario', 
    'Cantidad_de_preguntas_a_QNA', 'Herramientas_Usadas', 'Motivo_de_Cierre', 
    'Motivo_de_transferencia', 'ctx_creationRequestTimestamp', 
    'ctx_startedTime', 'ctx_startDelayMilliseconds', 'ctx_API_KEY', 
    'ctx_URL', 'ctx_tarjeta', 'ctx_siniestralidad', 'ctx_proxyData_recordFile', 
    'ctx_proxyData_roomId', 'ctx_proxyData_roomName', 'tool_URL_fetch', 
    'tool_body_fetch', 'tool_return_fetch', 'tool_tool', 'tool_status', 
    'tool_code_fetch', 'tool_return_fetch_msg', 'tool_return_fetch_message', 
    'tool_return_fetch_data_client_rfc', 'tool_return_fetch_data_client_complete_name', 
    'tool_return_fetch_data_program_status', 'tool_return_fetch_data_program_name', 
    'tool_return_fetch_data_client_type', 'tool_return_fetch_data_client_account', 
    'tool_return_fetch_data_client_card', 'tool_return_fetch_data_client_name', 
    'tool_return_fetch_httpCode', 'tool_return_fetch_status', 'tool_body_fetch_cas', 
    'tool_body_fetch_cancelMotive', 'tool_return_fetch_success', 
    'tool_body_fetch_lng_base', 'tool_body_fetch_firstDay', 
    'tool_body_fetch_thirdShift', 'tool_body_fetch_idBeneficiary', 
    'tool_body_fetch_thirdDay', 'tool_body_fetch_secondIdService', 
    'tool_body_fetch_firstShift', 'tool_body_fetch_firstIdService', 
    'tool_body_fetch_secondDay', 'tool_body_fetch_secondShift', 
    'tool_body_fetch_account', 'tool_return_fetch_cas_folio', 
    'tool_return_fetch_idAppointment', 'res_estado', 
    'res_especialization', 'res_authority'
]

def to_unix_ms(iso_date):
    dt = datetime.fromisoformat(iso_date).replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)

def safe_json_parse(val):
    try:
        if isinstance(val, (dict, list)): return val
        return json.loads(val) if (val and val != 'null') else {}
    except:
        return {}

def extract_inagent_data(start_iso, end_iso):
    all_data = []
    page = 0
    page_size = 100
    cols = []

    logger.info(f" Iniciando extracción desde {start_iso} hasta {end_iso}")

    while True:
        params = {
            "crew_id": CREW_ID,
            "start_ts": to_unix_ms(start_iso),
            "end_ts": to_unix_ms(end_iso),
            "page": page,
            "pageSize": page_size
        }
        headers = {"apikey": API_KEY}
        
        try:
            res = requests.get(ENDPOINT, headers=headers, params=params, timeout=30)
            res.raise_for_status()
            
            payload = res.json().get("data", {})
            rows = payload.get("rows", [])
            if page == 0:
                cols = payload.get("dataSchema", {}).get("columnNames", [])

            if not rows:
                break

            all_data.extend(rows)
            logger.info(f"Página {page} obtenida. Total acumulado: {len(all_data)}")

            if len(rows) < page_size:
                break
            
            page += 1
        except Exception as e:
            logger.error(f"Error en extracción (Página {page}): {e}")
            break

    return pd.DataFrame(all_data, columns=cols)

def transform_data(df_raw):
    if df_raw.empty: return pd.DataFrame()

    logger.info("Iniciando transformaciones...")

    ctx_raw = pd.json_normalize(df_raw['Contexto'].apply(safe_json_parse))
    ctx_raw.columns = [f"ctx_{c.replace('.', '_')}" for c in ctx_raw.columns]
    df_base = pd.concat([df_raw.drop(columns=['Contexto']), ctx_raw], axis=1)

    col_logs = 'ctx_toolLogs'
    if col_logs in df_base.columns:
        df_tools_exploded = df_base[['Id', col_logs]].dropna(subset=[col_logs]).explode(col_logs)
        tool_rows = df_tools_exploded[col_logs].apply(safe_json_parse).tolist()
        df_tools_flat = pd.json_normalize(tool_rows)
        
        df_tools_flat.columns = [f"tool_{c.replace('.', '_')}" for c in df_tools_flat.columns]
        df_tools_flat.index = df_tools_exploded.index
        df_tools_merged = pd.concat([df_tools_exploded[['Id']], df_tools_flat], axis=1)
        
        df_final = df_base.merge(df_tools_merged, on='Id', how='left')
    else:
        df_final = df_base

    target_col = 'tool_return_fetch_response'
    if target_col in df_final.columns:
        df_to_expand = df_final[['Id', 'tool_timestamp', target_col]].dropna(subset=[target_col]).copy()
        df_res_exploded = df_to_expand.explode(target_col)
        res_rows = df_res_exploded[target_col].apply(safe_json_parse).tolist()
        df_res_flat = pd.json_normalize(res_rows)
        
        res_whitelist = ['estado', 'especialization', 'authority']
        cols_p = [c for c in res_whitelist if c in df_res_flat.columns]
        df_res_final = df_res_flat[cols_p].add_prefix('res_')
        
        df_sub = pd.concat([
            df_res_exploded[['Id', 'tool_timestamp']].reset_index(drop=True),
            df_res_final.reset_index(drop=True)
        ], axis=1)
        
        df_final_v2 = df_final.merge(df_sub, on=['Id', 'tool_timestamp'], how='left')
    else:
        df_final_v2 = df_final

    df_final_v2.columns = [c.replace(" ", "_").replace("(", "").replace(")", "").replace(".", "_") for c in df_final_v2.columns]
    
    df_prod = df_final_v2[[c for c in SQL_WHITELIST if c in df_final_v2.columns]].copy()
    df_prod = df_prod.drop_duplicates(subset=['Id', 'tool_timestamp'], keep='first')
    
    logger.info(f" Transformación finalizada. Filas únicas: {len(df_prod)}")
    return df_prod

def prepare_for_sql(df):
    df_sql = df.copy()
    cols_num = ['Duración_s', 'Mensajes_del_asistente', 'Mensajes_del_usuario', 
                'ctx_startDelayMilliseconds', 'Cantidad_de_preguntas_a_QNA', 
                'Herramientas_Usadas', 'tool_code_fetch', 'tool_return_fetch_httpCode',
                'tool_body_fetch_lng_base', 'tool_body_fetch_secondIdService', 
                'tool_body_fetch_firstIdService', 'tool_return_fetch_idAppointment']
    
    cols_bit = ['Fue_resuelta', 'Fue_solo_agradecimiento', 'Es_saliente', 
                'Cerrada_por_inactividad', 'Fue_abandonada']

    for col in df_sql.columns:
        if col in cols_bit:
            df_sql[col] = pd.to_numeric(df_sql[col], errors='coerce').fillna(0).astype(int)
        elif col in cols_num:
            df_sql[col] = pd.to_numeric(df_sql[col], errors='coerce')
            df_sql[col] = df_sql[col].astype(object).where(pd.notnull(df_sql[col]), None)
        else:
            df_sql[col] = df_sql[col].astype(str).replace(['nan', 'None', 'NaN', 'null'], None)
    return df_sql

def load_to_sql(df_para_sql):
    if df_para_sql.empty:
        logger.info("No hay datos para cargar.")
        return

    conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={DB_SERVER},{DB_PORT};DATABASE={DB_NAME};UID={DB_USER};PWD={DB_PASS}"
    
    try:
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        cursor.fast_executemany = True

        cursor.execute(f"IF OBJECT_ID('tempdb..#stg_inagent') IS NOT NULL DROP TABLE #stg_inagent")
        cols = df_para_sql.columns.tolist()
        col_names = ", ".join(f"[{c}]" for c in cols)
        cursor.execute(f"SELECT TOP 0 {col_names} INTO #stg_inagent FROM {TABLE_NAME}")

        placeholders = ", ".join("?" for _ in cols)
        sql_insert = f"INSERT INTO #stg_inagent ({col_names}) VALUES ({placeholders})"
        cursor.executemany(sql_insert, [tuple(x) for x in df_para_sql.values])

        sql_merge = f"""
        WITH cte_source AS (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY Id, tool_timestamp ORDER BY (SELECT NULL)) as rn
            FROM #stg_inagent
        )
        MERGE {TABLE_NAME} AS target
        USING (SELECT * FROM cte_source WHERE rn = 1) AS source
        ON (target.Id = source.Id AND target.tool_timestamp = source.tool_timestamp)
        WHEN MATCHED THEN
            UPDATE SET target.Análisis_Sentimental = source.Análisis_Sentimental,
                       target.Resumen = source.Resumen,
                       target.tool_status = source.tool_status,
                       target.res_estado = source.res_estado
        WHEN NOT MATCHED THEN
            INSERT ({col_names})
            VALUES ({', '.join(f'source.[{c}]' for c in cols)});
        """
        cursor.execute(sql_merge)
        conn.commit()
        logger.info(f"Sincronización exitosa: {len(df_para_sql)} registros procesados.")

    except Exception as e:
        if 'conn' in locals(): conn.rollback()
        logger.error(f"Error en SQL: {e}")
    finally:
        if 'conn' in locals(): conn.close()
if __name__ == "__main__":
    ayer = datetime.now() - timedelta(days=1)
    start_date = ayer.strftime("%Y-%m-%dT00:00:00")
    end_date = ayer.strftime("%Y-%m-%dT23:59:59")

    df_raw = extract_inagent_data(start_date, end_date)
    if not df_raw.empty:
        df_prod = transform_data(df_raw)
        df_ready = prepare_for_sql(df_prod)
        load_to_sql(df_ready)
        df_ready.to_csv(f"notebooks/backup_inagent_{ayer.strftime('%Y%m%d')}.csv", index=False)
    else:
        logger.info("No se encontraron registros nuevos para procesar.")