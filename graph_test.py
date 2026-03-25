#!/usr/bin/python

-- coding: utf-8 --
============================================================
PIPELINE COMPLETO - DE_PARA_ANALYTICS
============================================================
Fluxo:
1) Busca token no Microsoft Graph
2) Lista arquivos da pasta no SharePoint
3) Baixa os arquivos de_para_crm e de_para_growth
4) Lê a aba "Tabela de Campanhas"
5) Trata os dois excels no layout:
codigo, chave_estrutura_campanha, dt_partition, origem
6) Importa legado de_para_codigos_whatsapp do lake
7) Gera de_para_from_campanhasgrowth de campanhas_growth
8) Consolida tudo em de_para_analytics
9) Escreve todas as tabelas com overwrite
============================================================
import io
import re
import unicodedata
from datetime import datetime, timezone

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import openpyxl

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

============================================================
SPARK
============================================================
spark = (
SparkSession.builder
.appName("pipeline_de_para_analytics_v1")
.enableHiveSupport()
.getOrCreate()
)

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

============================================================
ALERTAS
============================================================
def send_success(msg: str):
try:
send_teams_alert(msg)
except Exception:
print(f"[SUCCESS] {msg}")

def send_failure(msg: str):
try:
send_teams_failure(msg)
except Exception:
print(f"[FAILURE] {msg}")

============================================================
CONFIG HARD-CODED
============================================================
CONFIG = {
# Azure / Graph
"tenant_id": "80523cc1-a4a7-4cfd-9624-859fc5fbaac4",
"client_id": "b1bdf4dc-178a-4d0f-bbd8-fd22e7b319b7",
"client_secret": "COLE_AQUI_O_SEU_CLIENT_SECRET",

# SharePoint / Drive / Pasta
"drive_id": "b!vpArwvPq-EyKZQHnaaI6RMMgMtiL62hImGcYhj6H9Q9VSquiI5qCRpnj8-B8jVez",
"folder_item_id": "01POEKBK2KC64NAPG265ELFX4GGTYBVNWF",

# Aba do Excel
"sheet_name": "Tabela de Campanhas",

# Padrões de nome dos arquivos dentro da pasta
"crm_file_patterns": ["de_para_crm"],
"growth_file_patterns": ["de_para_growth", "de_para_growth_whatsapp"],

# Tabelas no lake
"cluster_area": "public",
"write_mode": "overwrite",

"legacy_table_de_para_codigos_whatsapp": "datalake_crm_public.de_para_codigos_whatsapp",
"source_table_campanhas_growth": "datalake_crm_public.campanhas_growth",

"stage_table_de_para_crm": "de_para_crm",
"stage_table_de_para_growth": "de_para_growth_whatsapp",
"stage_table_de_para_from_campanhasgrowth": "de_para_from_campanhasgrowth",
"final_table_de_para_analytics": "de_para_analytics",

# HTTP
"http_timeout_seconds": 180
}

============================================================
VALIDAÇÕES
============================================================
def validate_config():
required = [
"tenant_id",
"client_id",
"client_secret",
"drive_id",
"folder_item_id"
]
missing = [k for k in required if not CONFIG.get(k)]
if missing:
raise ValueError(
"Configuração obrigatória ausente: " + ", ".join(missing)
)

============================================================
REQUESTS SESSION COM RETRY
============================================================
def build_requests_session():
retry = Retry(
total=5,
connect=5,
read=5,
backoff_factor=2,
status_forcelist=[429, 500, 502, 503, 504],
allowed_methods=["GET", "POST"]
)
adapter = HTTPAdapter(max_retries=retry)

session = requests.Session()
session.mount("https://", adapter)
session.mount("http://", adapter)
return session
HTTP = build_requests_session()

============================================================
HELPERS
============================================================
def normalize_text(value):
value = "" if value is None else str(value).strip()
value = unicodedata.normalize("NFKD", value).encode("ascii", "ignore").decode("ascii")
value = re.sub(r"[^a-zA-Z0-9]+", "", value.lower()).strip("")
return value

def make_unique_names(columns):
out = []
seen = {}
for col in columns:
base = normalize_text(col)
if not base:
base = "coluna"
idx = seen.get(base, 0)
final_name = base if idx == 0 else f"{base}_{idx}"
seen[base] = idx + 1
out.append(final_name)
return out

def clean_excel_value(value):
if pd.isna(value):
return None

if isinstance(value, float) and value.is_integer():
    return str(int(value)).strip()

return str(value).strip()
def parse_graph_datetime(dt_str):
if not dt_str:
return datetime.utcnow()
return datetime.fromisoformat(dt_str.replace("Z", "+00:00")).astimezone(timezone.utc).replace(tzinfo=None)

def pick_first_existing(available_columns, candidate_columns, field_name, required=True):
available_set = set(available_columns)
for candidate in candidate_columns:
if candidate in available_set:
return candidate

if required:
    raise ValueError(
        f"Não encontrei coluna para '{field_name}'. "
        f"Candidatas testadas: {candidate_columns}. "
        f"Disponíveis: {available_columns}"
    )
return None
def assert_not_empty(df, df_name):
cnt = df.count()
print(f"[CHECK] {df_name}: {cnt} registros")
if cnt == 0:
raise ValueError(f"O DataFrame '{df_name}' ficou vazio.")

def print_df_summary(df, name):
print(f"\n===== RESUMO {name} =====")
print(f"Total registros: {df.count()}")
if "origem" in df.columns:
df.groupBy("origem").count().orderBy("origem").show(truncate=False)

============================================================
CREATE TABLE NO DATALAKE
============================================================
def create_table(df, table_raw, cluster_area="public", mode_type="overwrite"):
if mode_type not in ["overwrite", "append"]:
raise ValueError("mode_type must be either 'overwrite' or 'append'")

table_name = f"datalake_crm_{cluster_area}.{table_raw}"
s3_path = f"s3://prd-agibank-datalake-crm-{cluster_area}/{table_raw}/"

(
    df.write
    .mode(mode_type)
    .format("parquet")
    .save(s3_path)
)

spark.sql(f"DROP TABLE IF EXISTS {table_name}")

ddl_cols = ", ".join([f"`{col_name}` {data_type}" for col_name, data_type in df.dtypes])

ddl = f"""
    CREATE EXTERNAL TABLE {table_name} (
        {ddl_cols}
    )
    STORED AS PARQUET
    LOCATION '{s3_path}'
"""

print(f"[DDL] {ddl}")
spark.sql(ddl)
============================================================
MICROSOFT GRAPH
============================================================
def get_access_token():
url = f"https://login.microsoftonline.com/{CONFIG['tenant_id']}/oauth2/v2.0/token"

payload = {
    "client_id": CONFIG["client_id"],
    "scope": "https://graph.microsoft.com/.default",
    "client_secret": CONFIG["client_secret"],
    "grant_type": "client_credentials"
}

response = HTTP.post(
    url,
    data=payload,
    timeout=CONFIG["http_timeout_seconds"]
)
response.raise_for_status()

return response.json()["access_token"]
def graph_headers(access_token):
return {
"Authorization": f"Bearer {access_token}"
}

def list_folder_children(access_token, drive_id, folder_item_id):
url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/items/{folder_item_id}/children"
params = {
"$top": 200,
"$select": "id,name,file,folder,lastModifiedDateTime"
}

items = []
headers = graph_headers(access_token)

while url:
    response = HTTP.get(
        url,
        headers=headers,
        params=params,
        timeout=CONFIG["http_timeout_seconds"]
    )
    response.raise_for_status()
    payload = response.json()

    items.extend(payload.get("value", []))
    url = payload.get("@odata.nextLink")
    params = None

print(f"[GRAPH] Itens encontrados na pasta: {len(items)}")
return items
def choose_file_item(folder_items, match_patterns):
candidates = []
normalized_patterns = [normalize_text(x) for x in match_patterns]

for item in folder_items:
    if "file" not in item:
        continue

    item_name = item.get("name", "")
    item_name_norm = normalize_text(item_name)

    if any(pattern in item_name_norm for pattern in normalized_patterns):
        candidates.append(item)

if not candidates:
    raise FileNotFoundError(
        f"Nenhum arquivo encontrado para os padrões {match_patterns}"
    )

candidates.sort(key=lambda x: x.get("lastModifiedDateTime", ""), reverse=True)
chosen = candidates[0]

print(
    f"[GRAPH] Arquivo escolhido: {chosen.get('name')} | "
    f"id={chosen.get('id')} | "
    f"lastModifiedDateTime={chosen.get('lastModifiedDateTime')}"
)
return chosen
def download_drive_item_content(access_token, drive_id, item_id):
url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/items/{item_id}/content"

response = HTTP.get(
    url,
    headers=graph_headers(access_token),
    timeout=CONFIG["http_timeout_seconds"]
)
response.raise_for_status()

return response.content
============================================================
LEITURA DOS EXCELS
============================================================
def read_excel_mapping_from_bytes(file_bytes, origem, dt_partition):
"""
Lê a aba 'Tabela de Campanhas' e transforma no layout final:
codigo, chave_estrutura_campanha, dt_partition, origem
"""
pdf = pd.read_excel(
io.BytesIO(file_bytes),
sheet_name=CONFIG["sheet_name"],
engine="openpyxl",
dtype=object
)

pdf.columns = make_unique_names(pdf.columns)
available_columns = list(pdf.columns)

codigo_col = pick_first_existing(
    available_columns,
    ["codigo", "cod", "codigo_whatsapp", "utm_custom"],
    "codigo"
)

chave_col = pick_first_existing(
    available_columns,
    ["chave_estrutura_campanha", "chave_estrutura", "campanha"],
    "chave_estrutura_campanha"
)

rows = []
for _, row in pdf[[codigo_col, chave_col]].iterrows():
    codigo = clean_excel_value(row[codigo_col])
    chave = clean_excel_value(row[chave_col])

    if not codigo:
        continue
    if not chave:
        continue

    rows.append((codigo, chave, dt_partition, origem))

schema = T.StructType([
    T.StructField("codigo", T.StringType(), True),
    T.StructField("chave_estrutura_campanha", T.StringType(), True),
    T.StructField("dt_partition", T.TimestampType(), True),
    T.StructField("origem", T.StringType(), True)
])

spark_df = spark.createDataFrame(rows, schema=schema)
spark_df = standardize_mapping_df(spark_df)

return spark_df
============================================================
PADRONIZAÇÃO
============================================================
def standardize_mapping_df(df):
return (
df.select(
F.trim(F.col("codigo").cast("string")).alias("codigo"),
F.trim(F.col("chave_estrutura_campanha").cast("string")).alias("chave_estrutura_campanha"),
F.col("dt_partition").cast("timestamp").alias("dt_partition"),
F.col("origem").cast("string").alias("origem")
)
.filter(F.col("codigo").isNotNull())
.filter(F.length(F.trim(F.col("codigo"))) > 0)
.filter(F.col("chave_estrutura_campanha").isNotNull())
.filter(F.length(F.trim(F.col("chave_estrutura_campanha"))) > 0)
.dropDuplicates(["codigo", "chave_estrutura_campanha", "origem"])
)

============================================================
LEGADO: de_para_codigos_whatsapp
============================================================
def build_legacy_de_para_codigos_whatsapp():
df = spark.table(CONFIG["legacy_table_de_para_codigos_whatsapp"])
df = df.toDF(*make_unique_names(df.columns))

codigo_col = pick_first_existing(
    df.columns,
    ["codigo", "cod", "utm_custom"],
    "codigo legado"
)

campanha_col = pick_first_existing(
    df.columns,
    ["campanha", "chave_estrutura_campanha", "chave_estrutura"],
    "campanha/chave legado"
)

dt_col = pick_first_existing(
    df.columns,
    ["dt_partition", "date_partition", "data_partition"],
    "dt_partition legado",
    required=False
)

dt_expr = F.col(dt_col).cast("timestamp") if dt_col else F.current_timestamp()

out = (
    df.select(
        F.trim(F.col(codigo_col).cast("string")).alias("codigo"),
        F.trim(F.col(campanha_col).cast("string")).alias("chave_estrutura_campanha"),
        dt_expr.alias("dt_partition")
    )
    .withColumn("origem", F.lit("de_para_codigos_whatsapp"))
)

return standardize_mapping_df(out)
============================================================
DERIVADO: campanhas_growth -> de_para_from_campanhasgrowth
============================================================
def build_de_para_from_campanhasgrowth():
df = spark.table(CONFIG["source_table_campanhas_growth"])
df = df.toDF(*make_unique_names(df.columns))

out = (
    df.select(
        F.trim(F.col("utm_custom").cast("string")).alias("codigo"),
        F.concat(
            F.coalesce(F.trim(F.col("campanha").cast("string")), F.lit("")),
            F.lit("-"),
            F.coalesce(F.trim(F.col("grupo_anuncio").cast("string")), F.lit("")),
            F.lit("-"),
            F.coalesce(F.trim(F.col("criativo").cast("string")), F.lit(""))
        ).alias("chave_estrutura_campanha")
    )
    .withColumn("dt_partition", F.current_timestamp())
    .withColumn("origem", F.lit("de_para_from_campanhasgrowth"))
)

return standardize_mapping_df(out)
============================================================
PIPELINE
============================================================
def main():
validate_config()

print("[START] Iniciando pipeline de_para_analytics")

# 1) Token
access_token = get_access_token()
print("[OK] Token obtido com sucesso")

# 2) Lista pasta
folder_items = list_folder_children(
    access_token=access_token,
    drive_id=CONFIG["drive_id"],
    folder_item_id=CONFIG["folder_item_id"]
)

# 3) Resolve arquivos
crm_item = choose_file_item(folder_items, CONFIG["crm_file_patterns"])
growth_item = choose_file_item(folder_items, CONFIG["growth_file_patterns"])

crm_dt_partition = parse_graph_datetime(crm_item.get("lastModifiedDateTime"))
growth_dt_partition = parse_graph_datetime(growth_item.get("lastModifiedDateTime"))

# 4) Download
crm_bytes = download_drive_item_content(
    access_token=access_token,
    drive_id=CONFIG["drive_id"],
    item_id=crm_item["id"]
)

growth_bytes = download_drive_item_content(
    access_token=access_token,
    drive_id=CONFIG["drive_id"],
    item_id=growth_item["id"]
)

print("[OK] Downloads concluídos")

# 5) Excel -> Spark
df_crm = read_excel_mapping_from_bytes(
    file_bytes=crm_bytes,
    origem="de_para_crm",
    dt_partition=crm_dt_partition
)

df_growth = read_excel_mapping_from_bytes(
    file_bytes=growth_bytes,
    origem="de_para_growth_whatsapp",
    dt_partition=growth_dt_partition
)

# 6) Legado
df_legacy = build_legacy_de_para_codigos_whatsapp()

# 7) Derivado campanhas_growth
df_campanhasgrowth = build_de_para_from_campanhasgrowth()

# 8) Data Quality
assert_not_empty(df_crm, "df_crm")
assert_not_empty(df_growth, "df_growth")
assert_not_empty(df_legacy, "df_legacy")
assert_not_empty(df_campanhasgrowth, "df_campanhasgrowth")

# 9) Escrita stage
create_table(
    df_crm,
    table_raw=CONFIG["stage_table_de_para_crm"],
    cluster_area=CONFIG["cluster_area"],
    mode_type=CONFIG["write_mode"]
)

create_table(
    df_growth,
    table_raw=CONFIG["stage_table_de_para_growth"],
    cluster_area=CONFIG["cluster_area"],
    mode_type=CONFIG["write_mode"]
)

create_table(
    df_campanhasgrowth,
    table_raw=CONFIG["stage_table_de_para_from_campanhasgrowth"],
    cluster_area=CONFIG["cluster_area"],
    mode_type=CONFIG["write_mode"]
)

# 10) Consolidação final
df_final = (
    df_crm
    .unionByName(df_growth)
    .unionByName(df_legacy)
    .unionByName(df_campanhasgrowth)
)

df_final = standardize_mapping_df(df_final)

assert_not_empty(df_final, "df_final")
print_df_summary(df_final, "de_para_analytics")

# 11) Escrita final
create_table(
    df_final,
    table_raw=CONFIG["final_table_de_para_analytics"],
    cluster_area=CONFIG["cluster_area"],
    mode_type=CONFIG["write_mode"]
)

send_success(
    f"Pipeline de_para_analytics finalizado com sucesso. "
    f"Registros finais: {df_final.count()}"
)

print("[END] Pipeline finalizado com sucesso")
============================================================
EXECUÇÃO
============================================================
if name == "main":
try:
main()
except Exception as e:
send_failure(f"Falha no pipeline de_para_analytics: {e}")
raise
