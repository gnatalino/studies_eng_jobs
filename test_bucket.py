from py4j.java_gateway import java_import

def s3_path_exists(spark, path: str) -> bool:
    try:
        jvm = spark._jvm
        hconf = spark._jsc.hadoopConfiguration()
        
        # Corrigir o caminho S3 se necessário
        if path.startswith('s3://'):
            path = path.replace('s3://', 's3a://')
        
        hadoop_path = jvm.org.apache.hadoop.fs.Path(path)
        fs = hadoop_path.getFileSystem(hconf)
        
        return fs.exists(hadoop_path)
    except Exception as e:
        print(f"[ERROR] Erro ao verificar caminho: {e}")
        return False

# DEFINA O CONFIG AQUI
config = {
    "source": {
        "sql_file": "s3://datalake-configs/git_crm/datahubs_cores/querys/core_lista_hot.sql"
    }
}

sql_path = config["source"]["sql_file"]
print(f"[DEBUG] sql_path={sql_path}")
print(f"[DEBUG] sql_path_exists={s3_path_exists(spark, sql_path)}")
