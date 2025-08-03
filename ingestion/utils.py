import yaml
# import os
from pathlib import Path
from pyspark.sql import SparkSession

# CRIA A SESSAO SPARK
def get_spark_session(app_name):
    return SparkSession.builder.appName(app_name).getOrCreate()

# PEGA OS DADOS DO YAML
def get_config(config_path):    
    # VOLTA PARA A RAIZ DO PROJETO
    base_path = Path(__file__).resolve().parent.parent
    config_path = base_path / config_path

    with open(config_path, "r") as f:
        return yaml.safe_load(f)

# LER O ARQUIVO PARQUET GERADO
def read_and_show_parquet(spark, parquet_path):
    df = spark.read.parquet(parquet_path)
    df.show(truncate=False)
    return df
