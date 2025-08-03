# EXTRAI OS DADOS DO ARQUIVO SALVO NO YAML
def extract_data(spark, source_config):
    if source_config["type"] == "csv":
        df = spark.read.option("header", True).csv(source_config["path"])
        # df.show()
        return df
    else:
        raise ValueError(f"Unsupported source type: {source_config['type']}")