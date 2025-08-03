from ingestion.extract import extract_data
from ingestion.transform import transform_data
from ingestion.load import load_data
from ingestion.utils import get_config, get_spark_session, read_and_show_parquet

def main():
    spark = get_spark_session("GenericIngestionJob")
    config = get_config("config/sources.yaml")
    df_raw = extract_data(spark, config["source"])
    df_transformed = transform_data(df_raw)
    load_data(df_transformed, config["destination"])
    read_and_show_parquet(spark, config["destination"]["path"])

if __name__ == "__main__":
    main()
