from settings import *

def extract_from_gcs(dataset_name: str, dataset_file: str) -> Path:
    """Downlod traffic data from GCS"""

    gcs_path = f"data/{dataset_name}/{dataset_file}.parquet"
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.get_directory(from_path = gcs_path, local_path = "datalake/")
    
    return Path(f"datalake/{gcs_path}")

def write_bq(df: object) -> None:
    """Write data to BigQuery"""

    gcp_credentials_block = GcpCredentials.load("zoom-gcs-creds")

    
    df.to_gbq(destination_table="traffic_project.transformed_jams",
              project_id="stately-planet-407118",
              chunksize=500_000,
              credentials=gcp_credentials_block.get_credentials_from_service_account())


def schema_transform(spark: object, schema: object, data_path: str) -> object:
    
    
    new_df = spark.read \
        .option("header", "true") \
        .schema(schema) \
        .parquet(data_path)
        
    print("New schema is: ", new_df.schema)

    return new_df    

    
def tansform_main():
    
    schema = SCHEMA
    dataset_name = DATASET_NAME
    dataset_file = DATASET_FILE
    
    spark = SparkSession.builder \
                        .master("local[*]") \
                        .appName('traffic_project') \
                        .getOrCreate()
    
    data_path = extract_from_gcs(dataset_name, dataset_file)           
    df = schema_transform(spark, schema, str(data_path))
    print(type(df))
    dfc = df.toPandas()
    #write_bq(df)
    


if __name__ == "__main__":
    tansform_main()