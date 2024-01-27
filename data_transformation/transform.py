from settings import *


@task(log_prints=True)
def extract_from_gcs(dataset_name: str, dataset_file: str) -> Path:
    """Downlod traffic data from GCS"""

    gcs_path = f"data/{dataset_name}/{dataset_file}.parquet"
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.get_directory(from_path = gcs_path, local_path = "datalake/")
    
    return Path(f"datalake/{gcs_path}")

@task(log_prints=True)
def write_bq(df: object) -> None:
    """Write data to BigQuery"""

    gcp_credentials_block = GcpCredentials.load("zoom-gcs-creds")

    
    df.to_gbq(destination_table="traffic_project.transformed_jams",
              project_id="stately-planet-407118",
              chunksize=500_000,
              credentials=gcp_credentials_block.get_credentials_from_service_account(),
              if_exists='replace')

@task(log_prints=True)
def schema_transform(data_path: str) -> pd.DataFrame:

      
    df = pd.read_parquet(data_path)
    df['Datetime']= pd.to_datetime(df['Datetime'])
    print(df.head(5))
    print(df.dtypes)

    return df

@flow(name='data_transformation')
def transform_main():
    
    
    dataset_name = DATASET_NAME
    dataset_file = DATASET_FILE
                  
    data_path = extract_from_gcs(dataset_name, dataset_file)  
    df = schema_transform(str(data_path))
    write_bq(df)
    


if __name__ == "__main__":
    transform_main()