from settings import *

@task(log_prints=True, retries=3)
def extract_from_gcs(dataset_name: str, dataset_file: str) -> Path:
    """Downlod trip data from GCS"""

    gcs_path = f"data/{dataset_name}/{dataset_file}.parquet"
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.get_directory(from_path = gcs_path, local_path = "data/")
    return Path(f"data/{gcs_path}")


@task()
def transform(path: Path) -> pd.DataFrame:
    """Transform data"""
    df = pd.read_parquet(path)
    print(len(df))
    return df

@task()
def write_bq(df: pd.DataFrame) -> None:
    """Write data to BigQuery"""

    gcp_credentials_block = GcpCredentials.load("zoom-gcs-creds")

    df.to_gbq(destination_table="traffic_project.jams",
              project_id="stately-planet-407118",
              chunksize=500_000,
              credentials=gcp_credentials_block.get_credentials_from_service_account(),
              if_exists="append")





@flow()
def upload_bq():
    """Main ETL flow to load data into bigquery"""

    dataset_name = DATASET_NAME
    dataset_file = DATASET_FILE

    path = extract_from_gcs(dataset_name, dataset_file)
    df = transform(path)
    write_bq(df)


if __name__ == '__main__':
    upload_bq()