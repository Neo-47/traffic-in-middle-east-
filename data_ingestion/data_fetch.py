from settings import *

@task(retries=3)
def fetch(dataset_url: str, dataset_file: str, dataset_name: str, path: str) -> pd.DataFrame:
    """Read taxi data from web into pandas DataFrame"""
    
    od.download(dataset_url, path)
    df = pd.read_csv(f"{path}/{dataset_name}/{dataset_file}.csv")
    
    return df
 

@task(log_prints=True)
def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Fix dtype issues"""
    
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")
    
    return df


@task()
def write_local(df: pd.DataFrame, dataset_name: str, dataset_file: str) -> Path:
    
    """Write DataFrame out locally as parquet file"""
    
    path = Path(f"data/{dataset_name}/{dataset_file}.parquet")
    df.to_parquet(path, compression="gzip")
    
    return path


@task()
def write_gcs(path: Path) -> None:
    """Upload local parquet file to gcs"""
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.upload_from_path(from_path=path,to_path=path)
    return



@flow()
def fetch_upload_gcs() -> None:
    
    """The main ETL function"""
  
    path = PATH
    dataset_name = DATASET_NAME
    dataset_file = DATASET_FILE
    dataset_url = DATASET_URL

    df = fetch(dataset_url, dataset_file, dataset_name,  path)
    df_clean = clean(df)
    path = write_local(df_clean, dataset_name, dataset_file)
    write_gcs(path)


if __name__ == "__main__":
    fetch_upload_gcs()
