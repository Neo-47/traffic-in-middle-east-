from data_fetch import fetch_upload_gcs
from gcs_to_bq import upload_bq
from settings import *

@flow()
def main():
    fetch_upload_gcs()
    upload_bq()
    
    
if __name__ == '__main__':
    main()