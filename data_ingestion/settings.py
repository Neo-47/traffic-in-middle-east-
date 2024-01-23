from pathlib import Path
import pandas as pd
from prefect import task, flow
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials
import pandas as pd
import opendatasets as od

PATH = f"data"
DATASET_NAME = f"traffic-index-in-saudi-arabia-and-middle-east"
DATASET_FILE = f"traffic_index"
DATASET_URL = f"https://www.kaggle.com/datasets/majedalhulayel/traffic-index-in-saudi-arabia-and-middle-east?select=traffic_index.csv"
 