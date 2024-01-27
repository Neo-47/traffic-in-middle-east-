
import pandas as pd
from pathlib import Path
from prefect import task, flow
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials
import opendatasets as od



PATH = f"data"
DATASET_NAME = f"traffic-index-in-saudi-arabia-and-middle-east"
DATASET_FILE = f"traffic_index"


