import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import types
import pandas as pd
from pathlib import Path
from prefect import task, flow
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials
import opendatasets as od


PATH = f"data"
DATASET_NAME = f"traffic-index-in-saudi-arabia-and-middle-east"
DATASET_FILE = f"traffic_index"


SCHEMA = types.StructType([types.StructField('City', types.StringType(), True),
                         types.StructField('Datetime', types.TimestampType(), True),
                         types.StructField('TrafficIndexLive', types.IntegerType(), True),
                         types.StructField('JamsCount', types.IntegerType(), True),
                         types.StructField('JamsDelay', types.FloatType(), True),
                         types.StructField('JamsLength', types.FloatType(), True),
                         types.StructField('TrafficIndexWeekAgo', types.IntegerType(), True),
                         types.StructField('TravelTimeHistoric', types.FloatType(), True),
                         types.StructField('TravelTimeLive', types.FloatType(), True)])