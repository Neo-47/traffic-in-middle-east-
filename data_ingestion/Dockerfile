FROM prefecthq/prefect:2.7.7-python3.9

WORKDIR /opt/data_ingestion/

RUN apt-get update

COPY docker-requirements.txt .
COPY requirements.txt .

RUN pip install -r docker-requirements.txt --trusted-host pypi.python.org --no-cache-dir
RUN pip install -r requirements.txt --trusted-host pypi.python.org --no-cache-dir

COPY . data_ingest/

