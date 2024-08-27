from airflow.decorators import dag
from datetime import datetime

from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
from astro import sql as aql
from astro.files import File
from astro.sql.table import Table, Metadata
from astro.constants import FileType

from include.dbt.cosmos_config import DBT_PROJECT_CONFIG, DBT_CONFIG
from cosmos.airflow.task_group import DbtTaskGroup
from cosmos.constants import LoadMode
from cosmos.config import ProjectConfig, RenderConfig


@dag(
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=['uber_data'],   
)
def uber_data():
    upload_csv_to_gcs = LocalFilesystemToGCSOperator(
        task_id='upload_csv_to_gcs',
        src='/usr/local/airflow/include/dataset/uber_data.csv',
        dst='raw/uber_data.csv',
        bucket='uberdata-01',
        gcp_conn_id='gcp',
        mime_type='text/csv',
    )
    
    create_uber_data_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id='create_uber_data_dataset',
        dataset_id='uber_data',
        gcp_conn_id='gcp',
    )
    
    gcs_to_raw = aql.load_file(
        task_id='gcs_to_raw',
        input_file=File(
            'gs://uberdata-01/raw/uber_data.csv',
            conn_id='gcp',
            filetype=FileType.CSV,   
        ),
        output_table=Table(
            name='raw_uberdata',
            conn_id='gcp',
            metadata=Metadata(schema='uber_data')
        ),
        use_native_support=False,
    )
    
    transform = DbtTaskGroup(
        group_id='transform',
        project_config=DBT_PROJECT_CONFIG,
        profile_config=DBT_CONFIG,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            select=['path:models/transform']
        )
    )


    upload_csv_to_gcs >> create_uber_data_dataset >> gcs_to_raw  >> transform

uber_data()
