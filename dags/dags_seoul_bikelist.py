from operators.seoul_api_to_csv_operator import SeoulApiToCsvOperator
from airflow import DAG
import pendulum

with DAG(
    dag_id='dags_seoul_bikelist',
    schedule='0 7 * * *',
    start_date=pendulum.datetime(2024,6,16, tz='Asia/Seoul'),
    catchup=False
) as dag:
    '''서울시 공공자전거 실시간 대여 현황'''
    seoul_api2csv_bike_list = SeoulApiToCsvOperator(
        task_id='seoul_api2csv_bike_list',
        dataset_nm='bikeList',
        path='/opt/airflow/ingest/bikeList/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash }}',
        file_name='bikeList.csv'
    )