from sensors.seoul_api_date_sensor import SeoulApiDateSensor
from airflow import DAG
import pendulum

with DAG(
    dag_id='dags_custom_sensor',
    start_date=pendulum.datetime(2024,6,14, tz='Asia/Seoul'),
    schedule='0 9 * * *',
    catchup=False
) as dag:
    sensor__cycle_new_member_rent_info_day = SeoulApiDateSensor(
        task_id='sensor__cycle_new_member_rent_info_day',
        dataset_nm='cycleNewMemberRentInfoDay',
        check_date='{{data_interval_start.in_timezone("Asia/Seoul") | ds_nodash }}'
    )

