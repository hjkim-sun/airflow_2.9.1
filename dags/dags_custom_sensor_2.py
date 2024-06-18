from sensors.seoul_api_date_sensor import SeoulApiDateColumnSensor
from airflow import DAG
import pendulum

with DAG(
    dag_id='dags_custom_sensor_2',
    start_date=pendulum.datetime(2024,6,14, tz='Asia/Seoul'),
    schedule='0 9 * * *',
    catchup=False
) as dag:
    # 서울시 행정동별 대중교통 총 승차 승객수 정보 (https://data.seoul.go.kr/dataList/OA-21223/S/1/datasetView.do)
    sensor__tpss_passenger_cnt = SeoulApiDateColumnSensor(
        task_id='sensor__tpss_passenger_cnt',
        dataset_nm='tpssPassengerCnt',
        base_dt_col='CRTR_DT',
        day_off=-5,
        mode='reschedule'
    )

    # 한강공원 주차장 일별 이용현황 (https://data.seoul.go.kr/dataList/OA-21084/S/1/datasetView.do)
    sensor__tb_use_day_status_view = SeoulApiDateColumnSensor(
        task_id='sensor__tb_use_day_status_view',
        dataset_nm='TbUseDaystatusView',
        base_dt_col='DT',
        day_off=-1,
        mode='reschedule'
    )