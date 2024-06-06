from airflow import DAG
from airflow.sensors.python import PythonSensor
import pendulum
from airflow.hooks.base import BaseHook

with DAG(
    dag_id='dags_python_sensor',
    start_date=pendulum.datetime(2024,6,16, tz='Asia/Seoul'),
    schedule='10 1 * * *',
    catchup=False
) as dag:
    def check_api_update(http_conn_id, endpoint, check_date, **kwargs):
        import requests
        import json
        connection = BaseHook.get_connection(http_conn_id)
        url = f'http://{connection.host}:{connection.port}/{endpoint}/1/5/{check_date}'
        print(f'url: {url}')
        response = requests.get(url)
        contents = json.loads(response.text)
        print(f'response: {contents}')
        code = contents.get('CODE')

        # 에러코드 INFO-200: 해당되는 데이터가 없습니다.
        # 미 갱신시 INFO-200으로 리턴됨
        if code is not None and code == 'INFO-200':
            print('상태코드: INFO-200, 데이터 미갱신')
            return False
        elif code is None:
            keys = list(contents.keys())
            rslt_code = contents.get(keys[0]).get('RESULT').get('CODE')

            # 정상 조회 코드 (INFO-000)
            if rslt_code == 'INFO-000':
                print('상태코드: INFO-000, 데이터 갱신 확인')
                return True
        else:
            print('상태코드 불분명')
            return False

    sensor_task = PythonSensor(
        task_id='sensor_task',
        python_callable=check_api_update,
        op_kwargs={'http_conn_id':'openapi.seoul.go.kr',
                   'endpoint':'{{var.value.apikey_openapi_seoul_go_kr}}/json/tbCycleRentUseDayInfo',
                   'check_date':'{{data_interval_start.in_timezone("Asia/Seoul") | ds_nodash }}'},
        poke_interval=600,   #10분
        mode='reschedule'
    )