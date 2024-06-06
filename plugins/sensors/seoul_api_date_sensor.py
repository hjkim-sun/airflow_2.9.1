from airflow.sensors.base import BaseSensorOperator
from airflow.hooks.base import BaseHook
'''
서울시 공공데이터 API 추출시 특정 날짜로 업데이트 되었는지 확인하는 센서 
{dataset}/1/5/{yyyymmdd} 형태로 조회하는 데이터셋만 적용 가능
'''

class SeoulApiDateSensor(BaseSensorOperator):
    template_fields = ('endpoint','check_date')

    def __init__(self, dataset_nm, check_date, **kwargs):
        '''
        dataset_nm: 서울시 공공데이터 포털에서 센싱하고자 하는 데이터셋 명
        base_dt_col: 센싱 기준 컬럼 (yyyy.mm.dd... or yyyy/mm/dd... 형태만 가능)
        day_off: 배치일 기준 생성여부를 확인하고자 하는 날짜 차이를 입력 (기본값: 0)
        '''
        super().__init__(**kwargs)
        self.http_conn_id = 'openapi.seoul.go.kr'
        self.endpoint = '{{var.value.apikey_openapi_seoul_go_kr}}/json/' + dataset_nm + '/1/5'  # 5건만 추출
        self.check_date = check_date

    def poke(self, context):
        import requests
        import json
        connection = BaseHook.get_connection(self.http_conn_id)
        url = f'http://{connection.host}:{connection.port}/{self.endpoint}/{self.check_date}'
        self.log.info(f'url: {url}')
        response = requests.get(url)
        contents = json.loads(response.text)
        self.log.info(f'response: {contents}')
        code = contents.get('CODE')

        # 에러코드 INFO-200: 해당되는 데이터가 없습니다.
        # 미 갱신시 INFO-200으로 리턴됨
        if code is not None and code == 'INFO-200':
            self.log.info('상태코드: INFO-200, 데이터 미갱신')
            return False
        elif code is None:
            keys = list(contents.keys())
            try:
                rslt_code = contents.get(keys[0]).get('RESULT').get('CODE')
                # 정상 조회 코드 (INFO-000)
                if rslt_code == 'INFO-000':
                    self.log.info('상태코드: INFO-000, 데이터 갱신 확인')
                    return True
                else:
                    self.log.info('기타 상태코드')
                    return False
            except:
                # 비정상 호출일 경우 .get('CODE')에서 에러 발생
                self.log.info('기타 상태코드')
                return False


