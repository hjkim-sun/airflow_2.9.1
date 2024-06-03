from airflow import DAG
import pendulum
from airflow.decorators import task

with DAG(
    dag_id="dags_python_decorator_with_param",
    schedule="0 2 * * 1",
    start_date=pendulum.datetime(2024, 6, 14, tz="Asia/Seoul"),
    catchup=False,
) as dag:

    @task(task_id="python_task_1")
    def regist3(name, sex, *args, **kwargs):
        print(f'이름: {name}')
        print(f'성별: {sex}')
        print(f'기타옵션들: {args}')
        email = kwargs['email'] or 'empty'
        phone = kwargs['phone'] or 'empty'
        print(f'email: {email}')
        print(f'phone: {phone}')
        from pprint import pprint
        pprint(kwargs)

    python_task_1 = regist3('hjkim', 'man', 'seoul', email='hjkim_sun@naver.com', phone='010')