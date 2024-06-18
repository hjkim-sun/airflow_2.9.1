from airflow import DAG
import pendulum
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="dags_bash_trigger_rule",
    schedule=None,
    start_date=pendulum.datetime(2024, 6, 1, tz="Asia/Seoul"),
    catchup=False,
    tags=['homework']
) as dag:
    t1 = BashOperator(
        task_id="t1",
        bash_command="exit 0 ",
        dag=dag
    )

    t2 = BashOperator(
        task_id="t2",
        bash_command="exit 1",
        dag=dag
    )
    t3 = BashOperator(
        task_id="t3",
        bash_command="exit 1",
        dag=dag
    )
    t4 = BashOperator(
        task_id="t4",
        bash_command="echo \"last task\" ",
        trigger_rule='one_success',
        dag=dag
    )
    [t1, t2, t3] >> t4

