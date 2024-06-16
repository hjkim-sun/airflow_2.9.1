from airflow.decorators import dag
import pendulum
from airflow.operators.bash import BashOperator

@dag(dag_id='dags_bash_operator_decorator',
     schedule="0 9 * * 1,5",
     start_date=pendulum.datetime(2024, 6, 1, tz="Asia/Seoul"),
     catchup=False,
     tags=['homework']
)
def dags_bash_operator_decorator():
    bash_t1 = BashOperator(
        task_id="bash_t1",
        bash_command="echo whoami"
    )

    bash_t2 = BashOperator(
        task_id="bash_t2",
        bash_command="echo $HOSTNAME"
    )

    bash_t1 >> bash_t2

dags_bash_operator_decorator()