from airflow import DAG
from airflow.operators.bash import BashOperator



with DAG(
    "test"
) as dag:
    t1 = BashOperator(
        task_id="b1",
        bash_command="ls"
    )

    t2 = BashOperator(
            task_id="templated",
            depends_on_past=False,
            bash_command=templated_command,
        )

    t1 << t2