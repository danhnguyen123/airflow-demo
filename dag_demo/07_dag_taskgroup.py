from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.empty import EmptyOperator
from datetime import datetime

with DAG(dag_id='taskgroup_04',
         start_date=datetime(2021, 1, 1),
         max_active_runs=1,
         schedule_interval="* * * * *",
         catchup=False
         ) as dag:


    t0 = EmptyOperator(task_id='start_04')

    # Start task group definition
    with TaskGroup(group_id='group1_04') as tg1:
        t1 = EmptyOperator(task_id='task1_04')
        t2 = EmptyOperator(task_id='task2_04')

        t1 >> t2
    # End task group definition
        
    t3 = EmptyOperator(task_id='end')

    # Set task group's (tg1) dependencies
    t0 >> tg1 >> t3