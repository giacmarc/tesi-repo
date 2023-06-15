from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


def tone():
	print('\nTASK 1\n')

def ttwo():
	print('\nTASK 2\n')

def tthree():
	print('\nTASK 3\n')


with DAG('dipendenze', start_date=datetime(2023,1,1), schedule=None, catchup=False) as dag:
	
	task_one = PythonOperator(
	task_id = 'task_one',
	python_callable = tone)

	task_two = PythonOperator(
	task_id = 'task_two',
	python_callable = ttwo)

	task_three = PythonOperator(
	task_id = 'task_three',
	python_callable = tthree)



task_one >> [task_three, task_two]

