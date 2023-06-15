from airflow import DAG
from airflow.operators.python import PythonOperator

from datetime import datetime
from random import randint

import random, string

#import sys
#python_functions_files_path = '/home/giacomo/airflow/dags/functions'	# Directory file python con le task
#sys.path.append(python_functions_files_path)				# Importing files python

datafolder = '/home/giacomo/Documenti/Letture_sensori/'		# Directory con l'elaborazione dei dati (.txt, .json, .parquet)
namefile = "sensors_data.txt"					# Nome file .txt che verrà caricato nel primo bucket (lad)

Array = []


# data_generator TASK
def dct():
	print("\nINGESTION STARTED\nContent creation...")
	from functions import data_creator				
	data_creator.funct(datafolder, namefile)
	print("File " + datafolder + namefile + " created!\n")

# data_loader TASK
def dlt():
	print("\nUploading...")
	from functions import minio_file_manager
	minio_file_manager.funct_uploader("landingzone", namefile, datafolder)
	print("File uploaded\nINGESTION ENDED\n")


with DAG ('data_ingestion', start_date=datetime(2023,6,13), schedule='0 */1 * * *') as dag:

	data_creator=PythonOperator(
	task_id='data_creator',
	python_callable=dct)

	data_loader=PythonOperator(
	task_id='data_loader',
	python_callable=dlt)


data_creator >> data_loader

