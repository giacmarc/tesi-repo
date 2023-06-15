from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

oldnamefile = "sensors_data.txt"					# file name BEFORE VALIDATION
newnamefile = "sensors_data.json"					# file name AFTER VALIDATION


# data_downloader TASK
def ddt():
	print("\nVALIDATION STARTED\nDownloading...")
	from data_ingestion import datafolder
	from functions import minio_file_manager
	minio_file_manager.funct_downloader("landingzone", oldnamefile, datafolder)
	print("File downloaded\n")	

# data_validator TASK
def dvt():
	print("\nContent validation...")
	from data_ingestion import datafolder		# datafolder = '/home/giacomo/Documenti/Letture_sensori/sensors_data.txt'
	from functions import file_validator
	corrupted_lines = file_validator.funct(datafolder, oldnamefile, newnamefile)
	print(str(corrupted_lines) + " corrupted elements found.\n")

# data_uploader TASK
def dut():
	print("\nUploading...")
	from data_ingestion import datafolder
	from functions import minio_file_manager
	minio_file_manager.funct_uploader("trustedzone", newnamefile, datafolder)
	print("File uploaded\nVALIDATION ENDED\n")


with DAG('data_validation', start_date=datetime(2023,6,13), schedule='0 */3 * * *') as dag:

	data_downloader=PythonOperator(
	task_id='data_downloader',
	python_callable=ddt)

	data_validator=PythonOperator(
	task_id='data_validator',
	python_callable=dvt)

	data_uploader=PythonOperator(
	task_id='data_uploader',
	python_callable=dut)


data_downloader >> data_validator >> data_uploader
