from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


# data_downloader TASK
def ddt():
	print("\nPROCESSING STARTED\nDownloading...")
	from data_ingestion import datafolder
	from data_validation import newnamefile
	from functions import minio_file_manager	
	minio_file_manager.funct_downloader("trustedzone", newnamefile, datafolder)
	print("File downloaded\n")	

# data_processor TASK
def dpt():
	print("\nContent processing...")
	from data_ingestion import datafolder		# datafolder = '/home/giacomo/Documenti/Letture_sensori/sensors_data.txt'
	from data_validation import  newnamefile	# data_validation newnamefile is my oldnamefile here in data_processing
	from functions import file_processor
	corrupted_lines = file_processor.funct(datafolder, newnamefile)
	print(str(corrupted_lines) + " alert elements found.\n")
	
# data_uploader TASK
def dut():
	print("\nUploading...")
	from data_ingestion import datafolder
	from functions.data_creator import data
	from functions import minio_file_manager
	minio_file_manager.funct_parquetfileuploader("refinedzone", datafolder, data)
	print("File uploaded\nVALIDATION ENDED\n")
	


with DAG('data_processing', start_date=datetime(2023,6,13), schedule='0 */6 * * *') as dag:

	data_downloader=PythonOperator(
	task_id='data_downloader',
	python_callable=ddt)

	data_processor=PythonOperator(
	task_id='data_processor',
	python_callable=dpt)

	data_uploader=PythonOperator(
	task_id='data_uploader',
	python_callable=dut)


data_downloader >> data_processor >> data_uploader
