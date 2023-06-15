from minio import Minio


def funct_uploader(namebucket, namefile, datafolder):
	# Create a client with the MinIO server playground, its access key and secret key.
	client = Minio(
		"localhost:9000",					
		access_key="admin",		 			 
		secret_key="password",		
		secure=False,)

	# Make 'landingzone' bucket if not exist.
	found = client.bucket_exists(namebucket)	
	if not found:
		client.make_bucket(namebucket)		

	# Upload '/home/giacomo/Documenti/Letture_sensori/sensors_data.txt' as object name 'sensorsdata.txt' to bucket namebucket
	client.fput_object(namebucket, namefile, datafolder + namefile,)
	print(datafolder + namefile + " is successfully uploaded as "
		"object '" + namefile + "' into bucket '" + namebucket +"'.")


def funct_downloader(namebucket, namefile, datafolder):
	# Create a client with the MinIO server playground, its access key and secret key.
	client = Minio(
		"localhost:9000",	 				
		access_key="admin",		 			 
		secret_key="password",		
		secure=False,)

	# Make 'landingzone' bucket if not exist.
	found = client.bucket_exists(namebucket)
	if not found:
		client.make_bucket(namebucket)	
	
	# Download into '/home/giacomo/Documenti/Letture_sensori/sensors_data.txt' the object named 'sensorsdata.txt' from bucket namebucket
	client.fget_object(namebucket, namefile, datafolder + namefile,)
	print(datafolder + namefile + " is successfully downloaded as "
		"object '" + namefile + "' from bucket '" + namebucket +"'.")


def funct_parquetfileuploader(namebucket, datafolder, data):
	# Create a client with the MinIO server playground, its access key and secret key.
	client = Minio(
		"localhost:9000",					
		access_key="admin",		 			 
		secret_key="password",		
		secure=False,)

	# Make 'landingzone' bucket if not exist.
	found = client.bucket_exists(namebucket)	
	if not found:
		client.make_bucket(namebucket)		

	idnumber = 1
	
	while(idnumber < data):

		# Correzione nome file con ID e leading zeros
		idcreator = "ID" + str(idnumber).zfill(6) + ".parquet"
		# Upload '/home/giacomo/Documenti/Letture_sensori/sensors_data.txt' as object name 'sensorsdata.txt' to bucket namebucket
		client.fput_object(namebucket, idcreator, datafolder + idcreator,)
		print(datafolder + idcreator + " is successfully uploaded as "
			"object '" + idcreator + "' into bucket '" + namebucket +"'.")
		idnumber += 1




