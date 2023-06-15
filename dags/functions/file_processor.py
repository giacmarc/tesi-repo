import os
import json
import pandas as pd

from operator import itemgetter		# per il SortedArray
from fastparquet import ParquetFile



def funct(argpath, oldnamefile):

	Array = []
	NewArray = []
	
	# Conto le righe con dati corrotti (dato della temperatura corrotta = null -> errore di rilevamento)
	alert_lines = 0
	counter = 0

	# Verifica esistenza file json
	if os.path.isfile(argpath + oldnamefile): 
		check = True			
		print("File found!")
	else: 
		check = False
		print("File NOT found!")
	
	if (check == True):
		# Se il file esiste si procede con l'aggiunta del campo OK/ALERT
		with open(argpath + oldnamefile) as file:
			Array = json.load(file)					
			while(counter < len(Array)):	
				if(Array[counter][2] != "TEMPERATURE DETECTION ERROR" and float(Array[counter][2]) > 500):
					Array[counter] = (Array[counter][0], Array[counter][1], Array[counter][2], Array[counter][3], "ALERT")
					alert_lines += 1				
				else:
					Array[counter] = (Array[counter][0], Array[counter][1], Array[counter][2], Array[counter][3], "OK")
							
				NewArray.append(Array[counter])
				counter += 1
	
		SortedArray = sorted(NewArray, key=itemgetter(0))	# sorting by sensorID (reverse = False)
	
		counter = 0	
		while(counter < len(SortedArray)):
			LastArray = []
			sensorid = SortedArray[counter][0]
			while(counter < len(SortedArray) and sensorid == SortedArray[counter][0]):
				LastArray.append(SortedArray[counter])
				counter += 1
				dataframe = pd.DataFrame(LastArray)
			dataframe.columns = ["SensorID","Timestamp","Temperature","Unit","Warning"]
			print(dataframe)
			dataframe.to_parquet(argpath + sensorid + '.parquet')		
		

	return alert_lines
