from datetime import datetime

import os
import json
#import re



def funct(argpath, oldnamefile, newnamefile):
	
	PreviousArray = []
	Array = []
		
	# Verifica esistenza file json per il recupero della lista preesistente nel file json
	if os.path.isfile(argpath + newnamefile):
		counter = 0
		with open(argpath + newnamefile) as file:
			PreviousArray = json.load(file)
			while(counter < len(PreviousArray)):
				Array.append(PreviousArray[counter])
				counter += 1
	
	# Conto le righe con dati corrotti (dato della temperatura corrotta = null -> errore di rilevamento)
	corrupted_lines = 0

	# Verifica esistenza file txt 
	if os.path.isfile(argpath + oldnamefile): 
		check = True			
		print("File found!")
	else: 
		check = False
		print("File NOT found!")
	
	if (check == True):
		# Se il file esiste si procede con la lettura del file txt per salvare le linee in una lista 
		# che sarà appesa ad Array (eventualmente già riempito della preesistente lista)
		with open(argpath + oldnamefile, 'r', encoding='UTF-8') as file:
			for line in file:
				array = line.rstrip().split(",")
				
				now = datetime.now()			
				generationtime = array[1]
				delta = now - datetime.strptime(generationtime, '%Y-%m-%d %H:%M:%S.%f')
				hours = (delta.total_seconds() / 60) / 60
			# se il timestamp è da risale a meno di tre ore prendo i dati, altrimenti skip				
				if (1 <= int(hours) <= 3):	
					if (array[2] == "null"):
						array[2] = "TEMPERATURE DETECTION ERROR"
						corrupted_lines += 1
			
					# Appendo su Array vuoto o sulle preesistenti liste
					Array.append(array) 	
	
		# Scrivo il nuovo file e ritorno il numero di righe modificate
		jsonString = json.dumps(Array)
		jsonFile = open(argpath + newnamefile, "w", encoding='UTF-8')
		#print(jsonString)
		jsonFile.write(jsonString)
		jsonFile.close()

			
	return corrupted_lines
