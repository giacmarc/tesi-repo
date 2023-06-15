from datetime import datetime
from random import randint

import random, string


Array = []
# Per decidere quanti dati generare
data = 11

def funct(argpath, namefile):
	
	idnumber = 1
	
	while(idnumber < data): #201):
				
		idcreator = "ID" + str(idnumber).zfill(6)
		timestampcreator = str(datetime.now())
		supporttemperaturecreator = randint(0,60000)/100
		if(supporttemperaturecreator < 50):
			supporttemperaturecreator = 'null'
		temperaturecreator = str(supporttemperaturecreator).replace(',','.')
		supportarray = (idcreator, timestampcreator, temperaturecreator, '°C') #, 'Alert')
		Array.append(supportarray)
		idnumber += 1

	with open(argpath + namefile, 'a+', encoding='UTF-8') as txt_file:
		for line in Array: 
			txt_file.write(",".join(line) + "\n")


