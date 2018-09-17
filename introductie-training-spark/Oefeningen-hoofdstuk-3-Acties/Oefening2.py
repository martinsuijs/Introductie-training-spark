import sys
from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("Introductietraining-Spark").setMaster("local[1]")
sc = SparkContext(conf = conf)
sc.setLogLevel("ERROR")

# Oefening 2
# Maak een RDD van van het bestand sales.csv in de map bestanden
# Onderzoek of er dubbele rijen inzitten (hiervoor kun je transformatie distinct() gebruiken)
#
# Sla het python script op
# en voer het script uit in het terminal-venster onder in het scherm d.m.v. het commando 
# spark-submit Oefeningen-hoofdstuk-3-Acties/Oefening2.py

