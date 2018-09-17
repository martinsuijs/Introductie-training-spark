import sys
from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("Introductietraining-Spark").setMaster("local[1]")
sc = SparkContext(conf = conf)
sc.setLogLevel("ERROR")

# Oefening 5    
# Maak een RDD van van het bestand sales.csv in de map bestanden
# Maak een gesorteerde lijst van de jaartallen in het bestand
#
# Sla het python script op
# en voer het script uit in het terminal-venster onder in het scherm d.m.v. het commando 
## spark-submit Oefeningen-hoofdstuk-5-Groeperen-en-sorteren/Oefening5.py

