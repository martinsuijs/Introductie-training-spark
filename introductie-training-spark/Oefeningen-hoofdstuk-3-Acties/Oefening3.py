import sys
from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("Introductietraining-Spark").setMaster("local[1]")
sc = SparkContext(conf = conf)
sc.setLogLevel("ERROR")

# Oefening 1
# Maak een RDD van deze steden in Nederland: Amsterdam, Rotterdam, Den Haag, Utrecht, Groningen, Maastricht, Leeuwarden, Nijmegen
# Sla de RDD op als tekst bestand
#
# Sla het python script op
# en voer het script uit in het terminal-venster onder in het scherm d.m.v. het commando 
# spark-submit Oefeningen-hoofdstuk-3-Acties/Oefening3.py

