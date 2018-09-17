import sys
from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("Introductietraining-Spark").setMaster("local[1]")
sc = SparkContext(conf = conf)
sc.setLogLevel("ERROR")

# Oefening 1
# Maak een RDD van deze steden in Nederland: Amsterdam, Rotterdam, Den Haag, Utrecht, Groningen, Maastricht, Leeuwarden, Nijmegen
# Gebruik de actie count() om te testen of het aanmaken van de RDD gelukt is (lazy execution).
#
# Sla het python script op
# en voer het script uit in het terminal-venster onder in het scherm d.m.v. het commando 
# spark-submit Oefeningen-hoofdstuk-2-RDD/Oefening1.py

steden = ["Amsterdam", "Rotterdam", "Utrecht", "Den Haag", "Groningen", "Maastricht", "Leeuwarden", "Nijmegen"]
stedenRdd = sc.parallelize(steden)
aantalSteden = stedenRdd.count()

print("Aantal steden: {}".format(aantalSteden))

