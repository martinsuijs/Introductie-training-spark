import sys
from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("Introductietraining-Spark").setMaster("local[1]")
sc = SparkContext(conf = conf)
sc.setLogLevel("ERROR")

# Oefening 3
# Maak een RDD van van het bestand sales.csv in de map bestanden
# Maak een lijst van alle genres 
#
# Sla het python script op
# en voer het script uit in het terminal-venster onder in het scherm d.m.v. het commando 
# spark-submit Oefeningen-hoofdstuk-4-Trasnformaties/Oefening3.py

salesRdd = sc.textFile("Bestanden/sales.csv")
wordsRdd = salesRdd.flatMap(lambda x: x.split(","))

print("aantal woorden: {}".format(wordsRdd.count()))