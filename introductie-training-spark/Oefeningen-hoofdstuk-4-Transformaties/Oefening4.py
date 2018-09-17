import sys
from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("Introductietraining-Spark").setMaster("local[1]")
sc = SparkContext(conf = conf)
sc.setLogLevel("ERROR")

# Oefening 4
# Maak een RDD van van het bestand sales.csv in de map bestanden
# Maak een RDD zonder header regel
#
# Sla het python script op
# en voer het script uit in het terminal-venster onder in het scherm d.m.v. het commando 
# spark-submit Oefeningen-hoofdstuk-4-Trasnformaties/Oefening4.py

salesRdd = sc.textFile("Bestanden/sales.csv")
header = salesRdd.first()
salesClean = salesRdd.filter(lambda x: x != header)
print("aantal regels met header {}".format(salesRdd.count()))
print("aantal regels zonder header {}".format(salesClean.count()))
