import sys
from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("Introductietraining-Spark").setMaster("local[1]")
sc = SparkContext(conf = conf)
sc.setLogLevel("ERROR")

# Oefening 6
# Maak een RDD van van het bestand sales.csv in de map bestanden
# Toon 2010 per genre het aantal records
#
# Sla het python script op
# en voer het script uit in het terminal-venster onder in het scherm d.m.v. het commando 
# spark-submit Oefeningen-hoofdstuk-4-Transformaties/Oefening6.py

salesRdd = sc.textFile("Bestanden/sales.csv")
header = salesRdd.first()
salesClean = salesRdd.filter(lambda x: x != header).filter(lambda line: line.split(",")[3]!="N/A")

genres2010 = salesClean.map(lambda x: x.split(",")) \
    .filter(lambda x: x[3]=="2010") \
    .map(lambda x:x[4]) \
    .countByValue()

for genre, count in genres2010.items():
    print("{} : {}".format(genre, count))