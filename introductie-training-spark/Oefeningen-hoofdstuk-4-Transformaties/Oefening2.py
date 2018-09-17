import sys
from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("Introductietraining-Spark").setMaster("local[1]")
sc = SparkContext(conf = conf)
sc.setLogLevel("ERROR")

# Oefening 2
# Maak een RDD van van het bestand sales.csv in de map bestanden
# Maak een lijst van alle genres 
#
# Sla het python script op
# en voer het script uit in het terminal-venster onder in het scherm d.m.v. het commando 
# spark-submit Oefeningen-hoofdstuk-4-Trasnformaties/Oefening2.py

salesRdd = sc.textFile("Bestanden/sales.csv")
genresRdd = salesRdd.map(lambda x: x.split(",")).map(lambda x:x[4]).distinct()

genres = genresRdd.collect()

for genre in genres:
  print("genre: {}".format(genre))