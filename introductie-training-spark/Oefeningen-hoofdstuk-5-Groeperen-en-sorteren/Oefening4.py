import sys
from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("Introductietraining-Spark").setMaster("local[1]")
sc = SparkContext(conf = conf)
sc.setLogLevel("ERROR")

# Oefening 4
# Maak een RDD van van het bestand sales.csv in de map bestanden
# Maak een pairRDD met de sales in EU per jaar en groepeer per jaar
# Hoeveel elememten hou je over
# Toon het eerste element, wat valt op?
#
# Sla het python script op
# en voer het script uit in het terminal-venster onder in het scherm d.m.v. het commando 
# spark-submit Oefeningen-hoofdstuk-5-Groeperen-en-sorteren/Oefening4.py

