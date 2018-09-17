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
# spark-submit Oefeningen-hoofdstuk-3-Acties/Oefening3.py

steden = [["1, Amsterdam, Noord-Holland"],["2, Rotterdam, Zuid-Holland"],["3, Utrecht, Utrecht"],["4, Den Haag, Zuid-Holland"],["5,Groningen,Groningen"],["6,Maastricht,Limburg"],["7, Leeuwarden, Friesland"],["8, Nijmegen, Gelderland"]]
stedenRdd = sc.parallelize(steden)
stedenRdd.saveAsTextFile("bestanden/steden")
