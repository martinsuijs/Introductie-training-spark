import sys
from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("Introductietraining-Spark").setMaster("local[1]")
sc = SparkContext(conf = conf)
sc.setLogLevel("ERROR")

# Oefening 7
# Maak een RDD van van het bestand sales.csv in de map bestanden
# Maak een RDD met de gegevens van 2010
#
# Sla het python script op
# en voer het script uit in het terminal-venster onder in het scherm d.m.v. het commando 
# spark-submit Oefeningen-hoofdstuk-4-Transformaties/Oefening7.py

# Oefening 7
# Lees de bestanden landen1.csv en landen2.csv in en maak één RDD voor beide bestanden
#
# Sla het python script op
# en voer het script uit in het terminal-venster onder in het scherm d.m.v. het commando 
# spark-submit Oefeningen-hoofdstuk-4-Trasnformatiess/Oefening7.py

landen1Rdd = sc.textFile("bestanden/landen1.csv")
landen2Rdd = sc.textFile("bestanden/landen2.csv")

landen = landen1Rdd.union(landen2Rdd)

print("Aantal landen: {}".format(landen.count()))

for land in landen.collect():
    print(land)
