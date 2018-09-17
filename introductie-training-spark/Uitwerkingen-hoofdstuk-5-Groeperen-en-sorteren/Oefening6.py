import sys
from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("Introductietraining-Spark").setMaster("local[1]")
sc = SparkContext(conf = conf)
sc.setLogLevel("ERROR")

# Oefening 6
# Maak een RDD van van het bestand sales.csv in de map bestanden
# Maak een lijst van de jaartallen in het bestand met het aantal sales records per jaar
#
# Sla het python script op
# en voer het script uit in het terminal-venster onder in het scherm d.m.v. het commando 
# spark-submit Oefeningen-hoofdstuk-5-Groeperen-en-sorteren/Oefening6.py

salesRdd = sc.textFile("Bestanden/sales.csv")
header = salesRdd.first()

salesClean = salesRdd.filter(lambda x: x != header).filter(lambda line: line.split(",")[3]!="N/A")
euSales = salesClean.map(lambda x:x.split(",")).map(lambda x:(x[3],1))

euSalesCount = euSales.reduceByKey(lambda x, y: x+y).sortByKey()

for jaar, aantal in euSalesCount.collect():
    print("{}, {}".format(jaar, aantal))