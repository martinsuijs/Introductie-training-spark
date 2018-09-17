import sys
from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("Introductietraining-Spark").setMaster("local[1]")
sc = SparkContext(conf = conf)
sc.setLogLevel("ERROR")

# Oefening 1
# Maak een pairRdd van onderstaande sales data
# Jaar  Sales (mln)
# 2001  11.6
# 2002  12.3
# 2003  12.4
# 2004  13.0
# 2005  13.4
# Tel het aantal elementen
#
# Sla het python script op
# en voer het script uit in het terminal-venster onder in het scherm d.m.v. het commando 
# spark-submit Oefeningen-hoofdstuk-5-Groeperen-en-sorteren/Oefening1.py

sales = [(2000, 11.6),(2001,12.3),(2001,12.4),(2002,13.0),(2003,12.9),(2004,13.2),(2005,13.4)]
salesRdd = sc.parallelize(sales)

print("Aantal elementen: {}".format(salesRdd.count()))