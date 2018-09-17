import sys
from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("Introductietraining-Spark").setMaster("local[1]")
sc = SparkContext(conf = conf)
sc.setLogLevel("ERROR")

# Oefening 1
# Lees de bestanden steden.csv en landen1.csv in en koppel de bestanden aan elkaar
#
# Sla het python script op
# en voer het script uit in het terminal-venster onder in het scherm d.m.v. het commando 
# spark-submit Oefeningen-hoofdstuk-6-Joins/Oefening1.py

stedenRdd = sc.textFile("bestanden/steden.csv")
landenRdd = sc.textFile("bestanden/landen1.csv")

steden = stedenRdd.map(lambda x:x.split(",")).map(lambda x:(x[1].strip(),x[0]))
landen = landenRdd.map(lambda x:x.split(",")).map(lambda x:(x[0].strip(),x[1]))

print("Aantal steden: {}".format(steden.count()))
print("Aantal landen: {}".format(landen.count()))
combi = steden.leftOuterJoin(landen)

for code, stad in combi.collect():
    print("{}, {}, {}".format(code, stad[0], stad[1]))
