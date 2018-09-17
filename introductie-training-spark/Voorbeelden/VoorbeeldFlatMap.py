import sys
from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("flatMap").setMaster("local[1]")
sc = SparkContext(conf = conf)
sc.setLogLevel("ERROR")

regel1 = "Dit is de eerste regel"
regel2 = "van een stukje tekst met totaal 4 regels."
regel3 = "Het totaal aantal woorden in deze tekst is 29"
regel4 = "Dat zal blijken uit de flatMap transformatie"

tekst = sc.parallelize([regel1,regel2,regel3,regel4])
aantalRegels = tekst.map(lambda x:x.split(" ")).count()

aantalWoorden = tekst.flatMap(lambda x:x.split(" ")).count()
    
telling = tekst.flatMap(lambda x:x.split(" ")).countByValue()

print("aantal regels: {}".format(aantalRegels))
print("aantal woorden {}".format(aantalWoorden))

for woord, aantal in telling.items():
    print("{} : {}".format(woord, aantal))

