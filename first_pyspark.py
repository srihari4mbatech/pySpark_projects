'''firapp.py'''

from pyspark import SparkContext
logfile='file:////home/srimlcloud/temp_dat/sample.txt'
sc=SparkContext("local","first app")
logData=sc.textFile(logfile).cache()
numAs=logData.filter(lambda s: 'a' in s).count()
numBs=logData.filter(lambda s:'b' in s).count()
print("Lines with a: %i, lines with b: %i" %(numAs,numBs))

