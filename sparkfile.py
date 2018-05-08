from pyspark import SparkContext
from pyspark import SparkFiles
finddistance = "/home/srimlcloud/temp_dat/pySpark_projects/finddistance.R"
finddistancename = "finddistance.R"
sc = SparkContext("local", "SparkFile App")
sc.addFile(finddistance)
print "Absolute Path -> %s" % SparkFiles.get(finddistancename)
