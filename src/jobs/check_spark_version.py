from pyspark import SparkContext
sc = SparkContext()
print("Version----")
print(sc._jvm.org.apache.hadoop.util.VersionInfo.getVersion())


