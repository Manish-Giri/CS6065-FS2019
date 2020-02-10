import sys

# this code performs part 1 of the cleanup as shown in "temp_test.html"
# ie: read each year data from /data/weather/ , clean it up and save it in my 
# local HDFS directory under spark-outputs/weather/
# after this step, each year input data will be read from spark-outputs/weather/ manually in PySpark shell 
# to perform SQL operations

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

input_folder = '/data/weather/'
local_folder = 'spark-outputs/weather/'
years = [2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019]
if __name__ == "__main__":
        # create Spark context with Spark configuration
    #conf = SparkConf().setAppName("Spark P3 v1")
    spark = SparkSession.builder.appName("Spark P3 All Years Part1 v1)".config("spark.executor.memory", "14G").getOrCreate()
    # spark = SparkContext(conf=conf)
    for i in years:
	year = str(i)
        in_path = input_folder + year + '/'
        # print(path)
        inTextData = spark.read.format("csv").option(
            "header", "true").option("delimiter", "\t").load(in_path)
        name_list = inTextData.schema.names
        #print(name_list)
        name_list = str(name_list).strip("['']").split(' ')
        #print(name_list)
        names = [i for i in name_list if len(i) > 0]
        # print(names)
        rdd1 = inTextData.rdd
        #rdd1.take(4, False)
        
        # >>> type(rdd1)
        # <class 'pyspark.rdd.RDD'>
        rdd2 = rdd1.map(lambda x: str(x).split('=')[1])

        rdd3 = rdd2.map(lambda x: ' '.join(x.split()))

        rdd4 = rdd3.map(lambda x: x[2:-2])

        local_path = local_folder + year + '/temp/'

        rdd4.coalesce(1).saveAsTextFile(local_path)

        
