import sys

# this code list all the words which have length greater than threshold

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

folder = '/data/weather/2018/'
local_folder = 'weather/2018'

if __name__ == "__main__":
        # create Spark context with Spark configuration
    conf = SparkConf().setAppName("Spark P3 v1")
    spark = SparkSession.builder.appName(
        "Spark P3 SparkSession v1").getOrCreate()
    # spark = SparkContext(conf=conf)

    #in_path = folder + str(i) + '/'
    # print(path)
    inTextData = spark.read.format("csv").option(
        "header", "true").option("delimiter", "\t").load(folder)
    name_list = inTextData.schema.names
    print(name_list)
    name_list = str(name_list).strip("['']").split(' ')
    print(name_list)
    names = [i for i in name_list if len(i) > 0]
    print(names)
    rdd1 = inTextData.rdd
    rdd1.take(4, False)
    rdd2 = rdd1.map(lambda x: str(x).split('=')[1])
    rdd3 = rdd2.map(lambda x: ' '.join(x.split()))
    rdd4 = rdd3.map(lambda x: x[2:-2])
    rdd4.saveAsTextFile(local_folder)
        # >>> type(rdd1)
        # <class 'pyspark.rdd.RDD'>
        # rdd2 = rdd1.map(lambda x: str(x).split('=')[1])

        # rdd3 = rdd2.map(lambda x: ' '.join(x.split()))

        # rdd4 = rdd3.map(lambda x: x[2:-2])

        # local_path = local_folder + str(i) + '/temp'

        # rdd4.saveAsTextFile(local_path)

        #newInData = spark.read.csv(path+'temp', header=False, sep=' ')
