folder='/user/girimh/data/weather/2019/'
file='970080-99999-2019.op'
file_path=folder+file
#file_path
'/user/girimh/data/weather/2019/970080-99999-2019.op'

inTextData = spark.read.format("csv").option("header", "true").option("delimiter", "\t").load(file_path)

inTextData.count()
inTextData.show(2, False)

name_list = inTextData.schema.names
#>>> name_list
#['STN--- WBAN   YEARMODA    TEMP       DEWP      SLP        STP       VISIB      WDSP     MXSPD   GUST    MAX     MIN   PRCP   SNDP   FRSHTT']
name_list = str(name_list).strip("['']").split(' ')
#>>> name_list
#['STN---', 'WBAN', '', '', 'YEARMODA', '', '', '', 'TEMP', '', '', '', '', '', '', 'DEWP', '', '', '', '', '', 'SLP', '', '', '', '', '', '', '', 'STP', '', '', '', '', '', '', 'VISIB', '', '', '', '', '', 'WDSP', '', '', '', '', 'MXSPD', '', '', 'GUST', '', '', '', 'MAX', '', '', '', '', 'MIN', '', '', 'PRCP', '', '', 'SNDP', '', '', 'FRSHTT']
names = [i for i in name_list if len(i) > 0]
#>>> names
#['STN---', 'WBAN', 'YEARMODA', 'TEMP', 'DEWP', 'SLP', 'STP', 'VISIB', 'WDSP', 'MXSPD', 'GUST', 'MAX', 'MIN', 'PRCP', 'SNDP', 'FRSHTT']

rdd1=inTextData.rdd

#>>> type(rdd1)
#<class 'pyspark.rdd.RDD'>
rdd2=rdd1.map(lambda x: str(x).split('=')[1])

rdd3 = rdd2.map(lambda x: ' '.join(x.split()))

rdd4 = rdd3.map(lambda x: x[2:-2])

type(rdd4)
# <class 'pyspark.rdd.PipelinedRDD'>

rdd4.saveAsTextFile(folder+'temp1')

# for 2018 main folder
#path = 'user/girimh/data/weather/2018/'
#rdd4.saveAsTextFile(path+'temp')
#newInData = spark.read.csv(path+'temp', header=False, sep=' ')
newInData.count()
#4011046


type(newInData)
# <class 'pyspark.sql.dataframe.DataFrame'>
newInData.show(10)
# +------+-----+--------+----+---+----+---+------+---+------+----+----+----+----+----+----+-----+-----+-----+-----+-----+------+
# |   _c0|  _c1|     _c2| _c3|_c4| _c5|_c6|   _c7|_c8|   _c9|_c10|_c11|_c12|_c13|_c14|_c15| _c16| _c17| _c18| _c19| _c20|  _c21|
# +------+-----+--------+----+---+----+---+------+---+------+----+----+----+----+----+----+-----+-----+-----+-----+-----+------+
# |970080|99999|20190101|79.4|  8|76.1|  8|1011.4|  8|1010.1|   8| 4.4|   8| 2.1|   8| 7.0|999.9|81.7*|76.6*|1.42C|999.9|010000|
# |970080|99999|20190102|81.9|  8|77.2|  8|1011.9|  8|1010.6|   8| 4.9|   8| 1.1|   8| 8.9|999.9| 86.7| 75.6|0.47A|999.9|010000|
# |970080|99999|20190103|81.3|  8|75.4|  8|1013.0|  8|1011.7|   8| 4.5|   8| 3.8|   8| 5.1|999.9| 86.7| 75.9|0.71B|999.9|010000|
# |970080|99999|20190104|81.6|  8|75.5|  8|1013.3|  7|1011.9|   7| 4.7|   8| 3.2|   8| 8.9|999.9| 86.5| 77.0|0.17C|999.9|010000|
# |970080|99999|20190105|80.8|  8|75.7|  8|1012.5|  8|1011.2|   8| 5.1|   8| 3.5|   8| 8.0|999.9| 86.0|76.1*|0.31A|999.9|010000|
# |970080|99999|20190106|81.4|  8|76.3|  8|1011.8|  8|1010.4|   8| 5.0|   8| 4.2|   8| 6.0|999.9| 86.0| 75.9|99.99|999.9|010000|
# |970080|99999|20190107|80.4|  8|75.3|  8|1011.5|  8|1010.1|   8| 4.4|   8| 3.3|   8| 6.0|999.9|81.3*| 76.5|0.46C|999.9|010000|
# |970080|99999|20190108|81.9|  8|74.4|  8|1011.2|  8|1010.6|   8| 5.0|   8| 7.0|   8|13.0|999.9| 86.0| 77.4|0.15B|999.9|010000|
# |970080|99999|20190109|78.8|  8|74.5|  8|1011.0|  8|1009.7|   8| 4.8|   8| 3.1|   8| 6.0|999.9|81.0*|76.6*|0.46C|999.9|010000|
# |970080|99999|20190110|78.4|  8|75.1|  8|1010.5|  8|1009.2|   8| 4.7|   8| 1.4|   8| 5.1|999.9| 85.6|75.2*|0.16A|999.9|010000|
# +------+-----+--------+----+---+----+---+------+---+------+----+----+----+----+----+----+-----+-----+-----+-----+-----+------+
# only showing top 10 rows


cleanData = newInData.drop('_c1','_c4','_c6','_c8','_c10','_c12','_c14')
# not dropping column _C1 (WBAN)
cleanData = newInData.drop('_c4','_c6','_c8','_c10','_c12','_c14')


cleanData.show(5)
# +------+--------+----+----+------+------+----+----+----+-----+-----+-----+-----+-----+------+
# |   _c0|     _c2| _c3| _c5|   _c7|   _c9|_c11|_c13|_c15| _c16| _c17| _c18| _c19| _c20|  _c21|
# +------+--------+----+----+------+------+----+----+----+-----+-----+-----+-----+-----+------+
# |970080|20190101|79.4|76.1|1011.4|1010.1| 4.4| 2.1| 7.0|999.9|81.7*|76.6*|1.42C|999.9|010000|
# |970080|20190102|81.9|77.2|1011.9|1010.6| 4.9| 1.1| 8.9|999.9| 86.7| 75.6|0.47A|999.9|010000|
# |970080|20190103|81.3|75.4|1013.0|1011.7| 4.5| 3.8| 5.1|999.9| 86.7| 75.9|0.71B|999.9|010000|
# |970080|20190104|81.6|75.5|1013.3|1011.9| 4.7| 3.2| 8.9|999.9| 86.5| 77.0|0.17C|999.9|010000|
# |970080|20190105|80.8|75.7|1012.5|1011.2| 5.1| 3.5| 8.0|999.9| 86.0|76.1*|0.31A|999.9|010000|
# +------+--------+----+----+------+------+----+----+----+-----+-----+-----+-----+-----+------+
# only showing top 5 rows

cleanData = cleanData.withColumnRenamed('_c0','STN').withColumnRenamed('_c2','YEARMODA')\
                     .withColumnRenamed('_c3','TEMP').withColumnRenamed('_c5','DEWP')\
                     .withColumnRenamed('_c7','SLP').withColumnRenamed('_c9','STP')\
                     .withColumnRenamed('_c11','VISIB').withColumnRenamed('_c13','WDSP')\
                     .withColumnRenamed('_c15','MXSPD').withColumnRenamed('_c16','GUST')\
                     .withColumnRenamed('_c17','MAX').withColumnRenamed('_c18','MIN')\
                     .withColumnRenamed('_c19','PRCP').withColumnRenamed('_c20','SNDP')\
                     .withColumnRenamed('_c21','FRSHTT')

# rename - not dropping column _C1 (WBAN)

cleanData = cleanData.withColumnRenamed('_c0','STN').withColumnRenamed('_c1','WBAN').withColumnRenamed('_c2','YEARMODA')\
                     .withColumnRenamed('_c3','TEMP').withColumnRenamed('_c5','DEWP')\
                     .withColumnRenamed('_c7','SLP').withColumnRenamed('_c9','STP')\
                     .withColumnRenamed('_c11','VISIB').withColumnRenamed('_c13','WDSP')\
                     .withColumnRenamed('_c15','MXSPD').withColumnRenamed('_c16','GUST')\
                     .withColumnRenamed('_c17','MAX').withColumnRenamed('_c18','MIN')\
                     .withColumnRenamed('_c19','PRCP').withColumnRenamed('_c20','SNDP')\
                     .withColumnRenamed('_c21','FRSHTT')


cleanData.printSchema()
cleanData = cleanData.withColumnRenamed('_c0','STN').withColumnRenamed('_c2','YEARMODA')\
                     .withColumnRenamed('_c3','TEMP').withColumnRenamed('_c5','DEWP')\
                     .withColumnRenamed('_c7','SLP').withColumnRenamed('_c9','STP')\
                     .withColumnRenamed('_c11','VISIB').withColumnRenamed('_c13','WDSP')\
                     .withColumnRenamed('_c15','MXSPD').withColumnRenamed('_c16','GUST')\
                     .withColumnRenamed('_c17','MAX').withColumnRenamed('_c18','MIN')\
                     .withColumnRenamed('_c19','PRCP').withColumnRenamed('_c20','SNDP')\
                     .withColumnRenamed('_c21','FRSHTT')
# root
#  |-- STN: string (nullable = true)
#  |-- YEARMODA: string (nullable = true)
#  |-- TEMP: string (nullable = true)
#  |-- DEWP: string (nullable = true)
#  |-- SLP: string (nullable = true)
#  |-- STP: string (nullable = true)
#  |-- VISIB: string (nullable = true)
#  |-- WDSP: string (nullable = true)
#  |-- MXSPD: string (nullable = true)
#  |-- GUST: string (nullable = true)
#  |-- MAX: string (nullable = true)
#  |-- MIN: string (nullable = true)
#  |-- PRCP: string (nullable = true)
#  |-- SNDP: string (nullable = true)
#  |-- FRSHTT: string (nullable = true)

cleanData.createOrReplaceTempView("weather")
spark.sql("SELECT * from weather").show()
# +------+--------+----+----+------+------+-----+----+-----+-----+-----+-----+-----+-----+------+
# |   STN|YEARMODA|TEMP|DEWP|   SLP|   STP|VISIB|WDSP|MXSPD| GUST|  MAX|  MIN| PRCP| SNDP|FRSHTT|
# +------+--------+----+----+------+------+-----+----+-----+-----+-----+-----+-----+-----+------+
# |970080|20190101|79.4|76.1|1011.4|1010.1|  4.4| 2.1|  7.0|999.9|81.7*|76.6*|1.42C|999.9|010000|
# |970080|20190102|81.9|77.2|1011.9|1010.6|  4.9| 1.1|  8.9|999.9| 86.7| 75.6|0.47A|999.9|010000|
# |970080|20190103|81.3|75.4|1013.0|1011.7|  4.5| 3.8|  5.1|999.9| 86.7| 75.9|0.71B|999.9|010000|
# |970080|20190104|81.6|75.5|1013.3|1011.9|  4.7| 3.2|  8.9|999.9| 86.5| 77.0|0.17C|999.9|010000|
# |970080|20190105|80.8|75.7|1012.5|1011.2|  5.1| 3.5|  8.0|999.9| 86.0|76.1*|0.31A|999.9|010000|
# |970080|20190106|81.4|76.3|1011.8|1010.4|  5.0| 4.2|  6.0|999.9| 86.0| 75.9|99.99|999.9|010000|
# |970080|20190107|80.4|75.3|1011.5|1010.1|  4.4| 3.3|  6.0|999.9|81.3*| 76.5|0.46C|999.9|010000|
# |970080|20190108|81.9|74.4|1011.2|1010.6|  5.0| 7.0| 13.0|999.9| 86.0| 77.4|0.15B|999.9|010000|
# |970080|20190109|78.8|74.5|1011.0|1009.7|  4.8| 3.1|  6.0|999.9|81.0*|76.6*|0.46C|999.9|010000|
# |970080|20190110|78.4|75.1|1010.5|1009.2|  4.7| 1.4|  5.1|999.9| 85.6|75.2*|0.16A|999.9|010000|
# |970080|20190111|78.8|75.6|1010.3|1008.9|  4.2| 3.5|  6.0|999.9| 83.5| 75.2|2.72C|999.9|010000|
# |970080|20190112|80.2|75.3|1009.9|1008.6|  5.0| 1.6|  4.1|999.9|81.7*| 76.6|0.16A|999.9|010000|
# |970080|20190113|80.7|75.7|1008.6|1007.3|  4.7| 4.6|  8.0|999.9| 85.3| 77.4|0.06B|999.9|010000|
# |970080|20190114|80.4|75.3|1008.3|1007.0|  4.3| 4.2|  6.0|999.9| 85.6|78.8*|0.55B|999.9|010000|
# |970080|20190115|81.1|73.7|1009.3|1008.0|  5.1| 6.0| 11.1|999.9| 85.1| 76.5|0.43B|999.9|010000|
# |970080|20190116|81.3|73.8|1009.4|1008.0|  5.3| 3.6|  8.9|999.9| 84.6| 78.1|0.00I|999.9|000000|
# |970080|20190117|79.2|74.8|1009.7|1008.4|  4.7| 2.0|  4.1|999.9| 84.6|75.6*|0.16A|999.9|010000|
# |970080|20190118|80.0|73.8|1010.0|1008.7|  5.0| 1.7|  5.1|999.9| 84.2| 75.9|0.00I|999.9|000000|
# |970080|20190119|79.5|74.2|1008.5|1007.2|  5.3| 2.0|  5.1|999.9| 84.9|74.1*|0.00I|999.9|000000|
# |970080|20190120|81.2|74.0|1007.2|1006.0|  5.6| 3.7|  9.9|999.9| 89.6| 72.7|0.00I|999.9|000000|
# +------+--------+----+----+------+------+-----+----+-----+-----+-----+-----+-----+-----+------+
# only showing top 20 rows

# - Queries


# question 4 -

# what if gust is only 999.9 for all rows?

spark.sql("SELECT MAX(GUST) from weather where not GUST = '999.9'").show()

# +---------+
# |max(GUST)|
# +---------+
# |     null|
# +---------+


#question 5 -   
spark.sql("SELECT MAX(PRCP) from weather where not PRCP = '99.99'").show()


#===============================================================================

# 2018/2018 data
spark.sql("SELECT * from weather").count()
# 4011046
spark.sql("SELECT MA from weather WHERE NOT STN=999999").show()
# +------+
# |   STN|
# +------+
# |911650|
# |911650|
# |911650|
# |911650|
# |911650|
# |911650|
# |911650|
# |911650|
# |911650|
# |911650|
# |911650|
# |911650|
# |911650|
# |911650|
# |911650|
# |911650|
# |911650|
# |911650|
# |911650|
# |911650|
# +------+


spark.sql("SELECT STN, GUST from weather WHERE NOT STN=999999 AND NOT GUST=999.9").show()
# +------+----+
# |   STN|GUST|
# +------+----+
# |911650|21.0|
# |911650|22.9|
# |911650|22.0|
# |911650|15.9|
# |911650|26.0|
# |911650|32.1|
# |911650|27.0|
# |911650|22.0|
# |911650|22.0|
# |911650|19.0|
# |911650|24.1|
# |911650|19.0|
# |911650|28.9|
# |911650|27.0|
# |911650|28.9|
# |911650|48.0|
# |911650|20.0|
# |911650|28.0|
# |911650|26.0|
# |911650|26.0|
# +------+----+
# only showing top 20 rows

spark.sql("SELECT MAX(GUST) from weather WHERE NOT STN=999999 AND NOT GUST=999.9").show()
# +---------+
# |max(GUST)|
# +---------+
# |     99.1|
# +---------+


spark.sql("SELECT STN, MAX(GUST) from weather WHERE NOT STN=999999 AND NOT GUST=999.9 GROUP BY STN").show()
# +------+---------+
# |   STN|max(GUST)|
# +------+---------+
# |010875|     83.9|
# |011350|      9.9|
# |013840|     42.0|
# |029810|     50.5|
# |030064|     62.0|
# |032570|     48.0|
# |033735|     49.0|
# |033920|     55.9|
# |066720|      9.9|
# |073160|     47.0|
# |082220|      9.9|
# |111710|     58.1|
# |150200|      9.7|
# |163445|     27.0|
# |232050|     21.4|
# |255610|      9.7|
# |287990|     31.1|
# |296450|     25.3|
# |404000|     18.1|
# |720361|     38.1|
# +------+---------+
# only showing top 20 rows

spark.sql("SELECT w1.STN from weather w1 WHERE GUST = (SELECT MAX(GUST) FROM WEATHER w2 WHERE w1.STN = w2.STN AND NOT w2.GUST=999.9)").show()
# +------+
# |   STN|
# +------+
# |023290|
# |450320|
# |226190|
# |341990|
# |672150|
# |716940|
# |823366|
# |291110|
# |725417|
# |725417|
# |122700|
# |432790|
# |859212|
# |471520|
# |711450|
# |710550|
# |084290|
# |720596|
# |028750|
# |119275|
# +------+
# only showing top 20 rows

# probable correct answer

spark.sql("SELECT STN from weather WHERE GUST = (SELECT MAX(GUST) FROM WEATHER WHERE NOT GUST=999.9)").show()
# +------+
# |   STN|
# +------+
# |477740|
# |726130|
# +------+

# including DATE IN PREVIOUS QUERY
spark.sql("SELECT STN, YEARMODA from weather WHERE GUST = (SELECT MAX(GUST) FROM WEATHER WHERE NOT GUST=999.9)").show()
# +------+--------+
# |   STN|YEARMODA|
# +------+--------+
# |477740|20180904|
# |726130|20181114|
# +------+--------+

#======================================================

# QUERY 3
spark.sql("SELECT STN, YEARMODA from weather WHERE PRCP = (SELECT MAX(PRCP) FROM WEATHER WHERE NOT PRCP=99.99)").show()
# +------+--------+
# |   STN|YEARMODA|
# +------+--------+
# |480950|20180528|
# +------+--------+

# change to include year in query


# Q2

>>> spark.sql("SELECT YEARMODA FROM WEATHER where MAX = (SELECT max(MAX) FROM WEATHER WHERE NOT MAX=9999.9)").show()
# +--------+
# |YEARMODA|
# +--------+
# |20180630|
# |20180910|
# |20180615|
# |20180725|
# |20180727|
# |20180828|
# |20180711|
# |20180713|
# |20180805|
# |20180808|
# |20180608|
# |20180904|
# |20180604|
# |20180725|
# |20180628|
# |20180705|
# |20180817|
# |20180530|
# |20180725|
# |20180726|
# +--------+
# only showing top 20 rows

spark.sql("SELECT YEARMODA,MAX FROM WEATHER where MAX = (SELECT max(MAX) FROM WEATHER WHERE NOT MAX=9999.9)").show()
# +--------+----+
# |YEARMODA| MAX|
# +--------+----+
# |20180630|99.9|
# |20180910|99.9|
# |20180615|99.9|
# |20180725|99.9|
# |20180727|99.9|
# |20180828|99.9|
# |20180711|99.9|
# |20180713|99.9|
# |20180805|99.9|
# |20180808|99.9|
# |20180608|99.9|
# |20180904|99.9|
# |20180604|99.9|
# |20180725|99.9|
# |20180628|99.9|
# |20180705|99.9|
# |20180817|99.9|
# |20180530|99.9|
# |20180725|99.9|
# |20180726|99.9|
# +--------+----+
# only showing top 20 rows

