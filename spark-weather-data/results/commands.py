###################################################
# creating a list for question 2
hottest = []
coldest = []

# QUESTION 1 - 

# FOR YEAR 2010

df = spark.read.csv('spark-outputs/weather/2010/temp/part-00000', header=False, sep=' ')
df.count()
#3686244

cleanData = df.drop('_c4','_c6','_c8','_c10','_c12','_c14')
cleanData.show(2, False)
# +------+-----+--------+----+----+------+------+----+----+----+-----+-----+----+-----+-----+------+
# |_c0   |_c1  |_c2     |_c3 |_c5 |_c7   |_c9   |_c11|_c13|_c15|_c16 |_c17 |_c18|_c19 |_c20 |_c21  |
# +------+-----+--------+----+----+------+------+----+----+----+-----+-----+----+-----+-----+------+
# |010080|99999|20100101|23.2|19.0|9999.9|9999.9|7.0 |6.0 |15.9|999.9|33.8*|14.0|0.00H|999.9|001000|
# |010080|99999|20100102|20.5|16.4|9999.9|9999.9|6.2 |20.4|33.0|40.0 |33.4*|8.6*|0.00G|5.1  |001000|
# +------+-----+--------+----+----+------+------+----+----+----+-----+-----+----+-----+-----+------+


cleanData = cleanData.withColumnRenamed('_c0','STN').withColumnRenamed('_c1','WBAN').withColumnRenamed('_c2','YEARMODA')\
                      .withColumnRenamed('_c3','TEMP').withColumnRenamed('_c5','DEWP')\
                      .withColumnRenamed('_c7','SLP').withColumnRenamed('_c9','STP')\
                      .withColumnRenamed('_c11','VISIB').withColumnRenamed('_c13','WDSP')\
                      .withColumnRenamed('_c15','MXSPD').withColumnRenamed('_c16','GUST')\
                      .withColumnRenamed('_c17','MAX').withColumnRenamed('_c18','MIN')\
                      .withColumnRenamed('_c19','PRCP').withColumnRenamed('_c20','SNDP')\
                      .withColumnRenamed('_c21','FRSHTT')

convertedDF = cleanData.withColumn("TEMP", cleanData["TEMP"].cast("float"))\
                        .withColumn("DEWP", cleanData["DEWP"].cast("float"))\
                        .withColumn("SLP", cleanData["SLP"].cast("float"))\
                        .withColumn("STP", cleanData["STP"].cast("float"))\
                        .withColumn("VISIB", cleanData["VISIB"].cast("float"))\
                        .withColumn("WDSP", cleanData["WDSP"].cast("float"))\
                        .withColumn("MXSPD", cleanData["MXSPD"].cast("float"))\
                        .withColumn("GUST", cleanData["GUST"].cast("float"))\
                        .withColumn("MAX", cleanData["MAX"].cast("float"))\
                        .withColumn("MIN", cleanData["MIN"].cast("float"))\
                        .withColumn("PRCP", cleanData["PRCP"].cast("float"))\
                        .withColumn("SNDP", cleanData["SNDP"].cast("float"))

convertedDF.printSchema()
root
 |-- STN: string (nullable = true)
 |-- WBAN: string (nullable = true)
 |-- YEARMODA: string (nullable = true)
 |-- TEMP: float (nullable = true)
 |-- DEWP: float (nullable = true)
 |-- SLP: float (nullable = true)
 |-- STP: float (nullable = true)
 |-- VISIB: float (nullable = true)
 |-- WDSP: float (nullable = true)
 |-- MXSPD: float (nullable = true)
 |-- GUST: float (nullable = true)
 |-- MAX: float (nullable = true)
 |-- MIN: float (nullable = true)
 |-- PRCP: float (nullable = true)
 |-- SNDP: float (nullable = true)
 |-- FRSHTT: string (nullable = true)

spark.sql("Select MAX from weather2010 where MAX is not null and not max='9999.9' order by max desc").show()
# +-----+
# |  MAX|
# +-----+
# |128.8|
# |127.4|
# |126.5|
# |126.5|
# |126.0|
# |125.6|
# |125.6|
# |125.6|
# |125.6|
# |125.2|
# |124.7|
# |124.7|
# |124.5|
# |124.3|
# |124.2|
# |124.0|
# |123.8|
# |123.8|
# |123.8|
# |123.6|
# +-----+

spark.sql("select STN, YEARMODA AS DATE, MAX from weather2010 where MAX=(select MAX(MAX) from weather2010 where not MAX='9999.9' and not max='99.99')").show()
# +------+--------+-----+
# |   STN|    DATE|  MAX|
# +------+--------+-----+
# |703830|20100613|128.8|
# +------+--------+-----+

spark.sql("Select MIN from weather2010 where MIN is not null and not min='9999.9' order by min").show()
# +------+
# |   MIN|
# +------+
# |-115.2|
# |-112.5|
# |-112.2|
# |-110.6|
# |-110.4|
# |-110.4|
# |-110.0|
# |-109.7|
# |-109.3|
# |-109.1|
# |-108.9|
# |-108.8|
# |-107.9|
# |-107.9|
# |-107.7|
# |-107.3|
# |-107.1|
# |-106.8|
# |-106.4|
# |-105.3|
# +------+
# only showing top 20 rows

spark.sql("select STN, YEARMODA AS DATE, MIN from weather2010 where MIN=(select MIN(MIN) from weather2010 where not MIN='9999.9')").show()

# +------+--------+------+
# |   STN|    DATE|   MIN|
# +------+--------+------+
# |896060|20100802|-115.2|
# +------+--------+------+

# creating a list for question 2

hottest_2010 = ('703830','20100613',128.8)
hottest.append(hottest_2010)


coldest_2010 = ('896060','20100802',-115.2)
coldest.append(coldest_2010)

# >>> coldest
# [('896060', '20100802', -115.2)]
# >>> hottest
# [('703830', '20100613', 128.8)]


##################################################################
df2011 = spark.read.csv('spark-outputs/weather/2011/temp/part-00000', header=False, sep=' ')
df2011.count()
# 3559199

df2011Cleaned = df2011.drop('_c4','_c6','_c8','_c10','_c12','_c14')

df2011Cleaned = df2011Cleaned.withColumnRenamed('_c0','STN').withColumnRenamed('_c1','WBAN').withColumnRenamed('_c2','YEARMODA')\
                      .withColumnRenamed('_c3','TEMP').withColumnRenamed('_c5','DEWP')\
                      .withColumnRenamed('_c7','SLP').withColumnRenamed('_c9','STP')\
                      .withColumnRenamed('_c11','VISIB').withColumnRenamed('_c13','WDSP')\
                      .withColumnRenamed('_c15','MXSPD').withColumnRenamed('_c16','GUST')\
                      .withColumnRenamed('_c17','MAX').withColumnRenamed('_c18','MIN')\
                      .withColumnRenamed('_c19','PRCP').withColumnRenamed('_c20','SNDP')\
                      .withColumnRenamed('_c21','FRSHTT')

df2011Cleaned.show(4, False)
# +------+-----+--------+----+----+------+------+-----+----+-----+-----+-----+-----+-----+-----+------+
# |STN   |WBAN |YEARMODA|TEMP|DEWP|SLP   |STP   |VISIB|WDSP|MXSPD|GUST |MAX  |MIN  |PRCP |SNDP |FRSHTT|
# +------+-----+--------+----+----+------+------+-----+----+-----+-----+-----+-----+-----+-----+------+
# |010010|99999|20110101|23.3|15.6|1028.3|1027.1|14.3 |13.2|23.3 |999.9|27.0*|20.3 |0.02G|999.9|001000|
# |010010|99999|20110102|29.0|23.6|1017.4|1016.2|16.2 |10.6|15.5 |999.9|31.3 |22.8 |0.00G|999.9|010000|
# |010010|99999|20110103|29.2|26.5|1007.5|1006.3|8.5  |13.0|29.1 |999.9|32.4 |25.9*|0.00G|999.9|001000|
# |010010|99999|20110104|24.7|19.3|1013.2|1012.0|6.2  |24.9|31.1 |999.9|26.8*|21.6*|0.08G|999.9|001000|
# +------+-----+--------+----+----+------+------+-----+----+-----+-----+-----+-----+-----+-----+------+
