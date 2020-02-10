# QUESTION 1


# YEAR 2010

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



cleanData.show(4, False)
# +------+-----+--------+----+----+------+------+-----+----+-----+-----+-----+----+-----+-----+------+
# |STN   |WBAN |YEARMODA|TEMP|DEWP|SLP   |STP   |VISIB|WDSP|MXSPD|GUST |MAX  |MIN |PRCP |SNDP |FRSHTT|
# +------+-----+--------+----+----+------+------+-----+----+-----+-----+-----+----+-----+-----+------+
# |010080|99999|20100101|23.2|19.0|9999.9|9999.9|7.0  |6.0 |15.9 |999.9|33.8*|14.0|0.00H|999.9|001000|
# |010080|99999|20100102|20.5|16.4|9999.9|9999.9|6.2  |20.4|33.0 |40.0 |33.4*|8.6*|0.00G|5.1  |001000|
# |010080|99999|20100103|6.9 |-3.7|9999.9|9999.9|7.2  |14.1|21.4 |999.9|9.7* |4.5*|0.04G|5.1  |001000|
# |010080|99999|20100104|4.9 |-6.2|9999.9|9999.9|8.7  |13.1|19.4 |999.9|6.8* |3.2*|0.00G|999.9|001000|
# +------+-----+--------+----+----+------+------+-----+----+-----+-----+-----+----+-----+-----+------+
# only showing top 4 rows

cleanData.printSchema()
# root
#  |-- STN: string (nullable = true)
#  |-- WBAN: string (nullable = true)
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

cleanData.createOrReplaceTempView("weather2010uncleaned")

# HOTTEST DAY
spark.sql("select STN, YEARMODA AS DATE, cast(replace(MAX,'*') as float) from weather2010uncleaned where cast(replace(MAX,'*') as float)=(select MAX(cast(replace(MAX,'*') as float)) from weather2010uncleaned where not MAX='9999.9')").show()

# COLDEST DAY
spark.sql("select STN, YEARMODA AS DATE, cast(replace(MIN,'*') as float) from weather2010uncleaned where cast(replace(MIN,'*') as float)=(select MIN(cast(replace(MIN,'*') as float)) from weather2010uncleaned where not MIN='9999.9')").show()


hottest = []
coldest = []


hottest_2010=('720667', '20100923', '132.8')
hottest[0] = hottest_2010
coldest_2010 =('896060', '20100802', '-115.2')
coldest[0] = coldest_2010



# YEAR 2011

df2011 = spark.read.csv('spark-outputs/weather/2011/temp/part-00000', header=False, sep=' ')

df2011.count()
#3559199
df2011Cleaned = df2011.drop('_c4','_c6','_c8','_c10','_c12','_c14')


df2011Cleaned = df2011Cleaned.withColumnRenamed('_c0','STN').withColumnRenamed('_c1','WBAN').withColumnRenamed('_c2','YEARMODA')\
                       .withColumnRenamed('_c3','TEMP').withColumnRenamed('_c5','DEWP')\
                       .withColumnRenamed('_c7','SLP').withColumnRenamed('_c9','STP')\
                       .withColumnRenamed('_c11','VISIB').withColumnRenamed('_c13','WDSP')\
                       .withColumnRenamed('_c15','MXSPD').withColumnRenamed('_c16','GUST')\
                       .withColumnRenamed('_c17','MAX').withColumnRenamed('_c18','MIN')\
                       .withColumnRenamed('_c19','PRCP').withColumnRenamed('_c20','SNDP')\
                       .withColumnRenamed('_c21','FRSHTT')


df2011Cleaned.createOrReplaceTempView("weather2011")
spark.sql("select count(*) from weather2011").show()

# +--------+
# |count(1)|
# +--------+
# | 3559199|
# +--------+

spark.sql("select STN, YEARMODA AS DATE, cast(replace(MAX,'*') as float) from weather2011 where cast(replace(MAX,'*') as float)=(select MAX(cast(replace(MAX,'*') as float)) from weather2011 where not MAX='9999.9')").show()


spark.sql("select STN, YEARMODA AS DATE, cast(replace(MIN,'*') as float) from weather2011 where cast(replace(MIN,'*') as float)=(select MIN(cast(replace(MIN,'*') as float)) from weather2011 where not MIN='9999.9')").show()

# add to global lists

hottest_2011=('720293', '20110613', '131.0')
hottest[1] = hottest_2011

coldest_2011=('897340', '20110917', '-111.8')
coldest[1] = coldest_2011


# ************************************************

# 2012

df2012 = spark.read.csv('spark-outputs/weather/2012/temp/part-00000', header=False, sep=' ')
df2012.count()
#3739198

df2012Cleaned = df2012.drop('_c4','_c6','_c8','_c10','_c12','_c14')


df2012Cleaned = df2012Cleaned.withColumnRenamed('_c0','STN').withColumnRenamed('_c1','WBAN').withColumnRenamed('_c2','YEARMODA')\
                       .withColumnRenamed('_c3','TEMP').withColumnRenamed('_c5','DEWP')\
                       .withColumnRenamed('_c7','SLP').withColumnRenamed('_c9','STP')\
                       .withColumnRenamed('_c11','VISIB').withColumnRenamed('_c13','WDSP')\
                       .withColumnRenamed('_c15','MXSPD').withColumnRenamed('_c16','GUST')\
                       .withColumnRenamed('_c17','MAX').withColumnRenamed('_c18','MIN')\
                       .withColumnRenamed('_c19','PRCP').withColumnRenamed('_c20','SNDP')\
                       .withColumnRenamed('_c21','FRSHTT')

df2012Cleaned.show(4, False)

df2012Cleaned.createOrReplaceTempView("weather2012")

spark.sql("select STN, YEARMODA AS DATE, cast(replace(MAX,'*') as float) from weather2012 where cast(replace(MAX,'*') as float)=(select MAX(cast(replace(MAX,'*') as float)) from weather2012 where not MAX='9999.9')").show()

# +------+--------+--------------------------------+
# |   STN|    DATE|CAST(replace(MAX, *, ) AS FLOAT)|
# +------+--------+--------------------------------+
# |722577|20120712|                           132.8|
# +------+--------+--------------------------------+


spark.sql("select STN, YEARMODA AS DATE, cast(replace(MIN,'*') as float) from weather2012 where cast(replace(MIN,'*') as float)=(select MIN(cast(replace(MIN,'*') as float)) from weather2012 where not MIN='9999.9')").show()
# +------+--------+--------------------------------+
# |   STN|    DATE|CAST(replace(MIN, *, ) AS FLOAT)|
# +------+--------+--------------------------------+
# |896060|20120916|                          -119.6|
# +------+--------+--------------------------------+


hottest_2012=('722577', '20120712', '132.8')
hottest.append(hottest_2012)

coldest_2012=('896060', '20120916', '-119.6')
coldest.append(coldest_2012)


# *********************************************************************************

#2013

df2013 = spark.read.csv('spark-outputs/weather/2013/temp/part-00000', header=False, sep=' ')
df2013.count()

df2013Cleaned = df2013.drop('_c4','_c6','_c8','_c10','_c12','_c14')

df2013Cleaned = df2013Cleaned.withColumnRenamed('_c0','STN').withColumnRenamed('_c1','WBAN').withColumnRenamed('_c2','YEARMODA')\
                       .withColumnRenamed('_c3','TEMP').withColumnRenamed('_c5','DEWP')\
                       .withColumnRenamed('_c7','SLP').withColumnRenamed('_c9','STP')\
                       .withColumnRenamed('_c11','VISIB').withColumnRenamed('_c13','WDSP')\
                       .withColumnRenamed('_c15','MXSPD').withColumnRenamed('_c16','GUST')\
                       .withColumnRenamed('_c17','MAX').withColumnRenamed('_c18','MIN')\
                       .withColumnRenamed('_c19','PRCP').withColumnRenamed('_c20','SNDP')\
                       .withColumnRenamed('_c21','FRSHTT')


df2013Cleaned.createOrReplaceTempView("weather2013")

spark.sql("select STN, YEARMODA AS DATE, cast(replace(MAX,'*') as float) from weather2013 where cast(replace(MAX,'*') as float)=(select MAX(cast(replace(MAX,'*') as float)) from weather2013 where not MAX='9999.9')").show()

# +------+--------+--------------------------------+
# |   STN|    DATE|CAST(replace(MAX, *, ) AS FLOAT)|
# +------+--------+--------------------------------+
# |406890|20130712|                           132.8|
# +------+--------+--------------------------------+


spark.sql("select STN, YEARMODA AS DATE, cast(replace(MIN,'*') as float) from weather2013 where cast(replace(MIN,'*') as float)=(select MIN(cast(replace(MIN,'*') as float)) from weather2013 where not MIN='9999.9')").show()

# +------+--------+--------------------------------+
# |   STN|    DATE|CAST(replace(MIN, *, ) AS FLOAT)|
# +------+--------+--------------------------------+
# |895770|20130730|                          -115.1|
# |895770|20130731|                          -115.1|
# +------+--------+--------------------------------+


hottest_2013=('406890', '20130712', '132.8')
hottest.append(hottest_2013)
coldest_2013=('895770', '20130730', '-115.1')
coldest.append(coldest_2013)


#*********************************************************************************
#2014

df2014 = spark.read.csv('spark-outputs/weather/2014/temp/part-00000', header=False, sep=' ')


df2014Cleaned = df2014.drop('_c4','_c6','_c8','_c10','_c12','_c14')


df2014Cleaned = df2014Cleaned.withColumnRenamed('_c0','STN').withColumnRenamed('_c1','WBAN').withColumnRenamed('_c2','YEARMODA')\
                       .withColumnRenamed('_c3','TEMP').withColumnRenamed('_c5','DEWP')\
                       .withColumnRenamed('_c7','SLP').withColumnRenamed('_c9','STP')\
                       .withColumnRenamed('_c11','VISIB').withColumnRenamed('_c13','WDSP')\
                       .withColumnRenamed('_c15','MXSPD').withColumnRenamed('_c16','GUST')\
                       .withColumnRenamed('_c17','MAX').withColumnRenamed('_c18','MIN')\
                       .withColumnRenamed('_c19','PRCP').withColumnRenamed('_c20','SNDP')\
                       .withColumnRenamed('_c21','FRSHTT')

df2014Cleaned.createOrReplaceTempView("weather2014")

spark.sql("select STN, YEARMODA AS DATE, cast(replace(MAX,'*') as float) from weather2014 where cast(replace(MAX,'*') as float)=(select MAX(cast(replace(MAX,'*') as float)) from weather2014 where not MAX='9999.9')").show()

spark.sql("select STN, YEARMODA AS DATE, cast(replace(MIN,'*') as float) from weather2014 where cast(replace(MIN,'*') as float)=(select MIN(cast(replace(MIN,'*') as float)) from weather2014 where not MIN='9999.9')").show()

hottest_2014=('406650', '20140803', '129.6')
hottest.append(hottest_2014)
coldest_2014=('896060', '20140821', '-113.4')
coldest.append(coldest_2014)

#*********************************************************************************
#2015

df2015 = spark.read.csv('spark-outputs/weather/2015/temp/part-00000', header=False, sep=' ')


df2015Cleaned = df2015.drop('_c4','_c6','_c8','_c10','_c12','_c14')


df2015Cleaned = df2015Cleaned.withColumnRenamed('_c0','STN').withColumnRenamed('_c1','WBAN').withColumnRenamed('_c2','YEARMODA')\
                       .withColumnRenamed('_c3','TEMP').withColumnRenamed('_c5','DEWP')\
                       .withColumnRenamed('_c7','SLP').withColumnRenamed('_c9','STP')\
                       .withColumnRenamed('_c11','VISIB').withColumnRenamed('_c13','WDSP')\
                       .withColumnRenamed('_c15','MXSPD').withColumnRenamed('_c16','GUST')\
                       .withColumnRenamed('_c17','MAX').withColumnRenamed('_c18','MIN')\
                       .withColumnRenamed('_c19','PRCP').withColumnRenamed('_c20','SNDP')\
                       .withColumnRenamed('_c21','FRSHTT')

df2015Cleaned.createOrReplaceTempView("weather2015")

spark.sql("select STN, YEARMODA AS DATE, cast(replace(MAX,'*') as float) from weather2015 where cast(replace(MAX,'*') as float)=(select MAX(cast(replace(MAX,'*') as float)) from weather2015 where not MAX='9999.9')").show()

spark.sql("select STN, YEARMODA AS DATE, cast(replace(MIN,'*') as float) from weather2015 where cast(replace(MIN,'*') as float)=(select MIN(cast(replace(MIN,'*') as float)) from weather2015 where not MIN='9999.9')").show()


hottest_2015=('916700', '20151021', '132.4')
coldest_2015=('895770', '20150917', '-114.2')
hottest.append(hottest_2015)
coldest.append(coldest_2015)



#*********************************************************************************
#2016

df2016 = spark.read.csv('spark-outputs/weather/2016/temp/part-00000', header=False, sep=' ')


df2016Cleaned = df2016.drop('_c4','_c6','_c8','_c10','_c12','_c14')


df2016Cleaned = df2016Cleaned.withColumnRenamed('_c0','STN').withColumnRenamed('_c1','WBAN').withColumnRenamed('_c2','YEARMODA')\
                       .withColumnRenamed('_c3','TEMP').withColumnRenamed('_c5','DEWP')\
                       .withColumnRenamed('_c7','SLP').withColumnRenamed('_c9','STP')\
                       .withColumnRenamed('_c11','VISIB').withColumnRenamed('_c13','WDSP')\
                       .withColumnRenamed('_c15','MXSPD').withColumnRenamed('_c16','GUST')\
                       .withColumnRenamed('_c17','MAX').withColumnRenamed('_c18','MIN')\
                       .withColumnRenamed('_c19','PRCP').withColumnRenamed('_c20','SNDP')\
                       .withColumnRenamed('_c21','FRSHTT')

df2016Cleaned.createOrReplaceTempView("weather2016")

spark.sql("select STN, YEARMODA AS DATE, cast(replace(MAX,'*') as float) from weather2016 where cast(replace(MAX,'*') as float)=(select MAX(cast(replace(MAX,'*') as float)) from weather2016 where not MAX='9999.9')").show()

spark.sql("select STN, YEARMODA AS DATE, cast(replace(MIN,'*') as float) from weather2016 where cast(replace(MIN,'*') as float)=(select MIN(cast(replace(MIN,'*') as float)) from weather2016 where not MIN='9999.9')").show()


hottest_2016=('700638', '20160622', '129.0')
coldest_2016=('896060', '20160712', '-115.1')
# forgot to append 2016 here, added at the very end
coldest.append(coldest_2016)

#*********************************************************************************
#2017

df2017 = spark.read.csv('spark-outputs/weather/2017/temp/part-00000', header=False, sep=' ')


df2017Cleaned = df2017.drop('_c4','_c6','_c8','_c10','_c12','_c14')


df2017Cleaned = df2017Cleaned.withColumnRenamed('_c0','STN').withColumnRenamed('_c1','WBAN').withColumnRenamed('_c2','YEARMODA')\
                       .withColumnRenamed('_c3','TEMP').withColumnRenamed('_c5','DEWP')\
                       .withColumnRenamed('_c7','SLP').withColumnRenamed('_c9','STP')\
                       .withColumnRenamed('_c11','VISIB').withColumnRenamed('_c13','WDSP')\
                       .withColumnRenamed('_c15','MXSPD').withColumnRenamed('_c16','GUST')\
                       .withColumnRenamed('_c17','MAX').withColumnRenamed('_c18','MIN')\
                       .withColumnRenamed('_c19','PRCP').withColumnRenamed('_c20','SNDP')\
                       .withColumnRenamed('_c21','FRSHTT')

df2017Cleaned.createOrReplaceTempView("weather2017")

spark.sql("select STN, YEARMODA AS DATE, cast(replace(MAX,'*') as float) from weather2017 where cast(replace(MAX,'*') as float)=(select MAX(cast(replace(MAX,'*') as float)) from weather2017 where not MAX='9999.9')").show()

spark.sql("select STN, YEARMODA AS DATE, cast(replace(MIN,'*') as float) from weather2017 where cast(replace(MIN,'*') as float)=(select MIN(cast(replace(MIN,'*') as float)) from weather2017 where not MIN='9999.9')").show()

hottest_2017=('917430', '20170410', '129.6')
hottest.append(hottest_2017)
coldest_2017=('896250', '20170620', '-116.0')
coldest.append(coldest_2017)


#*********************************************************************************
#2018

df2018 = spark.read.csv('spark-outputs/weather/2018/temp/part-00000', header=False, sep=' ')


df2018Cleaned = df2018.drop('_c4','_c6','_c8','_c10','_c12','_c14')


df2018Cleaned = df2018Cleaned.withColumnRenamed('_c0','STN').withColumnRenamed('_c1','WBAN').withColumnRenamed('_c2','YEARMODA')\
                       .withColumnRenamed('_c3','TEMP').withColumnRenamed('_c5','DEWP')\
                       .withColumnRenamed('_c7','SLP').withColumnRenamed('_c9','STP')\
                       .withColumnRenamed('_c11','VISIB').withColumnRenamed('_c13','WDSP')\
                       .withColumnRenamed('_c15','MXSPD').withColumnRenamed('_c16','GUST')\
                       .withColumnRenamed('_c17','MAX').withColumnRenamed('_c18','MIN')\
                       .withColumnRenamed('_c19','PRCP').withColumnRenamed('_c20','SNDP')\
                       .withColumnRenamed('_c21','FRSHTT')

df2018Cleaned.createOrReplaceTempView("weather2018")

spark.sql("select STN, YEARMODA AS DATE, cast(replace(MAX,'*') as float) from weather2018 where cast(replace(MAX,'*') as float)=(select MAX(cast(replace(MAX,'*') as float)) from weather2018 where not MAX='9999.9')").show()

spark.sql("select STN, YEARMODA AS DATE, cast(replace(MIN,'*') as float) from weather2018 where cast(replace(MIN,'*') as float)=(select MIN(cast(replace(MIN,'*') as float)) from weather2018 where not MIN='9999.9')").show()

hottest_2018=('408110', '20180702', '126.3')
coldest_2018=('896060', '20180828', '-116.3')
hottest.append(hottest_2018)
coldest.append(coldest_2018)


#*********************************************************************************
#2019

df2019 = spark.read.csv('spark-outputs/weather/2019/temp/part-00000', header=False, sep=' ')


df2019Cleaned = df2019.drop('_c4','_c6','_c8','_c10','_c12','_c14')


df2019Cleaned = df2019Cleaned.withColumnRenamed('_c0','STN').withColumnRenamed('_c1','WBAN').withColumnRenamed('_c2','YEARMODA')\
                       .withColumnRenamed('_c3','TEMP').withColumnRenamed('_c5','DEWP')\
                       .withColumnRenamed('_c7','SLP').withColumnRenamed('_c9','STP')\
                       .withColumnRenamed('_c11','VISIB').withColumnRenamed('_c13','WDSP')\
                       .withColumnRenamed('_c15','MXSPD').withColumnRenamed('_c16','GUST')\
                       .withColumnRenamed('_c17','MAX').withColumnRenamed('_c18','MIN')\
                       .withColumnRenamed('_c19','PRCP').withColumnRenamed('_c20','SNDP')\
                       .withColumnRenamed('_c21','FRSHTT')

df2019Cleaned.createOrReplaceTempView("weather2019")

spark.sql("select STN, YEARMODA AS DATE, cast(replace(MAX,'*') as float) from weather2019 where cast(replace(MAX,'*') as float)=(select MAX(cast(replace(MAX,'*') as float)) from weather2019 where not MAX='9999.9')").show()

spark.sql("select STN, YEARMODA AS DATE, cast(replace(MIN,'*') as float) from weather2019 where cast(replace(MIN,'*') as float)=(select MIN(cast(replace(MIN,'*') as float)) from weather2019 where not MIN='9999.9')").show()


hottest_2019=('956660', '20190124', '121.1')
coldest_2019=('896060', '20190405', '-102.1')
hottest.append(hottest_2019)
coldest.append(coldest_2019)


# BUG FIXED - fix missing 2016 value in hottest
hottest_2016=('700638', '20160622', '129.0')
hottest.append(hottest_2016)


# ***********************************************************************
# ***********************************************************************
# ***********************************************************************
#  QUESTION 2- HOTTEST and COLDEST DAY ACROSS ALL YEARS

# HOTTEST DAY
sorted(hottest, key=lambda t: t[2], reverse=True)[0]
#('720667', '20100923', '132.8')

# NOTE - Multiple years have the same max temp - 132.8
sorted(hottest, key=lambda t: t[2], reverse=True)
#[('720667', '20100923', '132.8'), ('722577', '20120712', '132.8'), ('406890', '20130712', '132.8'), ('916700', '20151021', '132.4'), ('720293', '20110613', '131.0'), ('406650', '20140803', '129.6'), ('917430', '20170410', '129.6'), ('700638', '20160622', '129.0'), ('408110', '20180702', '126.3'), ('956660', '20190124', '121.1')]

sorted(coldest, key=lambda t: t[2], reverse=True)
#[('896060', '20120916', '-119.6'), ('896060', '20180828', '-116.3'), ('896250', '20170620', '-116.0'), ('896060', '20100802', '-115.2'), ('895770', '20130730', '-115.1'), ('896060', '20160712', '-115.1'), ('895770', '20150917', '-114.2'), ('896060', '20140821', '-113.4'), ('897340', '20110917', '-111.8'), ('896060', '20190405', '-102.1')]

# COLDEST DAY
sorted(coldest, key=lambda t: t[2], reverse=True)[0]
#('896060', '20120916', '-119.6')



# ***********************************************************************
# ***********************************************************************
# ***********************************************************************

#  QUESTION 3 -  MAXIMUM AND MINIMUM PRECIPITATION WITH STN CODE AND DATE FOR YEAR 2015
# per readme.txt, FLAG in PRCP column can be any character from A through G, so regex is required to replace these characters
# and then cast to float

spark.sql("select STN, YEARMODA AS DATE, cast(regexp_replace(PRCP, '(\w{1})', '') as float) from weather2015 where cast(regexp_replace(PRCP, '(\w{1})', '') as float)=(select MAX(cast(regexp_replace(PRCP, '(\w{1})', '') as float)) from weather2015 where not PRCP='99.99')").show()
# +------+--------+---------------------------------------------+
# |   STN|    DATE|CAST(regexp_replace(PRCP, (w{1}), ) AS FLOAT)|
# +------+--------+---------------------------------------------+
# |479120|20150808|                                        16.34|
#+------+--------+---------------------------------------------+

spark.sql("select STN, YEARMODA AS DATE, cast(regexp_replace(PRCP, '(\w{1})', '') as float) from weather2015 where cast(regexp_replace(PRCP, '(\w{1})', '') as float)=(select MIN(cast(regexp_replace(PRCP, '(\w{1})', '') as float)) from weather2015 where not PRCP='99.99')").show()




# ***********************************************************************
# ***********************************************************************
# ***********************************************************************

# Question 4- COUNT PERCENTAGE OF MISSING VALUES FOR MEAN STATION PRESSURE (STP) FOR YEAR 2019


spark.sql("select count(*) from weather2019 where STP='9999.9'").show()

# +--------+
# |count(1)|
# +--------+
# |  330144|
# +--------+


spark.sql("select count(*) from weather2019 where not STP='9999.9'").show()
# +--------+
# |count(1)|
# +--------+
# |  850580|
# +--------+




# ***********************************************************************
# ***********************************************************************
# ***********************************************************************

# Question 5 - STN CODE AND DATE FOR YEAR 2019 WITH MAXIMUM WIND GUST


spark.sql("select STN, YEARMODA as DATE, GUST from weather2019 where GUST=(SELECT MAX(GUST) FROM weather2019 where not gust='999.9')").show()

spark.sql("select STN, YEARMODA as DATE, GUST from weather2019 where not gust='999.9' order by GUST DESC").show()
# +------+--------+----+
# |   STN|    DATE|GUST|
# +------+--------+----+
# |726130|20190225|99.1|
# |726130|20190209|97.9|
# |726130|20190101|97.9|
# |726130|20190226|96.9|
# |726130|20190416|96.0|
# |726130|20190403|96.0|
# |726130|20190404|96.0|
# |116530|20190117|95.2|
# |030390|20190303|94.0|
# |726130|20190122|94.0|
# |726130|20190109|94.0|
# |077490|20190203|93.0|
# |726130|20190323|93.0|
# |726130|20190224|92.1|
# |111550|20190315|91.3|
# |144540|20190223|91.3|
# |060220|20190318|91.3|
# |111550|20190316|91.3|
# |712450|20190312|90.9|
# |726130|20190106|90.9|
# +------+--------+----+
# only showing top 20 rows