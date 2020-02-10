import sys

#this code list all the words which have length greater than threshold

from pyspark import SparkContext, SparkConf

if __name__ == "__main__":

    # create Spark context with Spark configuration
    conf = SparkConf().setAppName("Spark New Count")
    sc = SparkContext(conf=conf)

    # get threshold
    threshold = int(sys.argv[2])
    
    # read in text file and split each document into words
    tokenized = sc.textFile(sys.argv[1]).flatMap(lambda line: line.split(" "))

    finalWord = tokenized.flatMap(lambda word: (word, 1) if len(word)>15 else "")
    
    # count the occurrence of each word
    wordCounts = finalWord.map(lambda word: (word, 1)).reduceByKey(lambda v1,v2:v1 +v2)
    
    # filter out words with fewer than threshold occurrences
    #filtered = wordCounts.filter(lambda pair:pair[1] >= threshold)

    #filter out the words smaller than given length
    filtered = wordCounts.filter(lambda pair:pair[1] >= threshold)
    #filtered = wordCounts.filter(lambda pair:pair[1] >= threshold)
    #filtered.collect().saveAsTextFile("books/output1/")
    #print all the resulting word
    #filtered.saveAsTextFile("/user/kumarlt/books/output2")
    list = filtered.collect()
    #list.rdd.saveAsTextFile("/user/kumarlt/books/output1")
    #print repr(list)[1:-1]
    for str1 in list:
        print str1
    #saveAsTextFile("books/output1")
    # count characters
    #charCounts = filtered.flatMap(lambda pair:pair[0]).map(lambda c: c).map(lambda c: (c, 1)).reduceByKey(lambda v1,v2:v1 +v2)
    
    #list = charCounts.collect()
    #print repr(list)[1:-1]
