import sys

#this code list all the words which have length greater than threshold

from pyspark import SparkContext, SparkConf


#function definition
def checkLength(inWord):
    if len(inWord)>15:
        return (inWord,1)
    else:
       return ""



#function definition
def checkLength2(inword):
        if inword[1]>15:
            return inword
        #else:
        #    return ""

#function definition
def checkLengthNew(inWord,wlen):
        if len(inWord)>wlen:
            #return (inWord,1)
            return inWord
        #else:
        #    return "" 


if __name__ == "__main__":

    # create Spark context with Spark configuration
    conf = SparkConf().setAppName("Spark Function Count")
    sc = SparkContext(conf=conf)

    # get threshold
    threshold = int(sys.argv[2])
    
    # read in text file and split each document into words
    tokenized = sc.textFile(sys.argv[1]).flatMap(lambda line: line.split(" "))

    #finalWord = tokenized.flatMap(lambda word: (word, 1) if len(word)>10 else "")
    finalWord = tokenized.flatMap(lambda word:checkLength(word))
    #finalWord = tokenized.map(lambda word:checkLength2(word))
    #finalWord = tokenized.map(lambda word:(word,len(word))).filter(lambda t:t[1]>15)
    finalWord = tokenized.map(lambda word:(word,len(word))).filter(lambda t:checkLength2(t))

    #finalWord = tokenized.map(lambda word:checkLength2(word))
    #finalWord = tokenized.map(lambda word:checkLengthNew(word,15))
    
    # count the occurrence of each word
    wordCounts = finalWord.map(lambda word: (word, 1)).reduceByKey(lambda v1,v2:v1 +v2)
    
    # filter out words with fewer than threshold occurrences
    #filtered = wordCounts.filter(lambda pair:pair[1] >= threshold)

    #filter out the words smaller than given length
    filtered = wordCounts.filter(lambda pair:pair[1] >= threshold)
    # save file
    filtered.collect().saveAsTextFile("spark-outputs/books/output0")

    #filtered = wordCounts.filter(lambda pair:pair[1] >= threshold)
    #filtered.collect().saveAsTextFile("books/output1/")
    #print all the resulting word
    #filtered.saveAsTextFile("/user/kumarlt/books/output5")
    #list = filtered.collect()
    #list.rdd.saveAsTextFile("/user/kumarlt/books/output1")
    #print repr(list)[1:-1]

    #saveAsTextFile("books/output1")
    #count characters
    charCounts = filtered.flatMap(lambda pair:pair[0]).map(lambda c: c).map(lambda c: (c, 1)).reduceByKey(lambda v1,v2:v1 +v2)
    charCounts.collect().saveAsTextFile("spark-outputs/books/output1")
    
    #list = charCounts.collect()
    #print repr(list)[1:-1]
