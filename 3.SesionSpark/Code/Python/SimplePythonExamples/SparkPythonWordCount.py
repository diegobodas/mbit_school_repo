import sys
from pyspark import SparkContext, SparkConf

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: wordcount <inputfile> <outputdir> threshold")
        exit(-1)
    # create Spark context with Spark configuration
    conf = SparkConf().setAppName("Spark Count")
    sc = SparkContext(conf=conf)

    # read in text file and split each document into words
    # sys.argv[0]py --- in command line with python. I.e. -c

    text_file = sc.textFile(sys.argv[1])

    # non empty lines
    lines_nonempty = text_file.filter(lambda x: len(x) > 0)
    print("###########################################")
    print(lines_nonempty.count())
    print("###########################################")

    counts = text_file.flatMap(lambda line: line.split(" ")).map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)

    # get threshold
    threshold = int(sys.argv[3])
    # filter threshold
    filtered = counts.filter(lambda pair: pair[1] >= threshold)

    # save
    filtered.saveAsTextFile(sys.argv[2])

    # print
    print 'Top 100 words:'
    print filtered.take(100)
