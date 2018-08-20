import sys
import os

from pyspark import SparkContext
from pyspark import SparkConf
# os.environ['HADOOP_HOME'] = "F:\\Product2\\hadoop\\"
# os.environ['SPARK_HOME'] = "F:\\Product2\\spark-2.1.0-bin-hadoop2.7\\"
# sys.path.append("F:\\Product2\\spark-2.1.0-bin-hadoop2.7")

sc = SparkContext('local', 'first app')
recs = sc.textFile('dna_seq.txt')
"""
# Test:1
def nonMap():
    try:

        print(recs.collect())
        ones = recs.flatMap(lambda x: [(c, 1) for c in list(x)])
        print(ones.collect())
        baseCount1 = ones.reduceByKey(lambda x, y: x + y)
        res = baseCount1.collect()
        print(res)

    except Exception as ex:
        print("Exception:", ex)

def mapper(seq):
    freq = dict()
    for x in list(seq):
            if x in freq:
                freq[x] += 1
            else:
                freq[x] = 1

    kv = [(x, freq[x]) for x in freq]
    return kv


nonMap()

rdd = recs.flatMap(mapper)
print(rdd.collect())
baseCount = rdd.reduceByKey(lambda x, y: x + y)
print(baseCount.collect())
"""
my_list = [1,2,3,4,5,6,7,8,9,10]
# Lets say I want to square each term in my_list.
slist = list(map(lambda x: x**2, my_list))
print(slist)



