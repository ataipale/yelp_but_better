# run in ipython spark shell, IPYTHON=1 pyspark

from pyspark import SparkContext
from pyspark.mllib.feature import HashingTF
from pyspark.mllib.clustering import KMeans, KMeansModel
from numpy import array
from math import sqrt
import json
from pyspark.sql import SQLContext, Row


sqlContext = SQLContext(sc)
sample = sqlContext.read.json("./sample.json")
sample = sqlContext.read.json("/home/ubuntu/yelp_project/sample.json")
sample.registerTempTable("sample")
reviews = sample.map(lambda x: Row(name= x[1], reviews=' '.join((a[3] for a in x[0])))) 

# documents = sc.textFile("sample.json", 10).map(lambda line: json.loads(line))
hashingTF = HashingTF()
tf = hashingTF.transform(reviews.map(lambda x: x.reviews))
clusters = KMeans.train(tf, 2, maxIterations=10, runs=10, initializationMode="random")

# sqlContext.sql("SELECT lalal FROM sample")
#give list of lists, each line is all reviews of restaurant
