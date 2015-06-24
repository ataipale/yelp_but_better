# run in ipython spark shell, IPYTHON=1 pyspark

# When I was trying to edit memory use I edited these files:
# ~/spark-1.4.0-bin-hadoop2.6/conf
# vi spark-env.sh

from pyspark import SparkContext
from pyspark.mllib.feature import HashingTF
from pyspark.mllib.clustering import KMeans, KMeansModel
from numpy import array
from math import sqrt
import json
from pyspark.sql import SQLContext, Row
from pyspark.mllib.feature import IDF


stopwords = [i.strip() for i in open("stopwords_en.txt")]
sqlContext = SQLContext(sc)
sample = sqlContext.read.json("/home/ubuntu/yelp_project/sample.json")
sample.registerTempTable("sample")
# take only the reviews from each row of the sample
reviews = sample.map(lambda x: Row(name= x.name, reviews=' '.join(a[3] for a in x.reviews)))
reviews_clean = reviews.map(lambda x: Row(name = x.name, reviews = ' '.join(word for word in x.reviews if word.lower() not in stopwords)))
# sample = sqlContext.read.json("./sample.json")

# documents = sc.textFile("sample.json", 10).map(lambda line: json.loads(line))
#initialize with max number of features you want in your TFIDF
hashingTF = HashingTF(5000) 
tf = hashingTF.transform(reviews_clean.map(lambda x: x.reviews))
tf.cache()
idf = IDF(minDocFreq=2).fit(tf)
tfidf = idf.transform(tf)
clusters = KMeans.train(tfidf, 2, maxIterations=10, runs=10, initializationMode="random")

# sqlContext.sql("SELECT lalal FROM sample")
#give list of lists, each line is all reviews of restaurant
