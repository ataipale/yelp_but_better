# run in ipython spark shell, IPYTHON=1 pyspark

from pyspark import SparkContext
from pyspark.mllib.feature import HashingTF
from pyspark.mllib.clustering import KMeans, KMeansModel
from numpy import array
from math import sqrt
import json



documents = sc.textFile("sample.json").map(lambda line: json.loads(line))
hashingTF = HashingTF()
tf = hashingTF.transform(documents)
clusters = KMeans.train(tf, 2, maxIterations=10, runs=10, initializationMode="random")



            
