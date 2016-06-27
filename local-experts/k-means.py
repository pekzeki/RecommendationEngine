__author__ = 'hduser'

from pyspark import SparkContext, SparkConf
from pyspark.mllib.clustering import KMeans
from numpy import array
from math import sqrt
import sys, os


if len(sys.argv) < 3:
    print >> sys.stderr, \
        "Usage: <K> <MAX_ITERATIONS> <RUNS>"
    exit(-1)

conf = SparkConf().setAppName("K-MEANS")
sc = SparkContext(conf=conf)

_k = int(sys.argv[1])
_maxIterations = int(sys.argv[2])
_runs = int(sys.argv[3])

tfidf = sc.textFile("/user/admin/project/recommendation/final_tfidf_scores_pivot.csv", use_unicode=False)
tfidf = tfidf.map(lambda k: k.split("\t")).map(lambda p: (p[0], p[1:]))
header = tfidf.first()
x = tfidf.filter(lambda l: l != header)
y = x.map(lambda p: (p[0], map(float, p[1])))
z = y.map(lambda line: array(line[1]))

model = KMeans.train(z, _k, maxIterations=_maxIterations, runs=_runs, initializationMode="random")

# Evaluate clustering by computing Within Set Sum of Squared Errors
def error(point):
    center = model.centers[model.predict(point)]
    return sqrt(sum([x**2 for x in (point - center)]))
WSSSE = z.map(lambda point: error(point)).reduce(lambda x, y: x + y)
predictions = y.map(lambda user: (user[0], model.predict(user[1])))

local_path = os.path.join("/home/hduser", "K" + str(_k) + "_I" + str(_maxIterations) + "_R" + str(_runs) + ".txt")
with open(local_path, "w+") as testFile:
    testFile.write("k= " + str(_k) + " max_iterations = " + str(_maxIterations) + " runs = " + str(_runs) + " wssse = " + str(WSSSE))
testFile.close()

file_path = "/user/admin/project/clustering/" + "K" + str(_k) + "_I" + str(_maxIterations) + "_R" + str(_runs)
predictions.saveAsTextFile(file_path)
