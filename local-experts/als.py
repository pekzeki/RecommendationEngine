__author__ = 'hduser'

from pyspark.mllib.recommendation import ALS, Rating
from pyspark import SparkContext, SparkConf
import sys, os

if len(sys.argv) < 3:
    print >> sys.stderr, \
        "Usage: <RANK> <ITERATIONS> <LAMBDAS>"
    exit(-1)

conf = SparkConf().setAppName("ALS")
sc = SparkContext(conf=conf)

rank = int(sys.argv[1])
numIters = int(sys.argv[2])
lambdas = float(sys.argv[3])

stars = sc.textFile("/user/admin/project/recommendation/Recommendation_Phoenix_Stars/results.csv", use_unicode=False)
stars_temp = stars.map(lambda k: k.split(",")).map(lambda p: (p[0], p[1], float(p[2].strip('"'))))
ratings = stars_temp.map(lambda l: Rating(int(l[0]), int(l[1]), float(l[2])))
test, training = ratings.randomSplit([3, 7], 5L)
test_data = test.map(lambda p: (p[0], p[1]))

model = ALS.train(training, rank, numIters, lambda_=lambdas)
predictions = model.predictAll(test_data).map(lambda r: ((r[0], r[1]), r[2]))
ratesAndPreds = ratings.map(lambda r: ((r[0], r[1]), r[2])).join(predictions)
MSE = ratesAndPreds.map(lambda r: (r[1][0] - r[1][1])**2).mean()

local_path = os.path.join("/home/hduser", "R" + str(rank) + "_I" + str(numIters) + "_L" + str(lambdas) + ".txt")
with open(local_path, "w+") as testFile:
    testFile.write("r= " + str(rank) + " iterations = " + str(numIters) + " lambdas = " + str(lambdas) + " mse = " + str(MSE))
testFile.close()

file_path = "/user/admin/project/als/phoenix/" + "R" + str(rank) + "_I" + str(numIters) + "_L" + str(lambdas)
ratesAndPreds.saveAsTextFile(file_path)










