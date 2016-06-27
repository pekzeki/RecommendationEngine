__author__ = 'engin'

from pyspark.mllib.recommendation import ALS, Rating
from pyspark import SparkContext, SparkConf
import sys, os

if len(sys.argv) < 4:
    print >> sys.stderr, \
        "Usage: <RANK> <ITERATIONS> <LAMBDAS> <FILE_NAME" \
        ">"
    exit(-1)

conf = SparkConf().setAppName("ALS")
sc = SparkContext(conf=conf)

rank = int(sys.argv[1])
numIters = int(sys.argv[2])
lambdas = float(sys.argv[3])
file_name = str(sys.argv[4])

clusters = sc.textFile("/user/admin/project/clustering/" + file_name, use_unicode=False)
cluster_temp = clusters.map(lambda x: x.split(",")).map(lambda k: (k[0].strip('(\''), k[1].strip(')\'').strip(" "), 0.0))
k = cluster_temp.map(lambda x: x[1]).distinct().collect()

stars = sc.textFile("/user/admin/project/recommendation/Recommendation_Full_Data/stars.csv", use_unicode=False)
stars_temp = stars.map(lambda k: k.split(",")).map(lambda p: (p[0], p[1], float(p[2].strip('"'))))

user_item_rating_cluster = stars_temp.map(lambda x: (x[0], (x[1], x[2]))).join(cluster_temp.map(lambda x: (x[0], (x[1], x[2]))))
cluster_user_item_rating = user_item_rating_cluster.map(lambda x: (x[1][1][0], (x[0], x[1][0][0], x[1][0][1])))

a = 0
b = 0
for i in k:
    ratings = cluster_user_item_rating.filter(lambda x: x[0] == i).map(lambda l: Rating(int(l[1][0]), int(l[1][1]), float(l[1][2])))
    test, training = ratings.randomSplit([3, 7], 5L)
    test_data = test.map(lambda p: (p[0], p[1]))

    model = ALS.train(training, rank, numIters, lambda_=lambdas)
    predictions = model.predictAll(test_data).map(lambda r: ((r[0], r[1]), r[2]))
    ratesAndPreds = ratings.map(lambda r: ((r[0], r[1]), r[2])).join(predictions)
    MSE = ratesAndPreds.map(lambda r: (r[1][0] - r[1][1])**2).mean()

    #local_path = os.path.join("/home/hduser/als/clustering/", "R" + str(rank) + "_I" + str(numIters) + "_L" + str(lambdas) + ".txt")
    #with open(local_path, "w+") as testFile:
    #    testFile.write("r= " + str(rank) + " iterations = " + str(numIters) + " lambdas = " + str(lambdas) + " mse = " + str(MSE))
    #testFile.close()

    #file_path = "/user/admin/project/als/clustering/" + str(i) + "/R" + str(rank) + "_I" + str(numIters) + "_L" + str(lambdas)
    #ratesAndPreds.saveAsTextFile(file_path)

    a += MSE*test.count()
    b += test.count()
print "***************************************"
print "***************************************"
print "***************************************"
print "***************************************"
print "***************************************"
print "AVG MSE ::::: " + str(a / float(b))








