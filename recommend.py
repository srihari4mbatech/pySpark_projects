from __future__ import print_function
from pyspark import SparkContext
from pyspark.mllib.recommendation import ALS,MatrixFactorizationModel,Rating

if __name__ == '__main__':
    sc=SparkContext(appName='Pspark mlib Example')
    test_f="file:////home/srimlcloud/temp_dat/pySpark_projects/test.data"
    data=sc.textFile(test_f)
    ratings =data.map(lambda l:l.split(',')).map(lambda l:Rating(int(l[0]),int(l[1]),float(l[2])))

    # build the recommendation model using alternating least squares

    rank =10
    numIterations=10
    model =ALS.train(ratings,rank,numIterations)

    #Evaluate the model on training data
    testdata = ratings.map(lambda p: (p[0],p[1]))
    predictions=model.predictAll(testdata).map(lambda r:((r[0],r[1],r[2])))
    ratesAndPreds = ratings.map(lambda r: ((r[0], r[1]), r[2])).join(predictions)
    MSE = ratesAndPreds.map(lambda r: (r[1][0] - r[1][1]) ** 2).mean()
    print("Mean Squared Error = {}".format(MSE))

    model.save(sc,"/home/srimlcloud/temp_dat/pySpark_projects/model")
    sameModel = MatrixFactorizationModel.load(sc, "/home/srimlcloud/temp_dat/pySpark_projects/model")
