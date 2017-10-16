# -*- coding: UTF-8 -*-
from pyspark.ml import Pipeline
from pyspark.sql import SparkSession
from pyspark.ml.feature import Word2Vec, StringIndexer
import pyspark.sql.functions as f
from sklearn import svm, grid_search, datasets
from spark_sklearn import GridSearchCV
import cPickle as pickle

session = SparkSession.builder.master("local[2]").appName("test").getOrCreate()

# iris = datasets.load_iris()
# print(iris.target)

documentDF = session.createDataFrame([
    ("Hi I heard about Spark", "spark"),
    ("I wish Java could use case classes", "java"),
    ("Logistic regression models are neat", "mlib"),
    ("Logistic regression models are neat", "spark"),
    ("Logistic regression models are neat", "mlib"),
    ("Logistic regression models are neat", "java"),
    ("Logistic regression models are neat", "spark"),
    ("Logistic regression models are neat", "java"),
    ("Logistic regression models are neat", "mlib")
], ["text", "preds"]).select(f.split("text", "\\s+").alias("new_text"), "preds")

word2vec = Word2Vec(vectorSize=100, minCount=1, inputCol="new_text",
                    outputCol="features")
indexer = StringIndexer(inputCol="preds", outputCol="labels")

pipline = Pipeline(stages=[word2vec, indexer])
ds = pipline.fit(documentDF).transform(documentDF)

data = ds.toPandas()
parameters = {'kernel': ('linear', 'rbf')}
svr = svm.SVC()
clf = GridSearchCV(session.sparkContext, svr, parameters)
X = [x.values for x in data.features.values]
y = [int(x) for x in data.labels.values]
model = clf.fit(X, y)

# modelB = session.sparkContext.broadcast(pickle.dumps(model))
# wow = documentDF.rdd.map(lambda row: pickle.loads(modelB.value).transform(row["features"].values)).collect()
# print(wow)
