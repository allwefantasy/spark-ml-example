# -*- coding: UTF-8 -*-
from pyspark.ml import Pipeline
from pyspark.sql import SparkSession
from pyspark.ml.feature import Word2Vec, StringIndexer
import pyspark.sql.functions as f

session = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
documentDF = session.createDataFrame([
    ("Hi I heard about Spark", "spark"),
    ("I wish Java could use case classes", "java"),
    ("Logistic regression models are neat", "spark")
], ["text", "preds"]).select(f.split("text", "\\s+").alias("new_text"), "preds")

word2vec = Word2Vec(vectorSize=100, minCount=1, inputCol="new_text",
                    outputCol="word_embedding")
indexer = StringIndexer(inputCol="preds", outputCol="preds_index")

pipline = Pipeline(stages=[word2vec, indexer])
ds = pipline.fit(documentDF).transform(documentDF)

# labels = indexer.fit(documentDF).transform(documentDF)
# ds = word2vec.fit(labels).transform(labels)

ds.show()

word2vecModel = word2vec.fit(documentDF)
word_embedding = dict(
    word2vecModel.getVectors().rdd.map(
        lambda p: (p.word, p.vector.values.tolist())).collect())
print(word_embedding)
