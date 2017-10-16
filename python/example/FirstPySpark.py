# -*- coding: UTF-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.functions import udf
from pyspark.sql.types import *

session = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
documentDF = session.createDataFrame([
    ("Hi I heard about Spark", 1),
    ("I wish Java could use case classes", 0),
    ("Logistic regression models are neat", 2)
], ["text", "preds"])

# 如何读取文件？
print(session.sparkContext.textFile("/tmp/NetpasHelper.log").count())

# 对数据进行SQL操作
documentDF.registerTempTable("test")
session.sql("select * from test").show()

# 使用函数对数据进行处理
documentDF.select(f.split("text", "\\s+").alias("text_array")).show()


# 自定义函数
def split_sentence(s):
    return s.split(" ")


ss = udf(split_sentence, ArrayType(StringType()))
documentDF.select(ss("text").alias("text_array")).show()

# 转化为rdd进行处理
print(documentDF.rdd.map(lambda s: s["text"].split(" ")).collect())

session.stop()
