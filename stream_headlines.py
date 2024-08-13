import sys
sys.path.append('C:\\Users\\Anurag\\Desktop\\Kafka\\env\\Lib\\site-packages')
import findspark
findspark.init('C:\\spark')
import sys
sys.stdout.reconfigure(encoding='utf-8')

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark.sql.functions import expr

from pathlib import Path
import time

from preprocessing import preprocessing
from tfidf import Tfidf_Pipeline

kafka_topic_name = "trending"
kafka_bootstrap_servers = 'localhost:9092'

def find_similarity(data):
    print("in similarity")

    inshorts_df = data.filter(data.source == "inshorts")
    newsapi_df = data.filter(data.source == "newsapi")
    websearch_df = data.filter(data.source == "websearch")

    dot_udf = F.udf(lambda x,y: float(x.dot(y)), DoubleType())
    joined_df = inshorts_df.alias('inshorts').join(newsapi_df.alias('newsapi')).select(
        F.col("inshorts._id").alias("inshorts_id"),
        F.col("newsapi._id").alias("newsapi_id"),
        F.col("inshorts.original_text").alias("inshorts_text"),
        F.col("newsapi.original_text").alias("newsapi_text"),
        F.col("inshorts.score").alias("inshorts_score"),
        F.col("newsapi.score").alias("newsapi_score"),
        F.col("inshorts.norm").alias("inshorts_norm"),
        F.col("inshorts.source").alias("source"),
        dot_udf("inshorts.norm", "newsapi.norm").alias("similarity_score"))

    joined_df = joined_df.filter(joined_df.similarity_score > 0.3)
    #joined_df.explain()  # Print the execution plan
    #print(f"Number of rows in joined_df: {joined_df.count()}")

    inshorts_similar_df = joined_df.select(col("inshorts_id"))
    inshorts_filtered = inshorts_df.join(inshorts_similar_df, inshorts_df._id == inshorts_similar_df.inshorts_id,"left_anti")

    newsapi_similar_df = joined_df.select(col("newsapi_id"))
    newsapi_filtered = newsapi_df.join(newsapi_similar_df, newsapi_df._id == newsapi_similar_df.newsapi_id,"left_anti")

    joined_df = joined_df.withColumn('score', col('inshorts_score')+col('newsapi_score'))
    joined_df = joined_df.select(col("inshorts_id").alias("_id"),col("inshorts_text").alias("original_text"),col("score"),col("source"),col("inshorts_norm").alias("norm"))
    joined_df = joined_df.union(inshorts_filtered)
    joined_df = joined_df.union(newsapi_filtered)
    
    joined_final_df = joined_df.alias('joined').join(websearch_df.alias('websearch')).select(
        F.col("joined._id").alias("joined_id"),
        F.col("websearch._id").alias("websearch_id"),
        F.col("joined.original_text").alias("joined_text"),
        F.col("websearch.original_text").alias("websearch_text"),
        F.col("joined.score").alias("joined_score"),
        F.col("websearch.score").alias("websearch_score"),
        F.col("joined.norm").alias("norm"),
        F.col("joined.source").alias("source"),
        dot_udf("joined.norm", "websearch.norm").alias("similarity_score"))

    joined_final_df = joined_final_df.filter(joined_final_df.similarity_score > 0.3)
    #joined_final_df.explain()  # Print the execution plan
    #print(f"Number of rows in joined_final_df: {joined_final_df.count()}")

    joined_similar_df = joined_final_df.select(col("joined_id"))
    joined_filtered = joined_df.join(joined_similar_df, joined_df._id == joined_similar_df.joined_id,"left_anti")

    websearch_similar_df = joined_final_df.select(col("websearch_id"))
    websearch_filtered = websearch_df.join(websearch_similar_df, websearch_df._id == websearch_similar_df.websearch_id,"left_anti")

    joined_final_df = joined_final_df.withColumn('score', col('joined_score')+col('websearch_score'))
    joined_final_df = joined_final_df.select(col("joined_id").alias("_id"),col("joined_text").alias("original_text"),col("score"),col("source"),col("norm"))
    joined_final_df = joined_final_df.union(joined_filtered)
    joined_final_df = joined_final_df.union(websearch_filtered)
    joined_final_df = joined_final_df.limit(350)
    joined_final_df = joined_final_df.drop('norm')
    joined_final_df = joined_final_df.withColumnRenamed('original_text','text')

    print("exiting similarity")
    # Print the number of rows in joined_final_df
    #print(f"Number of rows in joined_final_df: {joined_final_df.count()}")

    return joined_final_df



def find_similiar_headlines(df):

    df = df.withColumn("text", F.split("text", ' '))
    tfidf = light_pipeline.transform(df)
    
    columns_to_drop = ['text','tf','feature']
    tfidf = tfidf.drop(*columns_to_drop)
    #tfidf.show(5)
    # Print the number of rows in joined_df
    print(f"Number of rows in tfidf: {tfidf.count()}")

    print("finding similarity")
    similairty_scores_df = find_similarity(tfidf)
    print("exited")
    #similairty_scores_df.cache()
    #similairty_scores_df.show(5)
    print("doneeeeeee")
    final_df = preprocessing(similairty_scores_df)
    #pandas_df = final_df.select("norm").toPandas()

    # Print the content of the 'norm' column
    
    final_df.write\
        .format("mongo")\
        .mode("append")\
        .option("uri", "mongodb://localhost:27017/NewsDB.Hot")\
        .save()
    
    
    print("SUCCESSFULLY STORED IN DATABASE")
    
    return final_df


if __name__ == "__main__":
    
    spark = SparkSession.builder\
    .appName("PySpark Structured Streaming with Kafka for headlines")\
    .master("local[*]")\
    .config("spark.mongodb.output.uri", "mongodb://localhost:27017/NewsDB.Hot")\
    .getOrCreate()

    print(time.strftime("%Y-%m-%d %H:%M:%S"))

    
    print("CONSUMING FROM TOPIC"+ kafka_topic_name)
    
    
    spark.sparkContext.setLogLevel("ERROR")


    light_pipeline = Tfidf_Pipeline(spark)


    headlines_df = spark.readStream\
        .format("kafka")\
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers)\
        .option("subscribe", kafka_topic_name)\
        .option("startingOffsets", "latest")\
        .option("maxOffsetsPerTrigger",500)\
        .load()

    headlines_df.printSchema()
    
    headlines_schema = StructType()\
        .add("_id", StringType())\
        .add("text", StringType())\
        .add("source", StringType())
    

    headlines_df1 = headlines_df.selectExpr("CAST(value AS STRING)")     
    headlines_df2 = headlines_df1\
        .select(from_json(col("value"), headlines_schema)
        .alias("headlines_columns"))
    headlines_df3 = headlines_df2.select("headlines_columns.*")
    headlines_df4 = headlines_df3.withColumn("score",lit(1000))

    final_df = preprocessing(headlines_df4)


    
    query_headlines = final_df \
        .writeStream.trigger(processingTime='10 seconds')\
        .outputMode("update")\
        .option("truncate", "true")\
        .format("console")\
        .foreachBatch(lambda batch_headline_df, batchId: find_similiar_headlines(batch_headline_df))\
        .start()
    
    spark.streams.awaitAnyTermination()
