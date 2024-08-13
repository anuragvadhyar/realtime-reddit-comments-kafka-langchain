from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.functions import *
from Stop import get_stopwords
def preprocessing(data):
    data = data.select(col("_id"),col("text").alias("original_text"),explode(split(data.text, "t_end")).alias("text"), col("score"),col("source"))
    data = data.na.replace('', None)
    data = data.na.drop()
    data = data.withColumn('text', F.regexp_replace('text', "[‘’]", ''))
    data = data.withColumn('text', F.regexp_replace('text', r'http\S+', ''))
    data = data.withColumn('text', F.regexp_replace('text', '@\w+', ''))
    data = data.withColumn('text', F.regexp_replace('text', '#', ''))
    data = data.withColumn('text', F.regexp_replace('text', 'RT', ''))
    data = data.withColumn('text', F.regexp_replace('text', '\\n', ''))
    data = data.withColumn('text', F.regexp_replace('text', '[.!?\\-]', ' '))
    data = data.withColumn('text', F.regexp_replace('text', "[^ 'a-zA-Z0-9]", ''))
    

    stopwords=get_stopwords()    
    for stopword in stopwords:
        data = data.withColumn('text', F.regexp_replace('text', ' '+stopword+' ' , ' '))
    data = data.withColumn('text', F.regexp_replace('text', ' +', ' '))
    data.select(trim(col("text")))
    return data
