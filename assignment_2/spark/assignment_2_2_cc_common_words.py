import pyspark.sql.functions as f
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import explode, count, row_number, desc, regexp_replace, col
from graphframes import *
from pyspark.ml.feature import StopWordsRemover, Tokenizer
import nltk

spark = SparkSession.builder.appName("sg.edu.smu.is459.assignment2").getOrCreate()

# I decided to use nltk stopwords instead as it is much more diverse list of stopwords
nltk.download("stopwords")

# Load the parquet containing connected components
# It is the output from assignment_2_1_create_cc_triangles.py
"""
Below is an example of the data:
+-------------+---------------------+----------------------+--------------------+------------------+--------------------+------------+---------+
|       author|author_reaction_score|author_number_of_posts|   author_user_title|author_date_joined|         all_content|          id|component|
+-------------+---------------------+----------------------+--------------------+------------------+--------------------+------------+---------+
|  50kilograms|                    0|                   117|              Member|       Nov 8, 2014|Don't care pinoy ...|730144440320|        0|
"""
author_community_df = spark.read.load("/user/izzkhair/spark-parquet-cc-output")

# One more lay of data cleaning
# remove double white spaces as well as punctuations
author_community_df_cleaned = (
    author_community_df.select(
        "author",
        "id",
        "component",
        (regexp_replace(col("all_content"), "  ", "").alias("all_content")),
    )
    .select(
        "author",
        "id",
        "component",
        (regexp_replace(col("all_content"), "[^\w\s]", "").alias("all_content")),
    )
    .na.drop()
)

# Tokenize the content of the previously "all_content" string column in an array/list of strings
tokenizer = Tokenizer(inputCol="all_content", outputCol="content_tokens")
author_cc_token = tokenizer.transform(author_community_df_cleaned).select(
    "author", "id", "component", "content_tokens"
)

# Get list stop words. I also added a few stopworks manually as when getting the result
# I find that some of the highest frequency words for the component isn't useful
# Use the StopWordsRemover function to remove the words from the array/list
stopwordsList = nltk.corpus.stopwords.words("english") + [
    "u",
    "00",
    "ok",
    "game",
    "also",
    "play",
    "players",
    "playing",
]
remover = StopWordsRemover(
    inputCol="content_tokens", outputCol="content_no_stop", stopWords=stopwordsList
)
author_cc_no_stop = remover.transform(author_cc_token).select(
    "author", "id", "component", "content_no_stop"
)


# Grouping by the component, all the array/list of string containing words combined
# into one list per component
cc_words_grouped = (
    author_cc_no_stop.select("component", "content_no_stop")
    .rdd.map(lambda r: (r.component, r.content_no_stop))
    .reduceByKey(lambda x, y: x + y)
    .toDF(["component", "content_no_stop"])
)

# Explode the words in the array/list to all form its own row
cc_words_explode = cc_words_grouped.select(
    cc_words_grouped.component, explode(cc_words_grouped.content_no_stop)
)

# Group by components and words,Aggregating count of each word in the component
# Hence, finding it the word frequency
cc_words_count = (
    cc_words_explode.groupBy("component", "col")
    .agg(count("*"))
    .withColumnRenamed("col", "word")
    .withColumnRenamed("count(1)", "count")
).filter("word!=''")

# Order by the count and each component to find the first row
# Which represents the word with the highest frequency
w = Window.partitionBy("component").orderBy(desc("count"))
cc_final = (
    cc_words_count.withColumn("row", row_number().over(w))
    .filter(col("row") == 1)
    .drop("row")
    .sort(desc("count"))
)

cc_final.write.parquet("/user/izzkhair/spark-parquet-cc-common-words")

# For question 2
# What is the key word in the community?
cc_final.show()
"""
Below are the results:
+-------------+-------+-----+
|    component|   word|count|
+-------------+-------+-----+
|            0|      u|11583|
|1374389534733|     00| 1038|
|1614907703303|   like|   30|
|1108101562378|   game|   22|
| 326417514499|  fixed|   21|
| 266287972362|      u|   21|
|1022202216455|   ship|   21|
| 996432412673| season|   12|
| 781684047876| spirit|   10|
| 635655159810|   game|    7|
|1245540515844|playing|    6|
| 120259084293|raiding|    6|
| 798863917060|   game|    5|
| 446676598798|    pub|    5|
| 283467841550|   game|    5|
|1477468749825|   game|    5|
|1142461300744|   like|    4|
| 257698037770|legends|    4|
| 412316860421|    9pm|    4|
|1623497637897|   game|    4|
+-------------+-------+-----+
"""
