import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id, count, desc
from graphframes import *

spark = SparkSession.builder.appName("sg.edu.smu.is459.assignment2").getOrCreate()

# Load data newly crawled from scrapy saved in parquet file
posts_df = spark.read.load(
    "/user/izzkhair/parquet-input-mine/hardwarezone_mine.parquet"
)
"""
# Below is an example of the data:
+--------------------+--------------+----------------------+------------------+--------------------+----------------------+---------------------+--------------------+
| date_thread_started|        author|               content|author_date_joined|   author_user_title|author_number_of_posts|author_reaction_score|               topic|
+--------------------+--------------+----------------------+------------------+--------------------+----------------------+---------------------+--------------------+
|        Aug 31, 2021|        Dragon|  to continue readi...|       Jan 1, 2000|                   *|                36,420|                   93|Paradise Killer S...|
"""

# Clean the dataframe by removing rows with any null value
posts_df = posts_df.na.drop()

# Find distinct users.
# I also concat all of the post posted by the user to a single string to make it easier for tokenization later.
author_df = (
    posts_df.groupby(
        "author",
        "author_reaction_score",
        "author_number_of_posts",
        "author_user_title",
        "author_date_joined",
    )
    .agg(f.concat_ws(", ", f.collect_list(posts_df.content)))
    .withColumnRenamed("concat_ws(, , collect_list(content))", "all_content")
)

# Assign ID to the users
author_id = author_df.withColumn("id", monotonically_increasing_id())

# Construct connection between post and author
left_df = (
    posts_df.select("topic", "author")
    .withColumnRenamed("topic", "ltopic")
    .withColumnRenamed("author", "src_author")
)

right_df = left_df.withColumnRenamed("ltopic", "rtopic").withColumnRenamed(
    "src_author", "dst_author"
)

# Self join on topic to build connection between authors
author_to_author = (
    left_df.join(right_df, left_df.ltopic == right_df.rtopic)
    .select(left_df.src_author, right_df.dst_author)
    .distinct()
)

# Convert it into ids
id_to_author = (
    author_to_author.join(author_id, author_to_author.src_author == author_id.author)
    .select(author_to_author.dst_author, author_id.id)
    .withColumnRenamed("id", "src")
)

id_to_id = (
    id_to_author.join(author_id, id_to_author.dst_author == author_id.author)
    .select(id_to_author.src, author_id.id)
    .withColumnRenamed("id", "dst")
)

id_to_id = id_to_id.filter(id_to_id.src >= id_to_id.dst).distinct()
id_to_id.cache()

# Build graph with RDDs
graph = GraphFrame(author_id, id_to_id)

# For complex graph queries, e.g., connected components, you need to set
# the checkopoint directory on HDFS, so Spark can handle failures.
# Remember to change to a valid directory in your HDFS
spark.sparkContext.setCheckpointDir("/user/izzkhair/spark-checkpoint")

# Find the community within the users with the help of built-in connected components algorithm
result = graph.connectedComponents()
result.write.parquet("/user/izzkhair/spark-parquet-cc-output")

result_triangle = graph.triangleCount()
result_triangle.write.parquet("/user/izzkhair/spark-parquet-triangle-output")

# For question 1
# How large is the community in the hardwarezone pc gaming forum?
author_community_size = (
    result.groupby("component").agg((count("*")).alias("count")).sort(desc("count"))
)
author_community_size.show()
"""
Below are the results:
+-------------+-----+
|    component|count|
+-------------+-----+
|            0| 4577|
|1108101562378|    3|
| 489626271746|    3|
| 266287972362|    3|
| 283467841550|    3|
| 575525617667|    2|
|  94489280515|    2|
| 549755813896|    2|
| 395136991238|    2|
|1468878815244|    2|
| 257698037764|    2|
| 996432412673|    2|
|1666447310852|    1|
|1477468749836|    1|
|            7|    1|
|1013612281863|    1|
| 120259084293|    1|
| 850403524615|    1|
|  94489280522|    1|
| 798863917060|    1|
+-------------+-----+
"""
