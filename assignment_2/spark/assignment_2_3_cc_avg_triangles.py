from pyspark.sql import SparkSession
from pyspark.sql.functions import desc, avg

spark = SparkSession.builder.appName("sg.edu.smu.is459.assignment2").getOrCreate()

# Import the cc & triangle parquet files created in assignment_2_1_create_cc_n_triangles.py
author_triangle = spark.read.load("/user/izzkhair/spark-parquet-triangle-output")
author_cc = spark.read.load("/user/izzkhair/spark-parquet-cc-output")
"""
Below is an example of the data:
-----Connected Components-----
+-------------+---------------------+----------------------+--------------------+------------------+--------------------+------------+---------+
|       author|author_reaction_score|author_number_of_posts|   author_user_title|author_date_joined|         all_content|          id|component|
+-------------+---------------------+----------------------+--------------------+------------------+--------------------+------------+---------+
|  50kilograms|                    0|                   117|              Member|       Nov 8, 2014|Don't care pinoy ...|730144440320|        0|

-----Triangle-------
+------+--------------+---------------------+----------------------+--------------------+------------------+--------------------+-------------+
| count|        author|author_reaction_score|author_number_of_posts|   author_user_title|author_date_joined|         all_content|           id|
+------+--------------+---------------------+----------------------+--------------------+------------------+--------------------+-------------+
| 52920|    IndigoEyes|                  209|                 5,640|    Supremacy Member|       Jan 3, 2012|I'm thinking of g...| 120259084290|
"""

# Join the connected components df and triangle df
author_triangle_cc = author_triangle.join(
    author_cc, author_triangle.id == author_cc.id
).select(author_triangle.id, author_cc.component, "count")

# Group by the component and find the average triangle count per component
cc_triangle_avg = (
    author_triangle_cc.groupby("component")
    .agg(avg("count").alias("avg_triangle"))
    .sort(desc("avg_triangle"))
)

cc_triangle_avg.write.parquet("/user/izzkhair/spark-parquet-cc-avg-triangles")

# For question 3
# Whats the average connected triangle size for each component
cc_triangle_avg.show()
"""
+-------------+------------------+
|    component|      avg triangle|
+-------------+------------------+
|            0|58818.026873497925|
| 266287972362|               1.0|
| 489626271746|               1.0|
| 283467841550|               1.0|
|1108101562378|               1.0|
| 601295421443|               0.0|
| 523986010118|               0.0|
|  94489280515|               0.0|
|1580547964928|               0.0|
|1297080123395|               0.0|
|1297080123397|               0.0|
| 120259084293|               0.0|
|1443109011456|               0.0|
|1443109011459|               0.0|
| 850403524615|               0.0|
|1236950581252|               0.0|
| 833223655434|               0.0|
|  94489280522|               0.0|
| 798863917060|               0.0|
|1571958030352|               0.0|
+-------------+------------------+
"""
