import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    desc,
    regexp_replace,
    col,
    current_date,
    datediff,
    to_date,
    lit,
    avg,
)

spark = SparkSession.builder.appName("sg.edu.smu.is459.assignment2").getOrCreate()

# Import the connect components parquet file created in assignment_2_1_create_cc_n_triangles.py
author_cc = spark.read.load("/user/izzkhair/spark-parquet-cc-output")
"""
Below is an example of the data:
-----Connected Components-----
+-------------+---------------------+----------------------+--------------------+------------------+--------------------+------------+---------+
|       author|author_reaction_score|author_number_of_posts|   author_user_title|author_date_joined|         all_content|          id|component|
+-------------+---------------------+----------------------+--------------------+------------------+--------------------+------------+---------+
|  50kilograms|                    0|                   117|              Member|       Nov 8, 2014|Don't care pinoy ...|730144440320|        0|

"""

# Remove the comma and dash in the author_date_joined column data
author_cc_profile = author_cc.select(
    "component",
    "author_number_of_posts",
    "author_reaction_score",
    regexp_replace(col("author_date_joined"), ",", "").alias("author_date_joined"),
).select(
    "component",
    "author_number_of_posts",
    "author_reaction_score",
    regexp_replace(col("author_date_joined"), " ", "-").alias("author_date_joined"),
)

# Find the years between today and the year the user joined hardwarezone
author_cc_reputation = author_cc_profile.select(
    "component",
    "author_reaction_score",
    "author_number_of_posts",
    (
        (
            datediff(current_date(), to_date("author_date_joined", "MMM-d-yyyy"))
            / lit(365)
        ).alias("years_since_joined")
    ),
)

cc_avg_reputation = (
    author_cc_reputation.groupby("component")
    .agg(
        avg("years_since_joined").alias("avg_years_since_joined"),
        avg("author_reaction_score").alias("avg_author_reaction_score"),
        avg("author_number_of_posts").alias("avg_author_number_of_posts"),
    )
    .sort(desc("avg_years_since_joined"))
)

cc_avg_reputation.write.parquet("/user/izzkhair/spark-parquet-cc-avg-reputation")


# Extra 1 - Average reaction score for users post in the connected components
cc_avg_reputation.sort(desc("avg_author_reaction_score")).show()
"""
Below is an example of the data:
+-------------+----------------------+-------------------------+--------------------------+
|    component|avg_years_since_joined|avg_author_reaction_score|avg_author_number_of_posts|
+-------------+----------------------+-------------------------+--------------------------+
|1297080123397|    10.416438356164383|                    437.0|                      null|
"""


# Extra 2 - Average number of posts for users post in the connected components
cc_avg_reputation.sort(desc("avg_author_number_of_posts")).show()
"""
Below is an example of the data:
+-------------+----------------------+-------------------------+--------------------------+
|    component|avg_years_since_joined|avg_author_reaction_score|avg_author_number_of_posts|
+-------------+----------------------+-------------------------+--------------------------+
|1580547964930|    16.334246575342465|                      0.0|                     716.0|
"""


# Extra 3 - Average avg_years_since_joined for each users in the connected components
cc_avg_reputation.show()
"""
Below is an example of the data:
+-------------+----------------------+-------------------------+--------------------------+
|    component|avg_years_since_joined|avg_author_reaction_score|avg_author_number_of_posts|
+-------------+----------------------+-------------------------+--------------------------+
|  60129542147|     21.72876712328767|                      0.0|                      null|
"""
