from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.functions import window
from pyspark.sql.functions import desc
from pyspark.sql.functions import regexp_replace
from pyspark.sql.functions import from_json
from pyspark.sql.functions import concat_ws
from pyspark.sql.functions import col
from pyspark.sql.functions import concat
from pyspark.sql.functions import lit
from pyspark.sql.functions import row_number
from pyspark.sql.functions import struct

# from pyspark.sql.functions import withColumnRenamed

if __name__ == "__main__":

    spark = SparkSession.builder.appName("KafkaWordCount").getOrCreate()
    spark.catalog.clearCache()
    # Read from Kafka's topic scrapy-output
    lines = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "scrapy-output")
        .option("includeTimestamp", "true")
        .load()
        .selectExpr("CAST(value AS STRING)", "timestamp")
    )

    schema = StructType(
        [
            StructField("topic", StringType(), True),
            StructField("author", StringType(), True),
            StructField("content", StringType(), True),
        ]
    )

    lines = lines.withColumn("value", from_json("value", schema)).select(
        "timestamp", "value.*"
    )

    lines = lines.withColumn(
        "content_stripped",
        regexp_replace(
            "content",
            """("content":)|(  )|("  ")|('  ')|(author:)|( the )|( and )|( of )|( for )|("})|( is )|( a )|( to )|( in )|( with )|( not )|( using )|( I )|( on )|( you )|( it )|(")|( i )|( if )|( or )|( my )|( can )|( will )|( be )|( from )|( so )|( that )|( have )|( too )|( me )|( like )|( are )|(this)|( they )|( at )|( us )|( u )|( but )|(-)""",
            "",
        ),
    )

    lines = lines.withColumn(
        "author_stripped",
        regexp_replace(
            "author",
            '("author":)|(  )|(")',
            "",
        ),
    )

    line_with_content_words = lines.select(
        explode(split(lines.content_stripped, " ")).alias("content_word"), "*"
    )

    windowedCounts2 = (
        lines.groupBy(
            window(lines.timestamp, "2 minutes", "1 minutes").alias("window"),
            "author_stripped",
        )
        .count()
        .orderBy(desc("window"), desc("count"))
        .select(
            concat(
                col("window").cast("string"),
                lit(";"),
                col("author_stripped").cast("string"),
                lit(";"),
                col("count").cast("string"),
            ).alias("value")
        )
        .filter("NOT value IS NULL")
    )

    query = (
        windowedCounts2.limit(10)
        .writeStream.format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("topic", "streaming-output")
        .outputMode("complete")
        .option("checkpointLocation", "/user/izzkhair/new-spark-checkpoint")
        .start()
    )

    query.awaitTermination()
