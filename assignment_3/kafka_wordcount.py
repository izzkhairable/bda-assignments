from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.functions import window
from pyspark.sql.functions import desc
from pyspark.sql.functions import regexp_replace
from pyspark.sql.functions import from_json

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
            """("content":)|(  )|(author:)|( the )|( and )|( of )|( for )|("})|( is )|( a )|( to )|( in )|( with )|( not )|( using )|( I )|( on )|( you )|( it )|(")|( i )|( if )|( or )|( my )|( can )|( will )|( be )|( from )|( so )|( that )|( have )|( too )|( me )|( like )|( are )|(this)|( they )|( at )|( us )|( u )""",
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

    windowedCounts = (
        line_with_content_words.groupBy(
            window(line_with_content_words.timestamp, "2 minutes", "1 minutes").alias(
                "window"
            ),
            "content_word",
        )
        .count()
        .orderBy(desc("window"), desc("count"))
    )

    windowedCounts2 = (
        lines.groupBy(
            window(lines.timestamp, "2 minutes", "1 minutes").alias("window"),
            "author_stripped",
        )
        .count()
        .orderBy(desc("window"), desc("count"))
    )

    query = (
        windowedCounts.writeStream.option("numRows", 10)
        .outputMode("complete")
        .format("console")
        .option("truncate", "false")
        .start()
    )

    query2 = (
        windowedCounts2.writeStream.option("numRows", 10)
        .outputMode("complete")
        .format("console")
        .option("truncate", "false")
        .start()
    )

    query.awaitTermination()
    query2.awaitTermination()
