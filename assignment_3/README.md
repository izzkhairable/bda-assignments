# BDA SMU-IS459 Assignment 3

### Mohamed Izzat Khair Bin Mohamed Noor (01368194)



## Scrapy Project
- For crawling the Hardwarezone PC Gaming Forum
- I included this folder as the spark project in assignment 2 has extra fields that are not used.
- This project also contain the pipeline code which produce message to kafka

### Fields outputted by Scrapy

| Field Name             |
| ---------------------- |
| title                  |
| author                 |
| content                |

### How to run Scrapy Project?

1. Ensure that kafka and zookeeper is running 

2. From /assignment_3 directory go to the /hardwarezonezone directory
```
cd Scrapy/hardwarezone/
```

3. Run the following command
```
scrapy crawl hardwarezone
```

4. Leave it running while you start the spark job below


## Spark Project

- Process the data stream from kafka
- For every window shows the top 10 words and top 10 users with most posts

### How to run Spark Project:

1. Go into the /spark directory
```
cd spark 
```

2. Run spark job by submitting the python file
```
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 kafka_wordcount.py
```
> It might take awhile in the beginning...

3. You should see to see the output every one minute
