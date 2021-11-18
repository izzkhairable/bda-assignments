# BDA SMU-IS459 Assignment 4

### Mohamed Izzat Khair Bin Mohamed Noor (01368194)

## Results
During the first load you should see the top 10 users based on the most recent window. If you keep refereshing the page, the data will still change as aggregation and processing is still on going for that window.
![24to26](https://user-images.githubusercontent.com/60332263/142249932-4f8a7c7e-bc01-48fe-bed9-587fe53ba439.png)

After two minutes if you refresh again, the time window period would change to a new window (like seen in the image below). You can see the post count for each user is still low as the aggregation and processing for the new window is still on going too.
![25to27](https://user-images.githubusercontent.com/60332263/142250367-01e6fb79-c7e7-4f0e-b406-21c7c51c5ad9.png)

## Terminal Windows
Upon completing the steps below you should have 5 terminal windows open and running. 
| Window Purpose     |
| --------------    |
| Kafka             |
| Zookeeper         |
| Scrapy            |
| Spark             |
| Django            |

Please run the various projects in the sequence listed below.
## Step 1) Scrapy Project

- For crawling the Hardwarezone PC Gaming Forum
- I included this folder as the spark project in assignment 2 has extra fields that are not used.
- This project also contain the pipeline code which produce message to kafka

### Fields outputted by Scrapy

| Field Name |
| ---------- |
| title      |
| author     |
| content    |

### How to run Scrapy Project?

1. Ensure that kafka and zookeeper is running

```
bin/zookeeper-server-start.sh config/zookeeper.properties
bin/kafka-server-start.sh config/server.properties
```

2. From /assignment_4 directory go to the /hardwarezonezone directory

```
cd Scrapy/hardwarezone/
```

3. Run the following command

```
scrapy crawl hardwarezone
```

> If this doesn't work., exit from virtual environment, then run "pip install kafka-python" and "pip install Scrapy". This will install them globally. Run the command above again.

4. Leave it running while you start the spark job below

## Step 2) Spark Project

- Process the data stream from kafka topic "scrapy-output"
- For every window push the top 10 users with most posts to Kafka topic "streaming-output"

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

3. Leave it running while you start the Django app below

## Step 3) Django Project
- Get the data from kafka topic "streaming-output"
- Display the Top 10 Users with the most posts using chart.js
1. Go into the /django/hwz_monitor directory
```
cd django/hwz_monitor
```
2. Run the Django Application
```
python manage.py runserver
```
3. Go to the URL below in your browser
```
http://127.0.0.1:8000/dashboard/barchart
```
> You should be able to see the bar chart of the top 10 users for each window like the one seen in the result section
