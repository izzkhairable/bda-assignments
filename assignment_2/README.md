# BDA SMU-IS459 Assignment 2

### Mohamed Izzat Khair Bin Mohamed Noor (01368194)



## Scrapy Project
- For crawling the Hardwarezone PC Gaming Forum
- I included this folder as I made a few changes to the data scrapped by adding a few new fields

### Fields outputted by Scrapy

| Field Name             |
| ---------------------- |
| title                  |
| date_thread_started    |
| author                 |
| content                |
| author_date_joined     |
| author_user_title      |
| author_number_of_posts |
| author_reaction_score  |

### How to run Scrapy Project?

1. Start MongoDB 
```
mongod
```

2. From /assignment_2 directory go to the /hardwarezonezone directory
```
cd Scrapy/hardwarezone/
```

3. Run the following command
```
scrapy crawl hardwarezone
```

4. Check your mongodb to see the data added
```
───> Database: hardwarezone
        |
        ├──> Collection: threads
```


## Spark Project

- Process the data
- Creating graphs, connected components and triangles
- Broke it up into multiple py files for faster processing and results
- Extras: Average reputation for connected components (years joined, number of posts, reaction score)

### How to run Spark Project:
1. Create the new directory
```
hadoop fs -mkdir parquet-input-mine
```

2. Move my input file to the hdfs /parquet-input-mine
```
hadoop fs -put ./spark/all_parquets/input/hardwarezone_mine.parquet parquet-input-mine/
```

3. Check that the input file have successfully been copied to the /parquet-input-mine in hdfs
```
hadoop fs -ls parquet-input-mine
```

4. Go into the /spark directory
```
cd spark 
```

5. Run the first python file 
```
python assignment_2_1_create_cc_n_triangles.py
```
> It might take awhile...

6. Once completed, you should see the following see a table completed with showing only the top 20 rows

7. Check that the output parquet file have been added to Hadoop fs in the /spark-parquet-triangle-output & /spark-parquet-cc-output directory

8. Run the remaining 3 python files in the following sequence,
    1. assignment_2_2_cc_common_words.py
    2. assignment_2_3_cc_avg_triangles.py
    3. assignment_2_4_cc_avg_reputation.py

> Running the python file in sequence is important as each python file requires an input parquet, which is the output parquet of the previous python file.
> After running each of the python, the results will be output in the terminal window. Showing the top 20 rows.
> Additionally, I have added the parquet files outputted by each of python file. It can be found in the /all_parquets directory.
