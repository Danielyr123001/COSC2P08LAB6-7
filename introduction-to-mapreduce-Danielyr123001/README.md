# PRACTICAL ASSIGNMENT 06: Introduction to Spark

This exercise is designed to introduce you to Apache Spark, a distributed computing framework. In this exercise, you will implement basic data processing tasks such as average calculation, word count, and natural join using different programming paradigms - Pandas, Spark DataFrames, and Spark RDDs.

## Requirements

- Python 3.x
- Apache Spark
- Pandas and Numpy
- Pytest

## Setup

1. Install Python 3.x.
2. Install Apache Spark. You can download it from the [official website](https://spark.apache.org/downloads.html) or use a package manager like `brew` on macOS or `apt` on Ubuntu.
3. Install Pandas if you haven't already installed it: `pip install pandas`.

## Instructions

### Average Calculation

- Go to the folder `average`
- Open the `average_pandas.py` file and implement the `average` function to calculate the average of a given list of numbers using Pandas Series.
- Open the `average_spark_rdd.py` file and implement the `average` function to calculate the average of a given list of numbers using Spark RDD.
- Open the `average_spark_df.py` file and implement the `average` function to calculate the average of a given list of numbers using Spark DataFrames.
- Run the script using `python average_exercise.py`.
- Test your implementation using `pytest test_average.py`

### Word Count

- Go to the folder `word_count`
- Open the `word_count_pandas.py` file and implement the `word_count` function to count the occurrences of each word in a given text file using Pandas DataFrame.
- Open the `word_count_spark_df.py` file and implement the `word_count` function to count the occurrences of each word in a given text file using Spark DataFrames.
- Open the `word_count_spark_rdd.py` file and implement the `word_count` function to count the occurrences of each word in a given text file using using Spark RDDs.
- Run the script using `word_count_exercise.py`.
- Test your implementation using `pytest test_word_count.py`

### Natural Join

- Go to the folder `natural_join`
- Open the `natural_join_pandas.py` file and implement the `natural_join_pandas` function to perform a natural join between two given DataFrames using Pandas DataFrame.
- Open the `natural_join_spark_df.py` file and implement the `natural_join_pandas` function to perform a natural join between two given DataFrames using Spark DataFrames.
- Open the `natural_join_rdd.py` file and implement the `natural_join_pandas` function to perform a natural join between two given RDDs using Spark DataFrames.using Spark RDDs.
- Run the script using `natural_join_exercise.py`.
- Test your implementation using `pytest test_natural_join.py`