import sys
from operator import add

from pyspark.sql import SparkSession
from pyspark.sql.functions import split, explode, col


def word_count(text_file):
    """
    Count the occurrences of each word in a text file using Spark.

    Parameters:
        text_file (str): Path to the text file.

    Returns:
        dict: A dictionary where keys are words and values are their counts.
    """


    spark = SparkSession\
        .builder\
        .appName("WordCountDF")\
        .getOrCreate()

    df = spark.read.text(text_file)

    # Split each line into words
    words_df = df.withColumn('word', explode(split(col('value'), ' ')))
    
    # Group by words and count occurrences
    word_count_df = words_df.groupBy('word').count()## ADD YOUR CODE HERE

    #Sort results by count
    word_count_df = word_count_df.orderBy('count', ascending=False) ## ADD YOUR CODE HERE
    
    # Show the word count
    word_count_dict = dict(word_count_df.collect())
  
    # Stop SparkSession
    spark.stop()

    return word_count_dict