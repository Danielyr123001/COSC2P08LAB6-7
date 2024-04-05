from pyspark.sql import SparkSession

#This implementation reads a text file into an RDD, 
#splits each line into words, and then uses flatMap to 
#flatten the list of words. It maps each word to a tuple 
#of (word, 1) to prepare for counting and then 
#reduces by key to obtain the count of occurrences 
#for each word. Finally, it collects the word counts as 
#a dictionary. 

#Make sure you have PySpark installed and configured properly to run this code.


def word_count(text_file):
    """
    Count the occurrences of each word in a text file using Spark.

    Parameters:
        text_file (str): Path to the text file.

    Returns:
        dict: A dictionary where keys are words and values are their counts.
    """
    spark = SparkSession.builder \
        .appName("WordCountRDD") \
        .getOrCreate()

    
    # Read the text file into an RDD
    lines = spark.sparkContext.textFile(text_file)

    # Split each line into words and flatten the result
    words = lines.flatMap(lambda line: line.split())
    
    # Map each word to a tuple of (word, 1) and then reduce by key to count occurrences
    pairs = words.map(lambda word: (word, 1)) ## ADD YOUR CODE HERE
    word_counts = pairs.reduceByKey(lambda a, b: a + b) ## ADD YOUR CODE HERE

 
    # Sort the word counts by count value in descending order
    sorted_word_counts = word_counts.sortBy(lambda x: x[1], ascending=False) ## ADD YOUR CODE HERE
    
    # Collect the word counts as a dictionary
    word_count_dict = dict(sorted_word_counts.collect())

    spark.stop()
    return word_count_dict


