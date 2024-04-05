import os
import pandas as pd
import re

def word_count(text_file):
    print("当前工作目录:", os.getcwd())
    """
    Count the occurrences of each word in a text file using Spark.

    Parameters:
        text_file (str): Path to the text file.

    Returns:
        dict: A dictionary where keys are words and values are their counts.
    """

    


    # Read input text file
    with open(text_file, "r") as file:
        text = file.read()

    # Split text into words
    words = text.split()

    # Create a Pandas DataFrame with the words
    word_df = pd.DataFrame(words, columns=['word'])


    # Count occurrences of each word
    word_count_df = word_df['word'].value_counts().reset_index()## ADD YOUR CODE HERE
    word_count_df.columns = ['word', 'count']
    

    # Sort the word count dataframe by count in descending order
    sorted_word_counts =word_count_df.sort_values(by='count', ascending=False)## ADD YOUR CODE HERE

   
    # Collect the word counts as a dictionary
    word_count_dict = dict(sorted_word_counts[['word', 'count']].to_numpy())
    
    return word_count_dict
    
