
from pyspark.sql import SparkSession

def natural_join(data1, cols1, data2, cols2, join_key):
    """
        Perform natural join using Spark DataFrames.
    
    Parameters:
        data1 (lst): value list
        data2 (lst): value list
        cols1 (lst): data1 column names
        cols2 (lst): data2 column names
        join_key (str): join key

    Returns:
        result: joined dataframe
    """
    spark = SparkSession.builder.appName("NaturalJoin").getOrCreate()
    
    df1 = spark.createDataFrame(data1, schema=cols1)
    df2 = spark.createDataFrame(data2, schema=cols2)

    joined_df =df1.join(df2, join_key) ## ADD YOUR CODE HERE

    result = joined_df.collect()
    spark.stop()
    return result

