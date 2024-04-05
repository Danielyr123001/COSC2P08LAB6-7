from pyspark.sql import SparkSession
from pyspark.sql.functions import avg

def average(data):
    """
        Perform average calculation using Spark Dataframes.
    
    Parameters:
        data1 (lst): value list

    Returns:
        result (float): average 
    """
    
    spark = SparkSession.builder \
        .appName("AverageCalculationRDD") \
        .getOrCreate()
    
    # Create DataFrame from the sample data
    df = spark.createDataFrame([(x,) for x in data], ["value"])

    # Calculate average using agg function
    average_df =df.agg(avg("value").alias("average"))
 ## ADD YOUR CODE HERE

    # Extract average value from DataFrame
    average = average_df.collect()[0]["average"]## ADD YOUR CODE HERE

    # Stop SparkContext
    spark.stop()
    return average