from pyspark.sql import SparkSession

def average(data):
    """
    Perform average calculation using Spark RDD.
    This task involves mapping each number to a tuple of (number, 1) (to represent sum and count), 
    and then reducing by summing up all the numbers and counts, and 
    finally dividing the total sum by the total count.


    Parameters:
        data1 (lst): value list

    Returns:
        result (float): average 
    """
    
    spark = SparkSession.builder \
        .appName("AverageCalculationRDD") \
        .getOrCreate()
    
    # Create RDD from the sample data
    rdd = spark.sparkContext.parallelize(data)

    # Calculate sum and count using map operation
    rdd_map = rdd.map(lambda x: (x, 1))## ADD YOUR CODE HERE
    sum_count = rdd_map.reduce(lambda x, y: (x[0] + y[0], x[1] + y[1])) ## ADD YOUR CODE HERE

    # Calculate average
    average = sum_count[0] / sum_count[1]

    # Stop SparkContext
    spark.stop()
    return average