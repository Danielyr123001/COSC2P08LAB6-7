from pyspark.sql import SparkSession

def natural_join(data1, data2):
    """Perform natural join using Spark RDD.
    
    Parameters:
        data1 (lst): value list
        data2 (lst): value list
        
    Returns:
        result: joined RDD
    """

    spark = SparkSession.builder \
        .appName("NaturalJoinRDD") \
        .getOrCreate()
    
    # Sample RDDs
    rdd1 = spark.sparkContext.parallelize(data1)
    rdd2 = spark.sparkContext.parallelize(data2)

    # Extract keys and values from RDDs
    rdd1_key_values = rdd1.map(lambda x: (x[0], x[1:]))## ADD YOUR CODE HERE
    rdd2_key_values = rdd2.map(lambda x: (x[0], x[1:]))## ADD YOUR CODE HERE

    # Perform join
    joined_rdd = rdd1_key_values.join(rdd2_key_values) ## ADD YOUR CODE HERE

    # Flatten the result
    flattened_rdd = joined_rdd.map(lambda x: (x[0], *x[1][0], *x[1][1]))

    result = flattened_rdd.collect()
    # Stop SparkContext
    spark.stop()
    
    return result



