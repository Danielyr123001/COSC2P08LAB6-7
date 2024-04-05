import average_pandas as avpd
import average_spark_rdd as avrdd
import average_spark_df as avdf

# Sample data
data = [1, 2, 3, 4, 5]

# Print the average
print("Average Pandas:", avpd.average(data))
print("Average Spark RDD:", avrdd.average(data))
print("Average Spark DataFrame:", avdf.average(data))
