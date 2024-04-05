import natural_join_pandas as njpd
import natural_join_rdd as njrdd
import natural_join_spark_df as njdf

# Sample DataFrames for testing
data1 = [(1, "Alice"), (2, "Bob"), (3, "Charlie")]
data2 = [(1, 25), (2, 30), (4, 40)]

columns1 = ["id", "name"]
columns2 = ["id", "age"]

print("Pandas: ", njpd.natural_join(data1, columns1, data2, columns2, "id"))
print("Spark Dataframe: ", sorted(njdf.natural_join(data1, columns1, data2, columns2, "id")))
print("Spark RDD: ", njrdd.natural_join(data1, data2))