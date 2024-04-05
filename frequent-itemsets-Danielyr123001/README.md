# PRACTICAL ASSIGNMENT 07: Frequent Itemsets

In this exercise, you will implement the Apriori algorithm. The Apriori algorithm is a classic algorithm used for frequent itemset mining and association rule learning over transactional databases. It works by iteratively finding frequent itemsets of increasing lengths.

## Requirements

- Python 3.x
- Apache Spark
- Mlxtend 

## Setup

1. Install Python 3.x.
2. Install Apache Spark. You can download it from the [official website](https://spark.apache.org/downloads.html) or use a package manager like `brew` on macOS or `apt` on Ubuntu.
3. Install Pandas if you haven't already installed it: `pip install mlxtend`.

## Instructions

### Apriori Algorithm (from Scratch)

- Open the `apriori_scratch.py` file and implement the `apriori` function. This implementation provides a basic version of the Apriori algorithm
- Run the script using `python apriori_scratch.py`.

### Apriori Algorithm (using Mlxtend library)

- Open the `apriori_mlx.py` file and implement the `apriori` function. This implementation calls the apriori method of the Mlxend library.
- Run the script using `python apriori_mlx.py`.


### Apriori Algorithm (using Spark RDDs)

- Open the `apriori_spark_rdd.py` file and implement the `apriori` function. This implementation parallelizes the Apriori algorithm using Spark RDDs (Resilient Distributed Datasets), we can leverage Spark's parallel processing capabilities to handle large-scale datasets efficiently. 
- Run the script using `python apriori_spark_rdd.py`.


### FP-growth algorithm (optimzed version of Apriori)

- Open the `apriori_spark_df.py` file and implement the `init` function. This implementation makes uses of the FPGrowth implemeneted within the spark DF library. FPGrowth implements the FP-growth algorithm.  A FP-Growth model for mining frequent itemsets using the Parallel FP-Growth algorithm (using Spark Dataframes).
- Run the script using `apriori_spark_df.py`.

