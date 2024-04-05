
import pandas as pd

import pyspark.pandas as ps
from pyspark.sql import SparkSession
from pyspark.ml.fpm import FPGrowth


# Mining frequent items, itemsets, subsequences, or other substructures is usually among the first steps to analyze a large-scale dataset, 
# which has been an active research topic in data mining for years

# We are using the FP-growth algorithm (optimzed version of Apriori). FPGrowth implements the FP-growth algorithm. 
# A FP-Growth model for mining frequent itemsets using the Parallel FP-Growth algorithm (using Spark Dataframes).

# Refer to https://spark.apache.org/docs/latest/ml-frequent-pattern-mining.html#fp-growth
# and https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.ml.fpm.FPGrowthModel.html for more details

class Apriori(object):
    def __init__(self, data, min_support=0.2, min_confidence=0.7):# Constructor
      self.min_support = min_support 
      self.min_confidence = min_confidence

      self.spark = SparkSession\
        .builder\
        .appName("AssociationRulesDF")\
        .getOrCreate()
      
      df = ps.DataFrame({"items": data})
      self.transactions = df.to_spark(index_col="index")
      
      # Train FP-Growth model
      ## ADD YOUR CODE HERE
      self.spark = SparkSession.builder.appName("AssociationRulesFP").getOrCreate()

      # 将数据转换为字典列表格式
      spark_data = self.spark.createDataFrame([{"items": transaction} for transaction in data])
        
      # 训练FP-Growth模型
      fpGrowth = FPGrowth(itemsCol="items", minSupport=self.min_support, minConfidence=self.min_confidence)
      self.model = fpGrowth.fit(spark_data)


      
    def apriori(self):
        # Display frequent itemsets.
        freqItemsets = self.model.freqItemsets
        freqItemsets = freqItemsets.toPandas()
        return freqItemsets

    def generate_association_rules(self, freqItemsets = None):
        # Display generated association rules.
        associationRules = self.model.associationRules
        associationRules = associationRules.toPandas()
        self.spark.stop()
        return associationRules
    
  

data = [["Milk", "Onion", "Nutmeg", "Kidney Beans", "Eggs", "Yogurt"],
        ["Dill", "Onion", "Nutmeg", "Kidney Beans", "Eggs", "Yogurt"],
        ["Milk", "Apple", "Kidney Beans", "Eggs"],
        ["Milk", "Unicorn", "Corn", "Kidney Beans", "Yogurt"],
        ["Corn", "Onion", "Kidney Beans", "Ice cream", "Eggs"]]


min_support = 0.6
min_confidence = 0.7

apriori_instance = Apriori(data, min_support, min_confidence)

frequent_itemsets = apriori_instance.apriori()
print("Frequent Itemsets:")
print(frequent_itemsets)
print(len(frequent_itemsets))

association_rules = apriori_instance.generate_association_rules(frequent_itemsets)
print("\nAssociation Rules:")
print(association_rules)
print(len(association_rules))