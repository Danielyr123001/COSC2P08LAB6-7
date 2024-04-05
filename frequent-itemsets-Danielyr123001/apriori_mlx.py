# Install mlxtend if you haven't already
# !pip install mlxtend

from mlxtend.frequent_patterns import apriori
from mlxtend.frequent_patterns import association_rules
from mlxtend.preprocessing import TransactionEncoder
import pandas as pd

class Apriori(object):
    def __init__(self, data, min_support=0.2, min_confidence=0.7):# Constructor
      self.min_support = min_support 
      self.min_confidence = min_confidence
      self.transactions = data
   
    def apriori(self):
        '''Implement the Apriori algorithm using the mlxtend library
        
        See documentation for more details: https://rasbt.github.io/mlxtend/user_guide/frequent_patterns/apriori/
        '''

        # The apriori function expects data in a one-hot encoded pandas DataFrame. We need to convert dataframe into one-hot encoded format
        te = TransactionEncoder()
        te_ary = te.fit(self.transactions).transform(self.transactions)
        df = pd.DataFrame(te_ary, columns=te.columns_)

        # Apply Apriori algorithm
        ## ADD YOUR CODE HERE
        frequent_itemsets = apriori(df, min_support=self.min_support, use_colnames=True)
        return frequent_itemsets

        
    def generate_association_rules(self, frequent_itemsets):
         # Generate association rules
        ## ADD YOUR CODE HERE
        associationRules = association_rules(frequent_itemsets, metric="confidence", min_threshold=self.min_confidence)
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

