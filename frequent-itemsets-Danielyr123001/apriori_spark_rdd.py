from pyspark.sql import SparkSession
from itertools import combinations


def apriori(transactions_rdd, min_support):
    k = 1
    frequent_itemsets = set()

    # Step 1: Generate candidate itemsets of size k
    candidates_rdd = transactions_rdd.flatMap(lambda basket: set(combinations(basket, k)))
    
    # Iterate until no frequent itemsets are found
    while True:
        
        # For each transaction in the RDD, it generates all possible combinations of items of size k using the combinations function from the itertools module.
        # The flatMap transformation ensures that the output is flattened into a single list of itemsets across all baskets.
        ## ADD YOUR CODE HERE
        candidates_rdd = transactions_rdd.flatMap(lambda basket: [frozenset(comb) for comb in combinations(basket, k)])

        
        

        
        # The reduceByKey transformation is applied to aggregate the counts of each itemset.
        # It combines the counts of the same itemsets across different transactions.
        # The key-value pairs consist of the itemset as the key and its count as the value.
        ## ADD YOUR CODE HERE
        itemset_counts_rdd = candidates_rdd.map(lambda itemset: (itemset, 1)).reduceByKey(lambda a, b: a + b)
        
        
        
        

        # PRUNNING STEP: The filter transformation is used to retain only the itemsets with counts greater than or equal to the minimum support threshold (min_support).
        # It ensures that only frequent itemsets are retained while removing infrequent ones.
        ## ADD YOUR CODE HERE
        local_frequent_itemsets_rdd = itemset_counts_rdd.filter(lambda itemset_count: itemset_count[1] >= min_support)

        # The map transformation is applied to convert each itemset-count pair into a tuple (itemset, count).
        # It prepares the data for further processing or for output.
        local_frequent_itemsets_rdd = local_frequent_itemsets_rdd.map(lambda itemset_count: itemset_count[0])

        # Step 3: ReduceByKey for global frequent itemsets
        global_frequent_itemsets = local_frequent_itemsets_rdd.collect()
    
        frequent_itemsets.update(global_frequent_itemsets)
        
        if not global_frequent_itemsets:
            # No frequent itemsets of size k were found, terminate the loop
            break
        
        k += 1
        #Generate candidate itemsets of size k
        candidates_rdd = transactions_rdd.flatMap(lambda basket: set(combinations(basket, k)))
        
        # Prune (Apriori) Filter transactions that contain the candidate itemset
        candidates_rdd = candidates_rdd.map(
            lambda candidate: (candidate, set(combinations(candidate, k - 1)))
        )
        candidates_rdd = candidates_rdd.filter(
            lambda candidate_subset: check_frequent(candidate_subset[1], global_frequent_itemsets)
        )
        candidates_rdd = candidates_rdd.map(lambda candidate_subset: candidate_subset[0])

    return frequent_itemsets

def check_frequent(subsets, prev_frequent):
    return all(subset in prev_frequent for subset in subsets)

def generate_association_rules(transactions_rdd, frequent_itemsets, min_confidence):
    '''Generates association rules from the frequent item sets discovered in the dataset. 
        The function iterates over each item set size in frequent_itemsets, starting from size 2.
        For each item set of a particular size, it iterates over each item set in that size category.
        For each item set, it generates all possible combinations of antecedents and consequents. 
        An antecedent is a subset of the item set, and the consequent is the complement of the antecedent in the item set.
        It calculates the confidence of each association rule. 
        Confidence is calculated as the ratio of the support count of the item set divided by the support count of the antecedent. 
        This represents the likelihood that the consequent appears in a transaction given that the antecedent appears.
        If the confidence of an association rule meets or exceeds the min_confidence threshold, the association rule is considered significant and added to the list of association rules.

    Parameters:
        frequent_itemsets: A dictionary containing frequent item sets discovered by the Apriori algorithm, where keys represent the size of the item sets, and values represent the actual item sets along with their support counts.
        min_confidence: The minimum confidence threshold for generating association rules.

    Return:
        the list of significant association rules.
    '''
    association_rules = []
    for item_set in frequent_itemsets:
        if len(item_set) < 2:
            continue
        for i in range(1, len(item_set)):
            for antecedent in combinations(item_set, i):
                antecedent = frozenset(antecedent)
                consequent = frozenset(item_set) - antecedent
                antecedent_support = transactions_rdd.filter(lambda transaction: antecedent.issubset(transaction)).count()
                consequent_support = transactions_rdd.filter(lambda transaction: consequent.issubset(transaction)).count()
                confidence = antecedent_support / consequent_support
                if confidence >= min_confidence:
                    association_rules.append((antecedent, consequent, confidence))
    
    return association_rules

def support_count(transactions, candidate):
    ''' Support count of an item set is the number of transactions in the dataset that contain that particular item set. 
    The support count is used to determine whether an item set is frequent or not based on a minimum support threshold.

    The function iterates through each transaction in the dataset.
    For each transaction, it checks if the itemset is a **subset** of that transaction. If it is, it means that the transaction contains all the items in the itemset.
    If the itemset is a subset of the transaction, the support count is incremented.

    Parameters:
        transactions: The transactional dataset.
        candidate: The item set whose support count needs to be calculated.

    Return:
        count: count of the item set
    '''
    
    candidate = set(candidate)
    count = 0
    for transaction in transactions:
        if candidate.issubset(transaction):
            count += 1
    return count/len(transactions)
    


transactions = [["Milk", "Onion", "Nutmeg", "Kidney Beans", "Eggs", "Yogurt"],
        ["Dill", "Onion", "Nutmeg", "Kidney Beans", "Eggs", "Yogurt"],
        ["Milk", "Apple", "Kidney Beans", "Eggs"],
        ["Milk", "Unicorn", "Corn", "Kidney Beans", "Yogurt"],
        ["Corn", "Onion", "Kidney Beans", "Ice cream", "Eggs"]]


min_support = 0.6
min_confidence = 0.7


spark = SparkSession\
    .builder\
    .appName("AssociationRulesRDD")\
    .getOrCreate()


transactions_rdd = spark.sparkContext.parallelize(transactions)

num_transactions = transactions_rdd.count()
min_support = min_support * num_transactions

frequent_itemsets = apriori(transactions_rdd, min_support)
print("Frequent Itemsets:")
print(frequent_itemsets)
print(len(frequent_itemsets))

association_rules = generate_association_rules(transactions_rdd, frequent_itemsets, min_confidence)
print("\nAssociation Rules:")
print(association_rules)
print(len(association_rules))

spark.stop()