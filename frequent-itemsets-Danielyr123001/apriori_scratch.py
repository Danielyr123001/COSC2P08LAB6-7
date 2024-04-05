import pandas as pd
import numpy as np
from itertools import combinations

## The Apriori algorithm is a classic algorithm used for frequent item set mining and association rule learning over transactional databases. 


## Apriori is a seminal algorithm proposed by R. Agrawal and R. Srikant in 1994 for mining frequent itemsets for Boolean association rules. 
## The name of the algorithm is based on the fact that the algorithm uses prior knowledge of frequent itemset properties. 
## Apriori employs an iterative approach known as a level-wise search, where k-itemsets are used to explore (k + 1)-itemsets. 
## First, the set of frequent 1-itemsets is found by scanning the database to accumulate the count for each item, 
## and collecting those items that satisfy minimum support. The resulting set is denoted by L1. 

## Next, L1 is used to find L2, the set of frequent 2-itemsets, which is used to find L3, and so on, until no more frequent k-itemsets can be found. 
## The finding of each Lk requires one full scan of the database.

## This implementation provides a basic concept of the Apriori algorithm. It takes a list of transactions and a 
## minimum support threshold as input and returns frequent item sets that satisfy the minimum support threshold. 

def apriori(transactions, min_support):
    item_sets = [set(transaction) for transaction in transactions]
    candidates = {frozenset([item]) for item in set.union(*item_sets)}
    frequent_itemsets = set()

    k = 2

    while candidates:
        ## ADD YOUR CODE HERE
        # Calculate the support for each candidate
        itemset_support = {itemset: support_count(transactions, itemset) for itemset in candidates}

        # Keep only itemsets with support above min_support
        candidates = {itemset for itemset, support in itemset_support.items() if support >= min_support}

        # Add the frequent itemsets to the overall list
        frequent_itemsets.update(candidates)

        # Generate new candidates from the current frequent itemsets
        candidates = generate_candidates(candidates, k)
        
        # Prune the candidates based on Apriori property
        candidates = prune_candidates(candidates, frequent_itemsets, k)
        k += 1

    return frequent_itemsets


def generate_association_rules(transactions, frequent_itemsets, min_confidence):
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
                consequent = item_set - antecedent
                confidence = support_count(transactions, item_set) / support_count(transactions, antecedent)
                if confidence >= min_confidence:
                    association_rules.append((antecedent, consequent, confidence))
    return association_rules

def generate_candidates(prev_candidates, k):
    ''' Generates candidate item sets of size k+1 from the frequent item sets of size k found in the previous iteration. 
    For the (k − 1)-itemset, the join is performed, where members of Lk − 1 are joinable if their items are in common. 
    It checks if the **union** of itemset1 and itemset2 results in an item set of size k+1. 
    If it does, it adds this union (candidate item set) to the candidates set.
    
    Parameters:
        prev_candidates: A set containing frequent item sets from the previous iteration, each item set being of size k.
        k: The size of the frequent item sets in prev_freq_itemsets, which is also the size of the item sets we want to generate candidates for.
        
    Return:
        candidates: List of candidates'''

    candidates = set()
    for itemset1 in prev_candidates:
        for itemset2 in prev_candidates:
            if len(itemset1.union(itemset2)) == k:
                candidates.add(itemset1.union(itemset2))
    return candidates


def prune_candidates(candidates, prev_frequent, k):
    ''' Prune the candidate item sets generated in the current iteration based on the support of their subsets. 
    For each candidate item set, we check if all of its subsets of size k-1 are frequent. 
    If any subset is not frequent (i.e., not present in prev_freq_itemsets), it means that the candidate item set cannot be frequent according to the Apriori property.
    If all subsets are frequent, we keep the candidate item set; otherwise, we discard it.
    Pruning is important because it helps reduce the search space by eliminating candidate item sets that cannot be frequent.

    Parameters:
        candidates: A set containing candidate item sets generated in the current iteration.
        prev_frequent: A set containing frequent item sets from the previous iteration.
        k: The size of the candidate item sets being generated.
        
    Return
        pruned_candidates: pruned candidates
    '''
    pruned_candidates = set()
    for candidate in candidates:
        subsets = {frozenset(subset) for subset in combinations(candidate, k-1)}
        if all(subset in prev_frequent for subset in subsets):
            pruned_candidates.add(candidate)
    return pruned_candidates


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
    count = 0
    for transaction in transactions:
        if candidate.issubset(transaction):
            count += 1
    return count/len(transactions)



data = [["Milk", "Onion", "Nutmeg", "Kidney Beans", "Eggs", "Yogurt"],
    ["Dill", "Onion", "Nutmeg", "Kidney Beans", "Eggs", "Yogurt"],
    ["Milk", "Apple", "Kidney Beans", "Eggs"],
    ["Milk", "Unicorn", "Corn", "Kidney Beans", "Yogurt"],
    ["Corn", "Onion", "Kidney Beans", "Ice cream", "Eggs"]]


min_support = 0.6
min_confidence = 0.7

frequent_itemsets = apriori(data, min_support)
print("Frequent Itemsets:", frequent_itemsets)
print(len(frequent_itemsets))

association_rules = generate_association_rules(data, frequent_itemsets, min_confidence)
print("\nAssociation Rules:", association_rules)
print(len(association_rules))