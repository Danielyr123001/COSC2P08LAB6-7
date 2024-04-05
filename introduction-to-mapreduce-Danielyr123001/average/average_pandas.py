import pandas as pd


def average(data):
    '''
    Perform average calculation using Pandas Series.
    
    Parameters:
        data1 (lst): value list

    Returns:
        result (float): average 
    '''
    df = pd.Series(data)
    return df.mean()
   ## ADD YOUR CODE HERE