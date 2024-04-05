import pandas as pd 

def natural_join(data1, cols1, data2, cols2, join_key):
    """Perform natural join using pandas.
    Parameters:
        data1 (lst): value list
        data2 (lst): value list
        cols1 (lst): data1 column names
        cols2 (lst): data2 column names
        join_key (str): join key

    Returns:
        result: joined dataframe
    """
    df1 = pd.DataFrame(data = data1,  columns = cols1) 
    df2 = pd.DataFrame(data = data2,  columns = cols2) 
    
    joined_df = pd.merge(df1, df2, on=join_key)
    
    return joined_df ## ADD YOUR CODE HERE
    