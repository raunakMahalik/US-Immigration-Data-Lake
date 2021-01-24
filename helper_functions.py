# Library imports
import pandas as pd

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, isnan, when, udf

import matplotlib.pyplot as plt
import seaborn as sns


def plot_nulls(df, isSpark = False):
    """Description:
        Display the percentage missing rows of every column in the input dataframe.
    
        Arguments:
            df {pandas dataframe}: Input pandas dataframe.
            isSpark{Boolean}: Determines if the dataframe is a spark dataframe or not
                              It is set to True when the input is a spark dataframe.

        Returns:
            None
    """
    if isSpark:
        nan_df = df.select([count(when(isnan(cl) | col(cl).isNull(), cl)).alias(cl) \
                            for cl in df.columns]).toPandas()
        # Format conversion
        nan_df = pd.melt(nan_df, var_name='cols', value_name='values')
        # count the number of rows in df
        tot_rows = df.count()
    else:
        nan_df = pd.DataFrame(data= df.isnull().sum(), columns=['values']) 
        nan_df = nan_df.reset_index()
        nan_df.columns = ['cols', 'values']
        tot_rows = df.shape[0]

    # Compute percentage missing values
    nan_df['Percentage null values'] = 100*nan_df['values']/tot_rows
    
    # plot the missing values
    plt.figure(figsize=(8,4))
    ax = sns.barplot(x='cols', y='Percentage null values', data = nan_df)
    ax.set_ylim(auto = True)
    ax.set_xticklabels(ax.get_xticklabels(), rotation=90)
    plt.show()

    return


def preprocess_immigration_dataframe(df, isSpark = False):
    """Description:
       Cleans the immigaration data by doing the following:
        1. Drop columns with excessive Null values.
        2. Delete records with missing values.
    
        Arguments:
            df {pandas dataframe}: Input pandas dataframe.
            isSpark{Boolean}: Determines if the dataframe is a spark dataframe or not
                              It is set to True when the input is a spark dataframe.
            
        Returns:
            out_df {pandas dataframe}: cleaned pandas dataframe.
    """
    # Columns with over 60-90% missing values
    nan_cols = ['visapost', 'entdepu', 'occup', 'insnum']
    
    print("preprocess_immigration_dataframe: Deleting Columns with missing values")
    # Drop columns with missing values
    if isSpark:
        temp_df = df.drop(*nan_cols)
    else:
        temp_df = df.drop(columns = nan_columns, axis=1)
    
    print("preprocess_immigration_dataframe: Columns deleted succesfully")
    # Delete records with missing values
    print("preprocess_immigration_dataframe: Deleting records with missing values")
    out_df = temp_df.dropna()
    print("preprocess_immigration_dataframe: Records deleted succesfully")

    return out_df


def preprocess_demographics_dataframe(df, isSpark = False):
    """Description:
       Cleans the immigaration data by doing the following:
        1. Drop columns with excessive Null values.
        2. Delete records with missing values.
        3. Delete rows with duplicate values.
    
        Arguments:
            df {pandas dataframe}: Input pandas dataframe.
            isSpark{Boolean}: Determines if the dataframe is a spark dataframe or not
                              It is set to True when the input is a spark dataframe.
            
        Returns:
            out_df {pandas dataframe}: cleaned pandas dataframe.
    """
    
    # Delete records with missing values
    print("preprocess_demographics_dataframe: Deleting records with missing values")
    nan_cols = ['Male Population', 'Female Population',
                'Number of Veterans', 'Foreign-born', 'Average Household Size']
    temp_df = df.dropna(subset = nan_cols)
    print("preprocess_demographics_dataframe: Records deleted succesfully")
    
    # Delete duplicate records
    print("preprocess_demographics_dataframe: Deleting records with duplicate values")
    dup_cols = ['City', 'State', 'State Code', 'Race']
    if isSpark:
        out_df = temp_df.dropDuplicates(subset = dup_cols)
    else:
        out_df = temp_df.drop_duplicates(subset = dup_cols)
    print("preprocess_demographics_dataframe: Records deleted succesfully")
    
    return out_df


def preprocess_airports_dataframe(df, isSpark = False):
    """Description:
       Cleans the immigaration data by doing the following:
        1. Drop columns with excessive Null values.
        2. Delete records with missing values.
        3. Delete rows with duplicate values.
    
        Arguments:
            df {pandas dataframe}: Input pandas dataframe.
            isSpark{Boolean}: Determines if the dataframe is a spark dataframe or not
                              It is set to True when the input is a spark dataframe.
            
        Returns:
            out_df {pandas dataframe}: cleaned pandas dataframe.
    """
    # Drop columns with missing values
    nan_cols = ['continent', 'local_code']
    print("preprocess_airports_dataframe: Deleting Columns with missing values")
    if isSpark:
        temp_df = df.drop(*nan_cols)
    else:
        temp_df = df.drop(columns = nan_cols, axis=1)
    print("preprocess_airports_dataframe: Columns deleted succesfully")
    
    # Delete duplicate records
    print("preprocess_airports_dataframe: Deleting records with duplicate values")
    dup_cols = ['iata_code']
    if isSpark:
        temp_df = temp_df.dropDuplicates(subset = dup_cols)
    else:
        temp_df = temp_df.drop_duplicates(subset = dup_cols)
    print("preprocess_airports_dataframe: Records deleted succesfully")

    # Delete records with missing values
    print("preprocess_airports_dataframe: Deleting records with missing values")
    out_df = temp_df.dropna()
    print("preprocess_airports_dataframe: Records deleted succesfully")
    
    return out_df