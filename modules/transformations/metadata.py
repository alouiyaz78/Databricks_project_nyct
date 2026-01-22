from pyspark.sql import DataFrame
from pyspark.sql.functions import  current_timestamp


def add_processed_timestamp(df: DataFrame) -> DataFrame:
    """
    Adds a 'processed_timestamp' column to the DataFrame with the current timestamp.

    Parameters:
    df (DataFrame): Input Spark DataFrame.

    Returns:
    DataFrame: Spark DataFrame with an additional 'processed_timestamp' column.
    """
    return df.withColumn("processed_timestamp", current_timestamp())




