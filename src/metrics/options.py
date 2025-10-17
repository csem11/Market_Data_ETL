import pandas as pd
import datetime

from pyspark.sql import SparkSession
from pyspark.sql import DataFrame as SparkDF
from pyspark.sql.functions import col, when
from pyspark.sql.functions import datediff, to_date, lit

from typing import Optional

from ..utils.spark_utils import get_spark_session



def calc_bid_ask_spread(df, spark_session: Optional[SparkSession] = None):
    """
    Calculate bid-ask spread as a new column 'bid_ask_spread' and percent spread 'bid_ask_spread_pct'.
    Accepts either pandas or pyspark DataFrame as input, always returns pyspark DataFrame.

    Args:
        df (pd.DataFrame or pyspark.sql.DataFrame): Input DataFrame with 'bid' and 'ask' columns.
        spark_session: Optional Spark session to use (if None, will get from manager)

    Returns:
        pyspark.sql.DataFrame: DataFrame with new columns 'bid_ask_spread' and 'bid_ask_spread_pct'.
    """
    # Get Spark session
    if spark_session is None:
        spark_session = get_spark_session()
    
    # If input is pandas DataFrame, convert to Spark DataFrame
    if isinstance(df, pd.DataFrame):
        spark_df = spark_session.createDataFrame(df)
    elif isinstance(df, SparkDF):
        spark_df = df
    else:
        raise ValueError("Input must be a pandas DataFrame or pyspark.sql.DataFrame")

    spark_df = spark_df.withColumn(
        "bid_ask_spread", col("ask") - col("bid")
    ).withColumn(
        "bid_ask_spread_pct",
        when(
            col("ask") != 0,
            (col("ask") - col("bid")) / col("ask")
        ).otherwise(None)
    )
    return spark_df

    def calc_days_to_expiration(df, spark_session: Optional[SparkSession] = None):
        """
        Calculate days to expiration as a new column 'days_to_expiration'.
        Uses today's date and 'expiration_date'/'eff_date' columns. Accepts either pandas or pyspark DataFrame,
        always returns pyspark DataFrame.

        Args:
            df (pd.DataFrame or pyspark.sql.DataFrame): Input DataFrame with 'expiration_date' (str) and 'eff_date' (datetime) columns.
            spark_session: Optional Spark session to use (if None, will get from manager)

        Returns:
            pyspark.sql.DataFrame: DataFrame with new column 'days_to_expiration'.
        """

        # Get Spark session
        if spark_session is None:
            spark_session = get_spark_session()

        # If input is pandas DataFrame, convert to Spark DataFrame
        if isinstance(df, pd.DataFrame):
            # Ensure 'eff_date' and 'expiration_date' are available and in string/ISO format as required
            if 'eff_date' in df.columns:
                df = df.copy()
                df['eff_date'] = pd.to_datetime(df['eff_date'])
            if 'expiration_date' in df.columns:
                df = df.copy()
                df['expiration_date'] = pd.to_datetime(df['expiration_date']).dt.strftime("%Y-%m-%d")
            spark_df = spark_session.createDataFrame(df)
        elif isinstance(df, SparkDF):
            spark_df = df
        else:
            raise ValueError("Input must be a pandas DataFrame or pyspark.sql.DataFrame")
        
        # Convert columns to date type if not already
        spark_df = spark_df.withColumn("eff_date", to_date(col("eff_date"))) \
                           .withColumn("expiration_date", to_date(col("expiration_date")))

        # Calculate days_to_expiration as expiration_date - eff_date
        spark_df = spark_df.withColumn(
            "days_to_expiration",
            datediff(col("expiration_date"), col("eff_date"))
        )
        return spark_df