from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import desc
from pyspark.sql.functions import asc, count, countDistinct
from pyspark.sql.functions import sum as Fsum

import datetime
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


def main():
    print('Starting.....')
    spark = SparkSession \
        .builder \
        .appName("Wrangling Data") \
        .getOrCreate()

    # Replace Key with your AWS account key (You can find this on IAM
    spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", "ASIAYZA6G7O6367YXACF")
    # Replace Key with your AWS secret key (You can find this on IAM
    spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key",
                                         "1lszbR/N3xbEg9Brigut9TGlhEQBAzVSYYtz5tu5")
    spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.amazonaws.com")

    dataCount = spark.read \
        .csv("s3://udacity-bucket-mm/cities.csv") \
        .count()
    print(f'We got back:{dataCount}')

