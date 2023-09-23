from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('NSYE Count').master('local').getOrCreate()

df = spark.read.csv('../data/nyse_all/nyse_data/*', schema='''
    stock_id STRING, trans_date STRING, open_price FLOAT, low_price FLOAT, high_price FLOAT, close_price FLOAT, volume BIGINT
''')
print('aa')
df.count()