from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from random import randint, choice, uniform
from config import *

spark = (SparkSession.builder
.appName("MySparkApp")
.getOrCreate())

data = []
for step in range(count_records):
    month = randint(1, today.month)
    max_day=31
    if month==2:
        max_day = 28
    elif month in [4,6,9,11]:
        max_day = 30
    day = randint(1, today.day) if month==today.month else randint(1,max_day)
    date = f'{today.year}-{month}-{day}'
    userid = randint(1, count_users)
    product = choice(products)
    count = randint(min_count_products, max_count_products)
    price = uniform(min_price, max_price)
    record = tuple([date, userid, product, count, price])
    data.append(record)

schema = StructType([
    StructField("date", StringType(), True),
    StructField("UserID", IntegerType(), True),
    StructField("product", StringType(), True),
    StructField("count", IntegerType(), True),
    StructField("price", DoubleType(), True)
])

df = spark.createDataFrame(data, schema=schema)

# df.show()

spark.stop()