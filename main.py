from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from random import randint, choice, uniform
from config import *

def get_date(today: datetime):
    year = choice([today.year-1, today.year])
    month = randint(1, today.month) if year==today.year else randint(today.month, 12)
    max_day = 31
    if month == 2:
        max_day = 28
    elif month in [4, 6, 9, 11]:
        max_day = 30
    day = randint(1, today.day) if month == today.month and year==today.year else randint(1, max_day)
    month = str(month) if month>9 else '0'+str(month)
    day = str(day) if day>9 else '0'+str(day)
    date = f'{year}-{month}-{day}'
    return date

spark = (SparkSession.builder
.appName("MySparkApp")
.getOrCreate())

data = []
for step in range(count_records):
    userid = randint(1, count_users)
    product = choice(products)
    count = randint(min_count_products, max_count_products)
    price = round(uniform(min_price, max_price),2)
    record = tuple([get_date(today=today), userid, product, count, price])
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
df.coalesce(1).write.csv('mySyntheticData', header=True)

spark.stop()