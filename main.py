from pyspark.sql import SparkSession
import pyspark
# spark=SparkSession.builder.appName("spark-app").master("local[*]").getOrCreate() # type: ignore

# df1= spark.read.format("csv") \
#     .option("inferSchema",True) \
#     .option("header",True)    \
#     .load("orders.csv")
# df1.show()


print(pyspark.__version__)
