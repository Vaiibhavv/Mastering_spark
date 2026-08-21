from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import pyspark
from pyspark.sql import Window

spark=SparkSession.builder.appName("spark-app").master("local[*]").getOrCreate() # type: ignore

# df1= spark.read.format("csv") \
#     .option("inferSchema",True) \
#     .option("header",True)    \
#     .load("orders.csv")
# #df1.show()



linkedin_data = [
    (1, 'Microsoft', 'developer', '2020-04-13', '2021-11-01'),
    (1, 'Google', 'developer', '2021-11-01', None),
    (2, 'Google', 'manager', '2021-01-01', '2021-01-11'),
    (2, 'Microsoft', 'manager', '2021-01-11', None),
    (3, 'Microsoft', 'analyst', '2019-03-15', '2020-07-24'),
    (3, 'Amazon', 'analyst', '2020-08-01', '2020-11-01'),
    (3, 'Google', 'senior analyst', '2020-11-01', '2021-03-04'),
    (4, 'Google', 'junior developer', '2018-06-01', '2021-11-01'),
    (4, 'Google', 'senior developer', '2021-11-01', None),
    (5, 'Microsoft', 'manager', '2017-09-26', None),
    (6, 'Google', 'CEO', '2015-10-02', None)
]

linkedn_schema= [
  'emp_id', 
  'employer', 
  'position', 
  'start_date', 
  'end_date'
]

employee_data=spark.createDataFrame(linkedin_data,schema=linkedn_schema)
employee_data.show()

## 
# print("Next Employeer ---------------####---------------------------------------------")

# window_spec= Window.partitionBy("emp_ID").orderBy(col("start_date").asc())
# employee_switch=employee_data.withColumn("next_employee",lead("employer",1).over(window_spec))

# #employee_switch.show()

# print("-----------------filter with next employee -------------------")

# filter_data=employee_switch.filter((col("employer")=="Microsoft") & (col("next_employee")=="Google"))
# #filter_data.show()

# employee_switch.createOrReplaceTempView("employeee_data_table")

# sql_df=spark.sql("(select * from employeee_data_table")
# sql_df.show()