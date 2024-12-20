from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# 初始化SparkSession
spark = SparkSession.builder.appName("City_Avg_Balance").getOrCreate()

#加载数据
user_balance_table = spark.read.option("header", "true").csv("/home/njucs/Desktop/test4/Purchase_Redemption_Data/user_balance_table.csv")
user_profile_table = spark.read.option("header", "true").csv("/home/njucs/Desktop/test4/Purchase_Redemption_Data/user_profile_table.csv")

# 转换字段类型
user_balance_table = user_balance_table.withColumn("tBalance", col("tBalance").cast("int"))

# 注册临时视图
user_balance_table.createOrReplaceTempView("user_balance")
user_profile_table.createOrReplaceTempView("user_profile")

# 使用Spark SQL执行查询：按城市统计2014年3月1日的平均余额
query = """
SELECT up.city, AVG(ub.tBalance) AS avg_balance
FROM user_balance ub
JOIN user_profile up ON ub.user_id = up.user_id
WHERE ub.report_date = '20140301'
GROUP BY up.city
ORDER BY avg_balance DESC
"""

result = spark.sql(query)
result.show()
output_path="/home/njucs/Desktop/test4/work2/output1/"
result.coalesce(1).write.option("header", "true").csv(output_path)