from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr

# 初始化SparkSession
spark = SparkSession.builder.appName("TopUsersByCity").getOrCreate()

#加载数据
user_balance_table = spark.read.option("header", "true").csv("/home/njucs/Desktop/test4/Purchase_Redemption_Data/user_balance_table.csv")
user_profile_table = spark.read.option("header", "true").csv("/home/njucs/Desktop/test4/Purchase_Redemption_Data/user_profile_table.csv")

# 转换字段类型
user_balance_table = user_balance_table \
    .withColumn("total_purchase_amt", col("total_purchase_amt").cast("int")) \
    .withColumn("total_redeem_amt", col("total_redeem_amt").cast("int"))

# 注册临时视图
user_balance_table.createOrReplaceTempView("user_balance")
user_profile_table.createOrReplaceTempView("user_profile")

# 使用Spark SQL进行查询:统计每个城市前3名总流量用户
query = """
WITH UserTotalFlow AS (
    SELECT up.city,ub.user_id,SUM(ub.total_purchase_amt + ub.total_redeem_amt) AS total_flow
    FROM user_balance ub JOIN user_profile up ON ub.user_id = up.user_id
    WHERE ub.report_date BETWEEN '20140801' AND '20140831'
    GROUP BY up.city, ub.user_id),
RankedUsers AS (
    SELECT city,user_id,total_flow,
    ROW_NUMBER() OVER (PARTITION BY city ORDER BY total_flow DESC) AS rank 
    FROM UserTotalFlow
)
SELECT city, user_id, total_flow
FROM RankedUsers
WHERE rank <= 3 
ORDER BY city, rank;
"""

result = spark.sql(query)
result.show()
output_path="/home/njucs/Desktop/test4/work2/output2/"
result.coalesce(1).write.option("header", "true").csv(output_path)

