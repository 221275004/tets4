from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, dayofyear, year, month, sum
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml import Pipeline
from pyspark.sql import functions as F


# 初始化 Spark 会话
spark = SparkSession.builder.appName("PurchaseRedeemPrediction").getOrCreate()

# 加载数据
user_balance_table = spark.read.option("header", "true").csv("/home/njucs/Desktop/test4/Purchase_Redemption_Data/user_balance_table.csv")

user_balance_table = user_balance_table.withColumn("total_purchase_amt", F.col("total_purchase_amt").cast("double"))
user_balance_table = user_balance_table.withColumn("total_redeem_amt", F.col("total_redeem_amt").cast("double"))

# 计算用户的每日申购和赎回金额
daily_data = user_balance_table.groupBy("report_date").agg(
    F.sum("total_purchase_amt").alias("purchase"),
    F.sum("total_redeem_amt").alias("redeem")
)

# 从 report_date 字符串中提取年、月、日作为特征
daily_data = daily_data.withColumn("year", F.substring("report_date", 1, 4).cast("int"))
daily_data = daily_data.withColumn("month", F.substring("report_date", 5, 2).cast("int"))
daily_data = daily_data.withColumn("day", F.substring("report_date", 7, 2).cast("int"))

# 使用 year, month, day 作为特征列
assembler = VectorAssembler(inputCols=["year", "month", "day"], outputCol="features", handleInvalid="skip")
daily_data = assembler.transform(daily_data)
# 显示数据，查看结果
daily_data.show()

# 线性回归模型预测申购金额（purchase）
lr_purchase = LinearRegression(featuresCol="features", labelCol="purchase")
lr_model_purchase = lr_purchase.fit(daily_data)

# 线性回归模型预测赎回金额（redeem）
lr_redeem = LinearRegression(featuresCol="features", labelCol="redeem")
lr_model_redeem = lr_redeem.fit(daily_data)

dates_sept_2014 = spark.createDataFrame(
    [(f"201409{day:02d}",) for day in range(1, 31)], ["report_date"]
)

# 从 report_date 字符串中提取年、月、日作为特征
dates_sept_2014 = dates_sept_2014.withColumn("year", F.substring("report_date", 1, 4).cast("int"))
dates_sept_2014 = dates_sept_2014.withColumn("month", F.substring("report_date", 5, 2).cast("int"))  
dates_sept_2014 = dates_sept_2014.withColumn("day", F.substring("report_date", 7, 2).cast("int")) 
# 通过 VectorAssembler 构建特征
dates_sept_2014 = assembler.transform(dates_sept_2014)
dates_sept_2014.show()
# 使用线性回归模型进行预测
predictions_purchase = lr_model_purchase.transform(dates_sept_2014)
predictions_redeem = lr_model_redeem.transform(dates_sept_2014)

# 合并申购和赎回的预测结果
result = predictions_purchase.select("report_date", "prediction").withColumnRenamed("prediction", "purchase")
result = result.join(predictions_redeem.select("report_date", "prediction").withColumnRenamed("prediction", "redeem"), on="report_date", how="inner")
# 去掉千分位，直接输出为数字格式
result = result.withColumn("purchase", F.col("purchase").cast("bigint")) \
               .withColumn("redeem", F.col("redeem").cast("bigint"))
# 输出
result.show(truncate=False)
output_path="/home/njucs/Desktop/test4/work3/output/"
result.coalesce(1).write.option("header", "true").csv(output_path)

# 停止 Spark 会话
spark.stop()