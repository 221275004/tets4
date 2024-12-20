from pyspark import SparkContext, SparkConf

# 设置 Spark 配置和上下文
conf = SparkConf().setAppName("Calculate Flow").setMaster("local[*]")
sc = SparkContext(conf=conf)
# 加载数据
file_path = "/home/njucs/Desktop/test4/Purchase_Redemption_Data/user_balance_table.csv"
data = sc.textFile(file_path)

def parse_line(line):
    # 按照逗号分割字段
    fields = line.split(",")
    # 跳过表头行
    if fields[0] == "user_id":
        return None

    report_date = fields[1]  # 日期
    total_purchase_amt = int(fields[4])  # 购买总金额
    total_redeem_amt = int(fields[8])  # 赎回总金额
    
    return (report_date, total_purchase_amt, total_redeem_amt)

# 解析每一行数据
parsed_data = data.map(parse_line).filter(lambda x: x is not None)
# 按日期进行计算资金流入和流出
aggregated_data = parsed_data.map(lambda x: (x[0], (x[1], x[2]))).reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
# 显示结果
result = aggregated_data.collect()
for record in result:
    print(f"{record[0]} {record[1][0]} {record[1][1]}")
# 保存输出结果
output_path = "/home/njucs/Desktop/test4/work1/output1/"
aggregated_data.map(lambda x: f"{x[0]} {x[1][0]} {x[1][1]}").coalesce(1).saveAsTextFile(output_path)
# 关闭 SparkContext
sc.stop()
