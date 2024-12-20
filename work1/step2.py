from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("Active Users in August 2014").setMaster("local[*]")
sc = SparkContext(conf=conf)

file_path = "/home/njucs/Desktop/test4/Purchase_Redemption_Data/user_balance_table.csv"
data = sc.textFile(file_path)

def parse_line(line):
    fields = line.split(",")
    
    if fields[0] == "user_id":
        return None
    
    user_id = fields[0]  # 用户ID
    report_date = fields[1]  # 日期
    
    # 如果日期是2014年8月，返回元组 (user_id, report_date)
    if report_date.startswith("201408"):
        return (user_id, report_date)
    else:
        return None

# 过滤2014年8月的数据
parsed_data = data.map(parse_line).filter(lambda x: x is not None)
# 统计每个用户在2014年8月的不同日期数量
user_activity = parsed_data.map(lambda x: (x[0], x[1])).distinct().groupByKey().mapValues(len)
# 筛选出活跃用户
active_users = user_activity.filter(lambda x: x[1] >= 5)
# 获取活跃用户的总数
active_user_count = active_users.count()
print(f"{active_user_count}")
# 保存结果
output_path = "/home/njucs/Desktop/test4/work1/output2/"
sc.parallelize([str(active_user_count)]).coalesce(1).saveAsTextFile(output_path)

sc.stop()
