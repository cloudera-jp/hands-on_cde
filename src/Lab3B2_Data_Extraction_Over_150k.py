#
#Copyright (c) 2020 Cloudera, Inc. All rights reserved.
#

from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col, regexp_replace
import sys

### ユーザー名の設定
username = "changeme" ## 講師の指示に従って更新してください

###  DB名・アプリ名をユーザー名に応じて設定
db_name = username + "_TexasPPP"
appName = username + "-Job3"

spark = SparkSession \
    .builder \
    .appName(appName) \
    .getOrCreate()

# インプットパスとして S3 のパスを設定
input_path ="s3a://bliu-sand-aws-ohio-202303/cde-workshop/PPP-Over-150k-ALL.csv"

# S3バケット から Spark にデータを取り込む
base_df=spark.read.option("header","true").option("inferSchema","true").csv(input_path)

# スキーマを表示（確認用）
print("...............................")
print(f"printing schema")
base_df.printSchema()

# 15万以内のローンをデータの中から、テキサス州のデータだけを抽出
texas_df = base_df.filter(base_df.State == 'TX')

# 抽出したテキサスのレコードを表示（確認用）
print("...............................")
print(f"How many TX records did we get?")
tx_cnt = texas_df.count()
print(f"We got: %i " % tx_cnt)

# カラム名をリネーム（LoanRange -> LoanAmount）して表示
filtered_df = texas_df.select(col("LoanRange").alias("LoanAmount"), "City", "State", "Zip", "BusinessType", "NonProfit", "JobsRetained", "DateApproved", "Lender")
filtered_df.show()

# 正規表現を利用し、文字列の情報を数値に変換し平均金額を算出
value_df=filtered_df.withColumn("LoanAmount",regexp_replace(col("LoanAmount"), "[a-z] \$5-10 million", "7500000").cast("double"))
value_df=value_df.withColumn('LoanAmount',regexp_replace(col("LoanAmount"), "[a-z] \$1-2 million", "1500000").cast("double"))
value_df=value_df.withColumn('LoanAmount',regexp_replace(col("LoanAmount"), "[a-z] \$5-10 million", "7500000").cast("double"))
value_df=value_df.withColumn('LoanAmount',regexp_replace(col("LoanAmount"), "[a-z] \$2-5 million", "3500000").cast("double"))
value_df=value_df.withColumn('LoanAmount',regexp_replace(col("LoanAmount"), "[a-z] \$350,000-1 million", "675000").cast("double"))

# 正しく値が算出できたことを確認
testdf = value_df.filter(value_df.LoanAmount != "7500000")
print("...............................")
print("Simple test to see if our data looks correct")
testdf.show()
# 最終的な集計結果を確認
print("...............................")
print("Show the final results for one more sanity check")
value_df.show()

# DBが存在しない場合は作成
print("...............................")
print(f"Creating {db_name} Database if it doesn't exist\n")
spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")
spark.sql(f"SHOW databases like '{username}*' ").show()

print("...............................")
print(f"Inserting Data into {db_name}.loan_data table \n")

# データをテーブルに上書き
value_df.\
  write.\
  mode("overwrite").\
  saveAsTable(db_name+'.'+"loan_data", format="parquet")

# データ件数の表示（確認用）
print("...............................")
print(f"Number of records \n")
spark.sql(f"Select count(*) as RecordCount from {db_name}.loan_data").show()

print("...............................")
print(f"Retrieve 15 records for validation \n")
spark.sql(f"Select * from {db_name}.loan_data limit 15").show()
