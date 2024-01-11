#
#Copyright (c) 2020 Cloudera, Inc. All rights reserved.
#

from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col
import sys

### ユーザー名の設定
username = "changeme" ## 講師の指示に従って更新してください

###  DB名・アプリ名をユーザー名に応じて設定
db_name = username + "_TexasPPP"
appName = username + "-Job2"

spark = SparkSession \
    .builder \
    .appName(appName) \
    .getOrCreate()

# インプットパスとして S3 のパスを設定
input_path ="s3a://bliu-sand-aws-ohio-202303/cde-workshop/PPP-Sub-150k-TX.csv"

# S3バケット から Spark にデータを取り込む
base_df=spark.read.option("header","true").option("inferSchema","true").csv(input_path)

# スキーマを表示する
print("...............................")
print(f"printing schema")
base_df.printSchema()

# 必要なカラムだけを取り出す
filtered_df = base_df.select("LoanAmount", "City", "State", "Zip", "BusinessType", "NonProfit", "JobsRetained", "DateApproved", "Lender")

# テキサスのデータのみを表示（参考用）
print("...............................")
print(f"How many TX records did we get?")
tx_cnt = filtered_df.count()
print(f"We got: %i " % tx_cnt)

# データベースが存在していない場合は作成
print("...............................")
print(f"Creating {db_name} Database \n")
spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")
spark.sql(f"SHOW databases like '{username}*' ").show()

print("...............................")
print(f"Inserting Data into {db_name}.loan_data table \n")

# データをテーブルに追加
filtered_df.\
  write.\
  mode("append").\
  saveAsTable(db_name+'.'+"loan_data", format="parquet")

# データ追加の結果（件数）を確認
print("...............................")
print(f"Number of records \n")
spark.sql(f"Select count(*) as RecordCount from {db_name}.loan_data").show()

print("...............................")
print(f"Retrieve 15 records for validation \n")
spark.sql(f"Select * from {db_name}.loan_data limit 15").show()
