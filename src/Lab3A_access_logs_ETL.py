#
#Copyright (c) 2020 Cloudera, Inc. All rights reserved.
#

from pyspark.sql import SparkSession
from pyspark.sql.functions import split, regexp_extract, regexp_replace, col
import sys

### ユーザー名を設定
username = "changeme" ## 講師の指示に従って更新してください

### DB名・アプリ名をユーザー名に応じて設定
db_name = username + "_retail"
appName = username + "-Job1"

spark = SparkSession \
    .builder \
    .appName(appName) \
    .getOrCreate()

# インプットパスとして S3 のパスを設定
input_path ="s3a://bliu-sand-aws-ohio-202303/cde-workshop/access-log.txt"

# データをまず全量取り込む
base_df=spark.read.text(input_path)

# 必要な部分をスクレイピング
split_df = base_df.select(regexp_extract('value', r'([^ ]*)', 1).alias('ip'),
                          regexp_extract('value', r'(\d\d\/\w{3}\/\d{4}:\d{2}:\d{2}:\d{2} -\d{4})', 1).alias('date'),
                          regexp_extract('value', r'^(?:[^ ]*\ ){6}([^ ]*)', 1).alias('url'),
                          regexp_extract('value', r'(?<=product\/).*?(?=\s|\/)', 0).alias('productstring')
                         )

# product部分の文字列が空行のレコードを除去
filtered_products_df = split_df.filter("productstring != ''")

# カラムの順番を調整
cleansed_products_df=filtered_products_df.select(regexp_replace("productstring", "%20", " ").alias('product'), "ip", "date", "url")

# 試しに1レコードを出力してみる
print("...............................")
print("Cleansed product sample:")
print(cleansed_products_df.take(1))

# DB存在しない時にDBを作成する
print("...............................")
print(f"Creating {db_name} Database \n")
spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")
print("...............................")
print(f"Inserting Data into {db_name}.tokenized_accesss_logs table \n")

# テーブルに書き込む
cleansed_products_df.\
  write.\
  mode("overwrite").\
  saveAsTable(db_name+'.'+"tokenized_access_logs", format="parquet")

print(f"Count number of records inserted \n")
spark.sql(f"Select count(*) as RecordCount from {db_name}.tokenized_access_logs").show()

print(f"Retrieve 15 records for validation \n")
spark.sql(f"Select * from {db_name}.tokenized_access_logs limit 15").show()
