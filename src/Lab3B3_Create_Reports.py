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
appName = username + "-Job4"

spark = SparkSession \
    .builder \
    .appName(appName) \
    .getOrCreate()

print("...............................")
print(f"Running report for Jobs Retained by City")

# 既に作成されたレポート（テーブル）を削除する
spark.sql(f"drop table IF EXISTS {db_name}.Jobs_Per_City_Report")
spark.sql(f"drop table IF EXISTS {db_name}.Jobs_Per_Company_Type_Report")

# 街ごとの職業のレポートを作成
cityReport = f"create table {db_name}.Jobs_Per_City_Report as \
select * from (Select \
  sum(jobsretained) as jobsretained, \
  city \
from \
  {db_name}.loan_data \
group by \
  city \
) A order by A.jobsretained desc"

# 抽出結果を新規のテーブルに書き込む
print("...............................")
print(f"Running - Jobs Per City Report \n")
spark.sql(cityReport)

# 上位10件の結果を表示
print("...............................")
print(f"Results - Jobs Per City Report \n")
cityReportResults = f"select * from {db_name}.Jobs_Per_City_Report limit 10"
spark.sql(cityReportResults).show()

# 会社の種類ごとの職業名のレポートを作成
companyTypeReport = f"create table {db_name}.Jobs_Per_Company_Type_Report as \
select * from (Select \
  sum(jobsretained) as jobsretained, \
  businesstype \
from \
  {db_name}.loan_data \
group by \
  businesstype \
) A order by A.jobsretained desc"

# 結果を新しいテーブルに書き込み
print("...............................")
print(f"Running - Jobs Per Company Type Report \n")
spark.sql(companyTypeReport)

# 上位10件の結果を表示
print("...............................")
print(f"Results - Jobs Per Company Type Report \n")
cityReportResults = f"select * from {db_name}.Jobs_Per_Company_Type_Report limit 10"
spark.sql(cityReportResults).show()
