//
//Copyright (c) 2020 Cloudera, Inc. All rights reserved.
//

import org.apache.spark.sql.SparkSession

//---------------------------------------------------
//              SPARK セッションを作成
//---------------------------------------------------
val user = "changeme" // 講師の指示に従って更新してください
val appName = user + "-AvgLoan-Texas"
val spark = SparkSession.builder.appName(appName).getOrCreate()

// インプットパスとして S3 のパスを設定
val input_path ="s3a://bliu-sand-aws-ohio-202303/cde-workshop/PPP-Sub-150k-TX.csv"

val base_df=spark.read.option("header","true").option("inferSchema","true").csv(input_path)

//------------------------------------------------------------------------------------------------------
//               関連するカラムを取得
//               -> CITY、LENDERごとにローン金額の平均を算出
//------------------------------------------------------------------------------------------------------

val filtered_df = base_df.select("LoanAmount", "City", "State", "Zip", "BusinessType", "NonProfit", "JobsRetained", "DateApproved", "Lender")

val final_df = filtered_df.groupBy("Lender", "City").agg(avg($"LoanAmount").as("Average_Loan_Amount"))

print("Average loan amount by a lender in Texas:- ")
print("........................")

final_df.show()
