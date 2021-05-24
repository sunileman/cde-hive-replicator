package com.cloudera.examples

import org.apache.spark.sql.{SaveMode, SparkSession}

object HiveReplicate {

  def main(args: Array[String]): Unit = {


    val spark = SparkSession.builder
      .appName("Hive Table Replication")
      .getOrCreate()


    spark.sql("select count(*) from default.airlines").show()

    val df = spark.sql("select * from default.airlines")


    //this is a different aws account
    df.write.mode("overwrite").parquet("s3a://sunileman1/replicationtest/")

  }

}
