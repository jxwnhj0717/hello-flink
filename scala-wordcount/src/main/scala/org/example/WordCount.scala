package org.example

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment, createTypeInformation}

object WordCount {
  def main(args : Array[String]) : Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds : DataSet[String] = env.readTextFile("/Users/huangjin/IdeaProjects/flink/scala-wordcount/input.txt")
    val ds2 : DataSet[String] = ds.flatMap(_.split(" "))
    ds2.map((_, 1))
      .groupBy(0)
      .sum(1)
      .print()
  }
}
