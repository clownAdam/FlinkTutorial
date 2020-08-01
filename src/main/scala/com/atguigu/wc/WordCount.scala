package com.atguigu.wc

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._

/*批处理WordCount代码*/
object WordCount {
  def main(args: Array[String]): Unit = {
    /*创建一个执行批处理的执行环境*/
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    /*从文件中读取数据*/
    val params: ParameterTool = ParameterTool.fromArgs(args)
    val inputPath: String = params.get("inputPath")
    val outputPath: String = params.get("outputPath")

    val inputDataSet: DataSet[String] = env.readTextFile(inputPath)

    /*分词之后做count*/
    val wordCountDataSet: AggregateDataSet[(String, Int)] = inputDataSet.flatMap(_.split(" "))
      .map((_, 1))
      .groupBy(0)
      .sum(1)

    /*打印输出*/
    wordCountDataSet.writeAsText(outputPath)
    wordCountDataSet.print()
  }
}
