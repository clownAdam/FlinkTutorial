package com.atguigu.wc

import org.apache.flink.api.scala._

object Demo {
  def main(args: Array[String]): Unit = {
    //1.获取ExecutionEnvironment
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    //2.加载数据
    //    import org.apache.flink.api.scala._
    val lines: DataSet[String] = env.fromElements("Apache Flink is an open source platform for distributed stream and batch data processing","Flink’s core is a streaming dataflow engine that provides data distribution")
    //3.处理数据 [\w]+和\w+没有区别，都是匹配数字和字母下划线的多个字符
    val words: DataSet[String] = lines.flatMap(_.split("\\W+"))//" "
    val wordAndOne: DataSet[(String, Int)] = words.map((_,1))
    //val value: GroupedDataSet[(String, Int)] = wordAndOne.groupBy(0)
    //val result: AggregateDataSet[(String, Int)] = wordAndOne.groupBy(_._1).sum(1)
    //Aggregate does not support grouping with KeySelector functions, yet.//聚合尚不支持使用键选择器函数进行分组
    //val result: AggregateDataSet[(String, Int)] = wordAndOne.groupBy("_1").sum(1)
    val result: AggregateDataSet[(String, Int)] = wordAndOne.groupBy(0).sum(1)
    //4.输出结果
//    result.writeAsText("hdfs://bd-101:9000/flink")

    result.print()
//    Thread.sleep(Long.MaxValue)
//    Thread.sleep(Long.MaxValue)
    //5.触发执行
    //env.execute()
    // No new data sinks have been defined since the last execution.
    //The last execution refers to the latest call to 'execute()', 'count()', 'collect()', or 'print()'.
  }
}
