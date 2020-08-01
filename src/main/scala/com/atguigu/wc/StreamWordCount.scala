package com.atguigu.wc

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._

/*批处理WordCount代码*/
object StreamWordCount {
  def main(args: Array[String]): Unit = {
    /*配置参数*/
    val params: ParameterTool = ParameterTool.fromArgs(args)
    val host: String = params.get("host")
    val port: Int = params.getInt("port")
    /*创建一个流处理的执行环境*/
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //接受socket数据流
    val textDataStream: DataStream[String] = env.socketTextStream(host,port)

    /*逐一读取数据，分词之后进行wordcount*/
    val wordCountDataStream: DataStream[(String, Int)] = textDataStream.flatMap(_.split(" "))
      .filter(_.nonEmpty)
      .map((_, 1))
      .keyBy(0)
      .sum(1)
    /*打印输出*/
    wordCountDataStream.print().setParallelism(1)
    wordCountDataStream.writeAsText("D:\\flink")
    /*执行任务*/
    env.execute("stream word count job")
  }
}
