package com.tomshidi.wc;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author TomShiDi
 * @description
 * @date 2021/2/10 16:26
 **/
public class StreamWordCount {
    public static void main(String[] args) throws Exception {
        // 创建流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置并行处理数，默认cpu核心数
//        env.setParallelism(8);

//        // 从文件中读取数据
//        String inputPath = "E:\\idea_project\\flink-task\\src\\main\\resources\\hello.txt";
//
//        DataStream<String> dataStream = env.readTextFile(inputPath);

        // 用parameter tool 工具同程序启动参数中提取配置
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String host = parameterTool.get("host");
        int port = parameterTool.getInt("port");

        // 从socket文本流读取数据
        DataStream<String> dataStream = env.socketTextStream(host, port);

        // 基于数据流进行转换计算
        DataStream<Tuple2<String, Integer>> resultStream = dataStream.flatMap(new WordCount.MyFlatMapper())
                .keyBy(0)
                .sum(1).setParallelism(2);

        resultStream.print().setParallelism(1);

        // 执行任务
        env.execute();
    }
}
