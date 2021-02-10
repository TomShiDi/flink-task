package com.tomshidi.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * 批处理word count
 *
 * @author TomShiDi
 * @date 2021/2/10 15:48
 **/
public class WordCount {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
        // 从文件中读取数据
        String inputPath = "E:\\idea_project\\flink-task\\src\\main\\resources\\hello.txt";
        DataSet<String> inputDataSet = environment.readTextFile(inputPath);

        // 处理数据集，按空格分词展开，转换成(word,1)二元组进行统计
        DataSet<Tuple2<String, Integer>> resultDataSet = inputDataSet.flatMap(new MyFlatMapper())
                .groupBy(0) //按照二元组中第一个位置的word分组
                .sum(1); // 将二元组(word,1)中第二个位置上的数据求和

        resultDataSet.print();
    }

    // 自定义类，实现FlatMapFunction接口
    public static class MyFlatMapper implements FlatMapFunction<String, Tuple2<String, Integer>> {
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            // 按空格分词
            String[] words = value.split(" ");
            // 遍历所有word，封装成二元组
            for (String word : words) {
                out.collect(new Tuple2<String, Integer>(word, 1));
            }
        }
    }

}
