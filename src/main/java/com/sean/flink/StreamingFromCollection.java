package com.sean.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

/**
 * Create by sean
 * Date: 19-10-16
 * Time: 下午5:41
 */
public class StreamingFromCollection {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 定义集合数据
        List<Integer> list = new ArrayList<>(3);
        list.add(10);
        list.add(15);
        list.add(20);

        // 创建数据源
        DataStreamSource<Integer> collectionData = env.fromCollection(list);

        // 通过map对数据源进行处理
        DataStream<Integer> num = collectionData.map(new MapFunction<Integer, Integer>() {
            @Override
            public Integer map(Integer value) throws Exception {
                return value + 1;
            }
        });

        // 输出处理之后的数据：直接打印到控制台
        num.print().setParallelism(1);

        env.execute("StreamingFromCollection");
    }
}
