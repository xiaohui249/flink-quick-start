package com.sean.flink;

import com.sean.flink.source.MyParalleSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * Create by sean
 * Date: 19-10-17
 * Time: 下午2:52
 */
public class StreamingDemoWithMyPralalleSource {

    public static void main(String[] args) throws Exception {

        //获取Flink的运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Long> source = env.addSource(new MyParalleSource()).setParallelism(2);

        DataStream<Long> num = source.map(new MapFunction<Long, Long>() {
            @Override
            public Long map(Long aLong) throws Exception {
                System.out.println("接收到数据：" + aLong);
                return aLong;
            }
        });

        // 每２秒钟处理一次数据
        DataStream<Long> sum = num.timeWindowAll(Time.seconds(2)).sum(0);

        // 打印结果
        sum.print().setParallelism(1);

        // 获取类名
        String jobName = StreamingDemoWithMyPralalleSource.class.getSimpleName();
        env.execute(jobName);
    }

}
