package com.sean.flink.source;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * Create by sean
 * Date: 19-10-17
 * Time: 下午2:49
 */
public class MyNoParalleSource implements SourceFunction<Long> {

    private long count = 1L;
    private boolean isRunning = true;

    @Override
    public void run(SourceContext<Long> ctx) throws Exception {
        while(isRunning) {
            ctx.collect(count);
            count++;
            // 每秒产生一条数据
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
