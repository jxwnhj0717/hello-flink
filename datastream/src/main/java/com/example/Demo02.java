package com.example;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.JoinedStreams;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;

/**
 * 2个数据流join
 * 输出：
 * out:> (S1,1,10)
 * out:> (S1,2,10)
 * out:> (S1,1,10)
 * out:> (S1,1,11)
 * out:> (S1,2,10)
 * out:> (S1,2,11)
 * out:> (S2,2,12)
 * out:> (S2,5,12)
 * out:> (S3,6,13)
 * 分析：
 * 1. source1 window: s1,1; s1,2
 * 2. source2 window: s1,10，join得到2条结果
 * 3. source2 window: s1,10; s1,11，join得到4条结果，故s1有6条输出
 */
public class Demo02 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Tuple2<String, Integer>> source1 = env.fromElements(
                Tuple2.of("S1", 1),
                Tuple2.of("S1", 2),
                Tuple2.of("S2", 2),
                Tuple2.of("S2", 5),
                Tuple2.of("S3", 6)
        );
        DataStreamSource<Tuple2<String, Integer>> source2 = env.fromElements(
                Tuple2.of("S1", 10),
                Tuple2.of("S1", 11),
                Tuple2.of("S2", 12),
                Tuple2.of("S3", 13)
                );
        source1.join(source2)
                .where(tp -> tp.f0)
                .equalTo(tp -> tp.f0)
                .window(GlobalWindows.create())
                .trigger(CountTrigger.of(1))
                .apply(new MyJoinFunction())
                .print("out:");

        System.out.println(env.getExecutionPlan());
        env.execute();
    }

    private static class MyJoinFunction implements JoinFunction
            <Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple3<String, Integer, Integer>> {

        @Override
        public Tuple3<String, Integer, Integer> join(Tuple2<String, Integer> first, Tuple2<String, Integer> second) throws Exception {
            return Tuple3.of(first.f0, first.f1, second.f1);
        }
    }
}
