package com.example;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.File;
import java.net.URL;
import java.util.Arrays;

/**
 * checkpoint
 */
public class Demo02 {
    public static void main(String[] args) throws Exception {
        String baseDir = "/Users/huangjin/IdeaProjects/flink/state";
        Configuration config = new Configuration();

        // 使用最新的checkpoint恢复
        File lastestCheckpoint = getLastestCheckpoint(baseDir);
        if(lastestCheckpoint != null) {
            System.out.println("checkpoint:" + lastestCheckpoint.getAbsolutePath());
            SavepointRestoreSettings srs = SavepointRestoreSettings.forPath(lastestCheckpoint.getAbsolutePath() + "/_metadata", true);
            SavepointRestoreSettings.toConfiguration(srs, config);
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        env.setParallelism(1);
        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(50000L);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().enableUnalignedCheckpoints();
        // 任务停止后保留checkpoints
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setStateBackend(new FsStateBackend("file:///Users/huangjin/IdeaProjects/flink/state/checkpoints"));

        DataStreamSource<String> source = env.socketTextStream("localhost", 7777);
        DataStreamSink<Tuple2<String, Long>> sink = source.map(
                        new MapFunction<String, Tuple2<String, Long>>() {
                            @Override
                            public Tuple2<String, Long> map(String value) throws Exception {
                                String[] split = value.split(",");
                                Tuple2 tp = Tuple2.of(split[0], Long.parseLong(split[1]));
                                System.out.println("input:" + tp);
                                return tp;
                            }
                        }).keyBy(e -> e.f0)
                .map(new MyMapFunc())
                .print("out:");

        System.out.println(env.getExecutionPlan());
        env.execute();

    }

    private static File getLastestCheckpoint(String baseDir) {
        File cpDir = new File(baseDir, "checkpoints");
        File lastestCpFile = null;
        if(cpDir.exists() ) {
            File lastestCpDir = null;
            for (File file : cpDir.listFiles()) {
                if(file.isDirectory()) {
                    if(lastestCpDir == null || lastestCpDir.lastModified() < file.lastModified()) {
                        lastestCpDir = file;
                    }
                }
            }
            if(lastestCpDir != null) {
                for (File file : lastestCpDir.listFiles(n -> n.getName().startsWith("chk-"))) {
                    if(file.isDirectory()) {
                        if(lastestCpFile == null || lastestCpFile.lastModified() < file.lastModified()) {
                            lastestCpFile = file;
                        }
                    }
                }
            }
        }
        return lastestCpFile;
    }

    private static class MyMapFunc extends RichMapFunction<Tuple2<String, Long>, Tuple2<String, Long>> {
        private ValueState<Long> state;
        private long sum = 0;
        @Override
        public Tuple2<String, Long> map(Tuple2<String, Long> tp) throws Exception {
            if(state.value() != null) {
                sum = state.value() + tp.f1;
            } else {
                sum = tp.f1;
            }
            state.update(sum);
            return Tuple2.of(tp.f0, sum);
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            ValueStateDescriptor<Long> stateDesc = new ValueStateDescriptor<>("mySumState",
                    TypeInformation.of(new TypeHint<Long>() {
                    }));
            state = getRuntimeContext().getState(stateDesc);
        }
    }
}
