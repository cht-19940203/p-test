package flink;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

public class TestCheckpoint {

    public static void main(String[] args) throws Exception {

        CountDownLatch latch = new CountDownLatch(1);
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.createLocalEnvironment();
        environment.setParallelism(1);
        environment.enableCheckpointing(10000);
        CheckpointConfig checkpointConfig = environment.getCheckpointConfig();
//        checkpointConfig.setCheckpointStorage("file:///D:/checkpoint-dir");
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        StateCKSource source = new StateCKSource();

        environment.addSource(source)
                .uid("state-source")
                .print()
                .uid("print-sink");
        environment.execute("job-20220424-1");

        latch.await();
    }


    private static class StateCKSource extends RichParallelSourceFunction<Integer>
            implements CheckpointedFunction {

        private ValueState<Integer> ck;

        private ListState<Integer> ck_1;

        private volatile boolean open = true;
        private AtomicInteger count = new AtomicInteger(0);


        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
        }


        @Override
        public void run(SourceContext<Integer> ctx) throws Exception {
            while (open){
                Thread.sleep(3000);
                count.incrementAndGet();
                ctx.collect(count.get());
            }
        }

        @Override
        public void cancel() {
            this.open = false;
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            ck_1.clear();
            ck_1.add(count.get());
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {

            this.ck_1 =
                    context.getOperatorStateStore().getListState(new ListStateDescriptor<Integer>("int-state-1",Integer.class));

            if(context.isRestored()){
                for (int i : ck_1.get()){
                    count.getAndSet(i);
                }

            }else {
                count.getAndSet(0);
            }
        }
    }


}
