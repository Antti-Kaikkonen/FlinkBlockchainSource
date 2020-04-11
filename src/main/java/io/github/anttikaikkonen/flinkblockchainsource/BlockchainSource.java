package io.github.anttikaikkonen.flinkblockchainsource;

import io.github.anttikaikkonen.flinkblockchainsource.models.Block;
import io.github.anttikaikkonen.flinkblockchainsource.rpcclient.RpcClient;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import lombok.Builder;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

public class BlockchainSource extends RichSourceFunction<Block> implements CheckpointedFunction {

    private volatile boolean isRunning = true;
    
    private BlockingQueue<CompletableFuture<Block>> workQueue;
    private BlockFetcherThread blockFetcherThread;
    private transient ListState<Long> checkpointedHeight;
    private long height = 0l;
    private transient ListState<Long> checkpointedTime;
    private long time = 0l;
    private RpcClient[] rpcClients;
    
    private final int minConfirmations;
    private final long pollingInterval;
    
    @Builder()
    private BlockchainSource(Integer minConfirmations, Long pollingInterval, RpcClient[] rpcClients) {
        this.minConfirmations = minConfirmations == null ? 5 : minConfirmations;
        this.pollingInterval = pollingInterval == null ? 1000l : pollingInterval;
        this.rpcClients = rpcClients;
    }
    
    
    @Override
    public void run(SourceContext<Block> ctx) throws Exception {
        
        System.out.println("starting run");
        
        while (isRunning) {
            CompletableFuture<Block> cf = this.workQueue.poll();
            if (cf == null && blockFetcherThread.isTemporarilyIdle()) {//Empty workQueue and processed blocks to target height
                ctx.markAsTemporarilyIdle();
                cf = this.workQueue.take();
            }
            Block block = cf.get();
            // this synchronized block ensures that state checkpointing,
            // internal state updates and emission of elements are an atomic operation
            synchronized (ctx.getCheckpointLock()) {
                if (block.getTime()*1000 > time) {
                    time = block.getTime()*1000;
                } else {
                    //System.out.println("Block "+block.getHeight()+" timestamp is not after the previous block. Using previous block timestamp to get monotonically increasing time.");
                    time = time+1;
                }
                ctx.collectWithTimestamp(block, time);
                ctx.emitWatermark(new Watermark(time));
                this.height++;
            }
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
    
    @Override
    public void open(Configuration parameters) throws Exception {
        getRuntimeContext().getMetricGroup().gauge("Current height", new Gauge<Long>() {
            @Override
            public Long getValue() {
                return height;
            }
        });
        //this.o
        this.workQueue = new LinkedBlockingQueue<>(32);
        this.blockFetcherThread = new BlockFetcherThread(this, this.rpcClients, this.height, this.minConfirmations, this.pollingInterval);
        this.blockFetcherThread.start();
        RuntimeContext runtimeContext = getRuntimeContext();
        System.out.println("RichFunction OPEN"+ runtimeContext.getTaskName());
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        System.out.println("Snapshotting state "+context.getCheckpointId());
        this.checkpointedHeight.clear();
        this.checkpointedHeight.add(height);
        this.checkpointedTime.clear();
        this.checkpointedTime.add(time);
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        System.out.println("Initialize state");
        this.checkpointedHeight = context.getOperatorStateStore().getListState(new ListStateDescriptor<>("height", Long.class));
        this.checkpointedTime = context.getOperatorStateStore().getListState(new ListStateDescriptor<>("time", Long.class));
        if (context.isRestored()) {
            for (Long height : this.checkpointedHeight.get()) {
                this.height = height;
            }
            for (Long time : this.checkpointedTime.get()) {
                this.time = time;
            }
        }
    }

    public BlockingQueue<CompletableFuture<Block>> getWorkQueue() {
        return workQueue;
    }
    
}
