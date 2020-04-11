package io.github.anttikaikkonen.flinkblockchainsource;

import io.github.anttikaikkonen.flinkblockchainsource.models.Block;
import io.github.anttikaikkonen.flinkblockchainsource.rpcclient.RpcClient;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

public class BlockFetcherThread extends Thread {
    
    private final BlockchainSource source;
    private long height;
    private long blockCount;
    private final ExecutorService executor;
    private final RpcClient[] rpcClients;
    private final int minConfirmations;
    private final long pollingInterval;
    private boolean temporarilyIdle = false;
    
    private final LinkedBlockingQueue<RpcClient> fifo;
    
    
    public BlockFetcherThread(BlockchainSource source, RpcClient[] rpcClients, long startHeight, int minConfirmations, long pollingInterval) {
        this.height = startHeight;
        this.source = source;
        this.rpcClients = rpcClients;
        this.minConfirmations = minConfirmations;
        this.pollingInterval = pollingInterval;
        fifo = new LinkedBlockingQueue<>();
        int totalRpcThreads = 0;
        for (int i = 0; i < rpcClients.length; i++) {
            int clientRpcThreads = rpcClients[i].getRpcThreads();
            totalRpcThreads += clientRpcThreads;
            for (int ii = 0; ii < clientRpcThreads; ii++) {
                fifo.add(rpcClients[i]);
            }
        }
        this.executor = Executors.newFixedThreadPool(totalRpcThreads);
    }
    
    @Override
    public void run() {
        System.out.println("STARTING BLOCKFETCHER THREAD");
        while(!isInterrupted()) {
            try {
                try {
                    this.blockCount = this.rpcClients[0].getBlockCount();
                } catch (Exception ex) {
                    System.out.println("Failed to get block count"+ex);
                    Thread.sleep(10000);
                    continue;
                }
                long targetHeight = this.blockCount-this.minConfirmations;
                if (this.height <= targetHeight) {
                    this.temporarilyIdle = false;
                    System.out.println("Current height: "+this.height+", Target height: "+targetHeight);
                    while (!isInterrupted() && this.height <= targetHeight) {
                        final long h = this.height;
                        RpcClient rpcClient = this.fifo.take();
                        CompletableFuture<Block> completableFuture = CompletableFuture.supplyAsync(() -> {
                            try {
                                String hash = rpcClient.getBlockHash(h);
                                Block block = rpcClient.getBlockRpc(hash);
                                this.fifo.add(rpcClient);
                                return block;
                            } catch (IOException ex) {
                                throw new CompletionException(ex);
                            }
                        }, this.executor);
                        this.height++;
                        this.source.getWorkQueue().put(completableFuture);
                        
                    } 
                } else {
                   this.temporarilyIdle = true;
                    Thread.sleep(this.pollingInterval);
                }
            } catch (InterruptedException ex) {
                break;
            }
        }
        //this.source.getContext().getLogger().error("BlockFetcherThread interrupted");
    }

    public boolean isTemporarilyIdle() {
        return temporarilyIdle;
    }
    
}
