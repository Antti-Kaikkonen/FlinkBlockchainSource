# FlinkBlockchainSource
A blockchain source connector for Apache Flink

## Prerequisites
* Java 8 or later
* Apache Flink 1.10.0 or later
* Apache Maven 3.3 or later
* Bitcoin Core or an RPC compatible client such as Litecoin Core or Dash Core.

## Installation
1) `git clone https://github.com/Antti-Kaikkonen/FlinkBlockchainSource.git`
2) `cd FlinkBlockchainSource && mvn install`
3) include it in your project:
```xml
<dependency>
    <groupId>io.github.antti-kaikkonen</groupId>
    <artifactId>FlinkBlockchainSource</artifactId>
    <version>1.0-SNAPSHOT</version>
</dependency>
```

## Example usage
```java
final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
BlockchainSource source = BlockchainSource
    .builder()
    .minConfirmations(5)
    .rpcClients(new RpcClient[]{
            RpcClient.builder()
            .rpc_url("http://localhost:8332")
            .rpc_username("rpcuser from bitcoin.conf")
            .rpc_password("rpcpassword from bitcoin.conf")
            .rpcThreads(4)
            .build();
        })
    .build();
DataStreamSource<Block> blocks = env.addSource(source);
blocks
	.filter(block -> block.getTx().length == 1)
	.map(e -> e.getHash()+"\t"+e.getHeight())
	.print();
env.execute("Empty block printer");
```
Fetching blocks from bitcoin core is usually the bottleneck so you can pass multiple RpcClients to distribute the RPC requests over them.
