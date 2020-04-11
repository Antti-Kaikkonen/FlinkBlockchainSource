package io.github.anttikaikkonen.flinkblockchainsource.rpcclient;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.anttikaikkonen.flinkblockchainsource.models.Block;
import java.io.IOException;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import lombok.Builder;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.util.EntityUtils;

public class RpcClient implements Serializable {
    
    private transient CloseableHttpClient httpClient;
    
    private final String rpcUrl;
    private final String rpcUsername;
    private final String rpcPassword;
    private final int rpcThreads;
    private ObjectMapper objectMapper;
    
    @Builder
    public RpcClient(String rpc_url, String rpc_username, String rpc_password, Integer rpcThreads) {
        System.out.println("NEW RPCCLIENT "+rpc_url+"\t"+rpc_username+"\t"+rpc_password);
        this.rpcUrl = rpc_url;
        this.rpcUsername = rpc_username;
        this.rpcPassword = rpc_password;
        this.rpcThreads = rpcThreads == null ? 1 : rpcThreads;
        this.objectMapper = new ObjectMapper();
        this.objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }
    
    private synchronized  void intializeClient() {
        if (this.httpClient != null) return;
        CredentialsProvider provider = new BasicCredentialsProvider();
        provider.setCredentials(
                AuthScope.ANY,
                new UsernamePasswordCredentials(rpcUsername, rpcPassword)
        );
        PoolingHttpClientConnectionManager connectionManager = new PoolingHttpClientConnectionManager();
        connectionManager.setMaxTotal(rpcThreads);
        connectionManager.setDefaultMaxPerRoute(rpcThreads);
        this.httpClient = HttpClientBuilder.create().setDefaultCredentialsProvider(provider).setConnectionManager(connectionManager)
                .build();
    }

    public int getRpcThreads() {
        return rpcThreads;
    }
    
    public long getBlockCount() throws IOException {
        intializeClient();
        HttpPost post = new HttpPost(rpcUrl);
        ObjectNode body = objectMapper.createObjectNode().put("method", "getblockcount");
        post.setEntity(new StringEntity(objectMapper.writeValueAsString(body)));
        
        try (CloseableHttpResponse response = httpClient.execute(post)) {
            return objectMapper.readTree(EntityUtils.toString(response.getEntity())).get("result").asLong();
        } 
    }
    
    public Block getBlockRpc(String hash) throws IOException {
        intializeClient();
        HttpPost post = new HttpPost(rpcUrl);
        int verbosity = 2;
        ObjectNode body = objectMapper.createObjectNode().put("method", "getblock");
        body.putArray("params").add(hash).add(verbosity);

        post.setEntity(new StringEntity(objectMapper.writeValueAsString(body)));
        
        try (CloseableHttpResponse response = httpClient.execute(post)) {
            JsonNode result = objectMapper.readTree(EntityUtils.toString(response.getEntity())).get("result");
            return objectMapper.treeToValue(result, Block.class);
        }
    }
   
    public String getBlockHash(long height) throws UnsupportedEncodingException, IOException {
        intializeClient();
        HttpPost post = new HttpPost(rpcUrl);
        ObjectNode body = objectMapper.createObjectNode().put("method", "getblockhash");
        body.putArray("params").add(height);

        post.setEntity(new StringEntity(objectMapper.writeValueAsString(body)));
        
        try (CloseableHttpResponse response = httpClient.execute(post)) {
            return objectMapper.readTree(EntityUtils.toString(response.getEntity())).get("result").asText();
        }
    }
}
