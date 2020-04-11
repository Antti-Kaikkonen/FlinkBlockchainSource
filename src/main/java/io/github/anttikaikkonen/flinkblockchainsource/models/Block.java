package io.github.anttikaikkonen.flinkblockchainsource.models;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Block {
    
    String hash;
    int size;
    int height;
    long version;
    String merkleroot;
    Transaction[] tx;
    long time;
    long mediantime;
    long nonce;
    String bits;
    double difficulty;
    String chainwork;
    String previousblockhash;
    
}