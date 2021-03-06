package io.github.anttikaikkonen.flinkblockchainsource.models;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Transaction {
    String txid;
    int size;
    long version;
    //long type;
    long locktime;
    TransactionInput[] vin;
    TransactionOutput[] vout;
}