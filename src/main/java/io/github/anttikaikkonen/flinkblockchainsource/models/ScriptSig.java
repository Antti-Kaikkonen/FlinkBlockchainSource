package io.github.anttikaikkonen.flinkblockchainsource.models;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ScriptSig {
    String asm;
    String hex;
}
