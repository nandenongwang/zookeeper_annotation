package org.apache.zookeeper.server;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.jute.Record;
import org.apache.zookeeper.txn.TxnDigest;
import org.apache.zookeeper.txn.TxnHeader;

/**
 * A helper class to represent the txn entry.
 */
@AllArgsConstructor
@Data
public final class TxnLogEntry {

    /**
     *
     */
    private final Record txn;

    /**
     *
     */
    private final TxnHeader header;

    /**
     *
     */
    private final TxnDigest digest;
}
