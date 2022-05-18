package org.apache.zookeeper.server.persistence;

import org.apache.jute.Record;
import org.apache.zookeeper.server.ServerStats;
import org.apache.zookeeper.txn.TxnDigest;
import org.apache.zookeeper.txn.TxnHeader;

import java.io.Closeable;
import java.io.IOException;

/**
 * Interface for reading transaction logs.
 */
public interface TxnLog extends Closeable {

    /**
     * Setter for ServerStats to monitor fsync threshold exceed
     *
     * @param serverStats used to update fsyncThresholdExceedCount
     */
    void setServerStats(ServerStats serverStats);

    /**
     * 滚动下一个日志
     * roll the current
     * log being appended to
     *
     * @throws IOException
     */
    void rollLog() throws IOException;

    /**
     * 添加事务日志
     * Append a request to the transaction log
     *
     * @param hdr the transaction header
     * @param r   the transaction itself
     * @return true iff something appended, otw false
     * @throws IOException
     */
    boolean append(TxnHeader hdr, Record r) throws IOException;

    /**
     * Append a request to the transaction log with a digset
     *
     * @param hdr    the transaction header
     * @param r      the transaction itself
     * @param digest transaction digest
     *               returns true iff something appended, otw false
     * @throws IOException
     */
    boolean append(TxnHeader hdr, Record r, TxnDigest digest) throws IOException;

    /**
     * 读取事务日志
     * Start reading the transaction logs
     * from a given zxid
     *
     * @param zxid
     * @return returns an iterator to read the
     * next transaction in the logs.
     * @throws IOException
     */
    TxnIterator read(long zxid) throws IOException;

    /**
     * 获取最新事务ID
     * the last zxid of the logged transactions.
     *
     * @return the last zxid of the logged transactions.
     * @throws IOException
     */
    long getLastLoggedZxid() throws IOException;

    /**
     * 删除指定ID之后日志
     * truncate the log to get in sync with the
     * leader.
     *
     * @param zxid the zxid to truncate at.
     * @throws IOException
     */
    boolean truncate(long zxid) throws IOException;

    /**
     * the dbid for this transaction log.
     *
     * @return the dbid for this transaction log.
     * @throws IOException
     */
    long getDbId() throws IOException;

    /**
     * 提交所有日志
     * commit the transaction and make sure
     * they are persisted
     *
     * @throws IOException
     */
    void commit() throws IOException;

    /**
     * @return transaction log's elapsed sync time in milliseconds
     */
    long getTxnLogSyncElapsedTime();

    /**
     * close the transactions logs
     */
    @Override
    void close() throws IOException;

    /**
     * Sets the total size of all log files
     */
    void setTotalLogSize(long size);

    /**
     * Gets the total size of all log files
     */
    long getTotalLogSize();

    /**
     * 读取事务日志的迭代器接口
     * an iterating interface for reading
     * transaction logs.
     */
    interface TxnIterator extends Closeable {

        /**
         * 获取事务头部
         * return the transaction header.
         *
         * @return return the transaction header.
         */
        TxnHeader getHeader();

        /**
         * 获取事务内容
         * return the transaction record.
         *
         * @return return the transaction record.
         */
        Record getTxn();

        /**
         * @return the digest associated with the transaction.
         */
        TxnDigest getDigest();

        /**
         * 下个事务
         * go to the next transaction record.
         *
         * @throws IOException
         */
        boolean next() throws IOException;

        /**
         * close files and release the
         * resources
         *
         * @throws IOException
         */
        @Override
        void close() throws IOException;

        /**
         * Get an estimated storage space used to store transaction records
         * that will return by this iterator
         *
         * @throws IOException
         */
        long getStorageSize() throws IOException;

    }

}

