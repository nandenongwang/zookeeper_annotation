package org.apache.zookeeper;

import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.AsyncCallback.MultiCallback;
import org.apache.zookeeper.data.ACL;

import java.util.ArrayList;
import java.util.List;

/**
 * 打包一组操作一次性进行multi请求
 * Provides a builder style interface for doing multiple updates.  This is
 * really just a thin layer on top of Zookeeper.multi().
 *
 * @since 3.4.0
 */
@InterfaceAudience.Public
public class Transaction {

    private final ZooKeeper zk;
    private final List<Op> ops = new ArrayList<>();

    protected Transaction(ZooKeeper zk) {
        this.zk = zk;
    }

    public Transaction create(final String path, byte[] data, List<ACL> acl, CreateMode createMode) {
        ops.add(Op.create(path, data, acl, createMode.toFlag()));
        return this;
    }

    public Transaction delete(final String path, int version) {
        ops.add(Op.delete(path, version));
        return this;
    }

    public Transaction check(String path, int version) {
        ops.add(Op.check(path, version));
        return this;
    }

    public Transaction setData(final String path, byte[] data, int version) {
        ops.add(Op.setData(path, data, version));
        return this;
    }

    public List<OpResult> commit() throws InterruptedException, KeeperException {
        return zk.multi(ops);
    }

    public void commit(MultiCallback cb, Object ctx) {
        zk.multi(ops, cb, ctx);
    }

}
