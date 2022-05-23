package org.apache.zookeeper.server.quorum;

import org.apache.jute.Record;
import org.apache.zookeeper.common.Time;
import org.apache.zookeeper.server.ObserverBean;
import org.apache.zookeeper.server.Request;
import org.apache.zookeeper.server.ServerMetrics;
import org.apache.zookeeper.server.TxnLogEntry;
import org.apache.zookeeper.server.quorum.QuorumPeer.QuorumServer;
import org.apache.zookeeper.server.quorum.flexible.QuorumVerifier;
import org.apache.zookeeper.server.util.SerializeUtils;
import org.apache.zookeeper.txn.SetDataTxn;
import org.apache.zookeeper.txn.TxnDigest;
import org.apache.zookeeper.txn.TxnHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReference;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * observer服务控制器
 * Observers are peers that do not take part in the atomic broadcast protocol.
 * Instead, they are informed of successful proposals by the Leader. Observers
 * therefore naturally act as a relay point for publishing the proposal stream
 * and can relieve Followers of some of the connection load. Observers may
 * submit proposals, but do not vote in their acceptance.
 * <p>
 * See ZOOKEEPER-368 for a discussion of this feature.
 */
public class Observer extends Learner {

    private static final Logger LOG = LoggerFactory.getLogger(Observer.class);

    /**
     * When observer lost its connection with the leader, it waits for 0 to the
     * specified value before trying to reconnect with the leader. So that
     * the entire observer fleet won't try to run leader election and reconnect
     * to the leader at once. Default value is zero.
     */
    public static final String OBSERVER_RECONNECT_DELAY_MS = "zookeeper.observer.reconnectDelayMs";

    /**
     * Delay the Observer's participation in a leader election upon disconnect
     * so as to prevent unexpected additional load on the voting peers during
     * the process. Default value is 200.
     */
    public static final String OBSERVER_ELECTION_DELAY_MS = "zookeeper.observer.election.DelayMs";

    private static final long reconnectDelayMs;

    private static volatile long observerElectionDelayMs;

    static {
        reconnectDelayMs = Long.getLong(OBSERVER_RECONNECT_DELAY_MS, 0);
        LOG.info("{} = {}", OBSERVER_RECONNECT_DELAY_MS, reconnectDelayMs);
        observerElectionDelayMs = Long.getLong(OBSERVER_ELECTION_DELAY_MS, 200);
        LOG.info("{} = {}", OBSERVER_ELECTION_DELAY_MS, observerElectionDelayMs);
    }

    /**
     * next learner master to try, when specified
     */
    private static final AtomicReference<QuorumPeer.QuorumServer> nextLearnerMaster = new AtomicReference<>();

    private QuorumPeer.QuorumServer currentLearnerMaster = null;

    Observer(QuorumPeer self, ObserverZooKeeperServer observerZooKeeperServer) {
        this.self = self;
        this.zk = observerZooKeeperServer;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Observer ").append(sock);
        sb.append(" pendingRevalidationCount:").append(pendingRevalidations.size());
        return sb.toString();
    }

    /**
     * observer
     * the main method called by the observer to observe the leader
     *
     * @throws Exception
     */
    void observeLeader() throws Exception {
        zk.registerJMX(new ObserverBean(this, zk), self.jmxLocalPeerBean);
        long connectTime = 0;
        boolean completedSync = false;
        try {
            self.setZabState(QuorumPeer.ZabState.DISCOVERY);
            QuorumServer master = findLearnerMaster();
            try {
                //region 与leader建立连接并注册该observer
                connectToLeader(master.addr, master.hostname);
                connectTime = System.currentTimeMillis();
                long newLeaderZxid = registerWithLeader(Leader.OBSERVERINFO);
                if (self.isReconfigStateChange()) {
                    throw new Exception("learned about role change");
                }
                //endregion

                //region 注册完毕、进入同步阶段
                final long startTime = Time.currentElapsedTime();
                self.setLeaderAddressAndId(master.addr, master.getId());

                self.setZabState(QuorumPeer.ZabState.SYNCHRONIZATION);
                syncWithLeader(newLeaderZxid);
                //endregion

                //region 同步完成、进入消息广播阶段 【持续读取报文并处理报文】
                self.setZabState(QuorumPeer.ZabState.BROADCAST);
                completedSync = true;
                final long syncTime = Time.currentElapsedTime() - startTime;
                ServerMetrics.getMetrics().OBSERVER_SYNC_TIME.add(syncTime);
                QuorumPacket qp = new QuorumPacket();
                while (this.isRunning() && nextLearnerMaster.get() == null) {
                    readPacket(qp);
                    processPacket(qp);
                }
                //endregion

            } catch (Exception e) {
                LOG.warn("Exception when observing the leader", e);
                closeSocket();

                // clear pending revalidations
                pendingRevalidations.clear();
            }
        } finally {
            currentLearnerMaster = null;
            zk.unregisterJMX(this);
            if (connectTime != 0) {
                long connectionDuration = System.currentTimeMillis() - connectTime;

                LOG.info(
                        "Disconnected from leader (with address: {}). Was connected for {}ms. Sync state: {}",
                        leaderAddr,
                        connectionDuration,
                        completedSync);
                messageTracker.dumpToLog(leaderAddr.toString());
            }
        }
    }

    /**
     * 获取当前learner的leader地址
     */
    private QuorumServer findLearnerMaster() {
        QuorumPeer.QuorumServer prescribedLearnerMaster = nextLearnerMaster.getAndSet(null);
        if (prescribedLearnerMaster != null
                && self.validateLearnerMaster(Long.toString(prescribedLearnerMaster.id)) == null) {
            LOG.warn("requested next learner master {} is no longer valid", prescribedLearnerMaster);
            prescribedLearnerMaster = null;
        }
        final QuorumPeer.QuorumServer master = (prescribedLearnerMaster == null)
                ? self.findLearnerMaster(findLeader())
                : prescribedLearnerMaster;
        currentLearnerMaster = master;
        if (master == null) {
            LOG.warn("No learner master found");
        } else {
            LOG.info("Observing new leader sid={} addr={}", master.id, master.addr);
        }
        return master;
    }

    /**
     * 处理从leader接收到的报文
     * Controls the response of an observer to the receipt of a quorumpacket
     */
    protected void processPacket(QuorumPacket qp) throws Exception {
        TxnLogEntry logEntry;
        TxnHeader hdr;
        TxnDigest digest;
        Record txn;
        switch (qp.getType()) {
            //region 向leader汇报该observer客户端连接情况
            case Leader.PING:
                ping(qp);
                break;
            //endregion

            case Leader.PROPOSAL:
                LOG.warn("Ignoring proposal");
                break;
            case Leader.COMMIT:
                LOG.warn("Ignoring commit");
                break;
            case Leader.UPTODATE:
                LOG.error("Received an UPTODATE message after Observer started");
                break;
            case Leader.REVALIDATE:
                revalidate(qp);
                break;
            case Leader.SYNC:
                ((ObserverZooKeeperServer) zk).sync();
                break;
            //region 接受leader新的已达成一致的已提交日志通知、直接提交到本地
            case Leader.INFORM:
                ServerMetrics.getMetrics().LEARNER_COMMIT_RECEIVED_COUNT.add(1);
                logEntry = SerializeUtils.deserializeTxn(qp.getData());
                hdr = logEntry.getHeader();
                txn = logEntry.getTxn();
                digest = logEntry.getDigest();
                Request request = new Request(hdr.getClientId(), hdr.getCxid(), hdr.getType(), hdr, txn, 0);
                request.logLatency(ServerMetrics.getMetrics().COMMIT_PROPAGATION_LATENCY);
                request.setTxnDigest(digest);
                ObserverZooKeeperServer obs = (ObserverZooKeeperServer) zk;
                obs.commitRequest(request);
                break;
            //endregion

            case Leader.INFORMANDACTIVATE:
                // get new designated leader from (current) leader's message
                ByteBuffer buffer = ByteBuffer.wrap(qp.getData());
                long suggestedLeaderId = buffer.getLong();

                byte[] remainingdata = new byte[buffer.remaining()];
                buffer.get(remainingdata);
                logEntry = SerializeUtils.deserializeTxn(remainingdata);
                hdr = logEntry.getHeader();
                txn = logEntry.getTxn();
                digest = logEntry.getDigest();
                QuorumVerifier qv = self.configFromString(new String(((SetDataTxn) txn).getData(), UTF_8));

                request = new Request(hdr.getClientId(), hdr.getCxid(), hdr.getType(), hdr, txn, 0);
                request.setTxnDigest(digest);
                obs = (ObserverZooKeeperServer) zk;

                boolean majorChange = self.processReconfig(qv, suggestedLeaderId, qp.getZxid(), true);

                obs.commitRequest(request);

                if (majorChange) {
                    throw new Exception("changes proposed in reconfig");
                }
                break;
            default:
                LOG.warn("Unknown packet type: {}", LearnerHandler.packetToString(qp));
                break;
        }
    }

    /**
     * Shutdown the Observer.
     */
    @Override
    public void shutdown() {
        LOG.info("shutdown Observer");
        super.shutdown();
    }

    static void waitForReconnectDelay() {
        waitForReconnectDelayHelper(reconnectDelayMs);
    }

    static void waitForObserverElectionDelay() {
        waitForReconnectDelayHelper(observerElectionDelayMs);
    }

    private static void waitForReconnectDelayHelper(long delayValueMs) {
        if (delayValueMs > 0) {
            long randomDelay = ThreadLocalRandom.current().nextLong(delayValueMs);
            LOG.info("Waiting for {} ms before reconnecting with the leader", randomDelay);
            try {
                Thread.sleep(randomDelay);
            } catch (InterruptedException e) {
                LOG.warn("Interrupted while waiting", e);
            }
        }
    }

    public long getLearnerMasterId() {
        QuorumPeer.QuorumServer current = currentLearnerMaster;
        return current == null ? -1 : current.id;
    }

    /**
     * Prompts the Observer to disconnect from its current learner master and reconnect
     * to the specified server. If that connection attempt fails, the Observer will
     * fail over to the next available learner master.
     */
    public boolean setLearnerMaster(String learnerMaster) {
        final QuorumPeer.QuorumServer server = self.validateLearnerMaster(learnerMaster);
        if (server == null) {
            return false;
        } else if (server.equals(currentLearnerMaster)) {
            LOG.info("Already connected to requested learner master sid={} addr={}", server.id, server.addr);
            return true;
        } else {
            LOG.info("Requesting disconnect and reconnect to new learner master sid={} addr={}", server.id, server.addr);
            nextLearnerMaster.set(server);
            return true;
        }
    }

    public QuorumPeer.QuorumServer getCurrentLearnerMaster() {
        return currentLearnerMaster;
    }

    public static long getObserverElectionDelayMs() {
        return observerElectionDelayMs;
    }

    public static void setObserverElectionDelayMs(long electionDelayMs) {
        observerElectionDelayMs = electionDelayMs;
        LOG.info("{} = {}", OBSERVER_ELECTION_DELAY_MS, observerElectionDelayMs);
    }

}

