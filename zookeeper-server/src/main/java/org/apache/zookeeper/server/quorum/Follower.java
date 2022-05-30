package org.apache.zookeeper.server.quorum;

import org.apache.jute.Record;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.common.Time;
import org.apache.zookeeper.server.Request;
import org.apache.zookeeper.server.ServerMetrics;
import org.apache.zookeeper.server.TxnLogEntry;
import org.apache.zookeeper.server.quorum.QuorumPeer.QuorumServer;
import org.apache.zookeeper.server.quorum.flexible.QuorumVerifier;
import org.apache.zookeeper.server.util.SerializeUtils;
import org.apache.zookeeper.server.util.ZxidUtils;
import org.apache.zookeeper.txn.SetDataTxn;
import org.apache.zookeeper.txn.TxnDigest;
import org.apache.zookeeper.txn.TxnHeader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * 普通议员服务窗口
 * This class has the control logic for the Follower.
 */
public class Follower extends Learner {

    private long lastQueued;
    // This is the same object as this.zk, but we cache the downcast op
    final FollowerZooKeeperServer fzk;

    ObserverMaster om;

    Follower(final QuorumPeer self, final FollowerZooKeeperServer zk) {
        this.self = Objects.requireNonNull(self);
        this.fzk = Objects.requireNonNull(zk);
        this.zk = zk;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Follower ").append(sock);
        sb.append(" lastQueuedZxid:").append(lastQueued);
        sb.append(" pendingRevalidationCount:").append(pendingRevalidations.size());
        return sb.toString();
    }

    /**
     * the main method called by the follower to follow the leader
     *
     * @throws InterruptedException
     */
    void followLeader() throws InterruptedException {

        //region 计算选举花费时间并注册FollowerBean
        self.end_fle = Time.currentElapsedTime();
        long electionTimeTaken = self.end_fle - self.start_fle;
        self.setElectionTimeTaken(electionTimeTaken);
        ServerMetrics.getMetrics().ELECTION_TIME.add(electionTimeTaken);
        LOG.info("FOLLOWING - LEADER ELECTION TOOK - {} {}", electionTimeTaken, QuorumPeer.FLE_TIME_UNIT);
        self.start_fle = 0;
        self.end_fle = 0;
        fzk.registerJMX(new FollowerBean(this, zk), self.jmxLocalPeerBean);
        //endregion

        //region 与observer相似 【与leader建立连接、注册本节点成follower、同步数据后开始事件循环】
        long connectionTime = 0;
        boolean completedSync = false;
        try {
            self.setZabState(QuorumPeer.ZabState.DISCOVERY);
            QuorumServer leaderServer = findLeader();
            try {
                //region 发现阶段 【发送learner信息注册包、处理响应leader信息包并回复任期确认包】
                connectToLeader(leaderServer.addr, leaderServer.hostname);
                connectionTime = System.currentTimeMillis();
                long newEpochZxid = registerWithLeader(Leader.FOLLOWERINFO);
                if (self.isReconfigStateChange()) {
                    throw new Exception("learned about role change");
                }
                //check to see if the leader zxid is lower than ours
                //this should never happen but is just a safety check
                long newEpoch = ZxidUtils.getEpochFromZxid(newEpochZxid);
                if (newEpoch < self.getAcceptedEpoch()) {
                    LOG.error("Proposed leader epoch "
                            + ZxidUtils.zxidToString(newEpochZxid)
                            + " is less than our accepted epoch "
                            + ZxidUtils.zxidToString(self.getAcceptedEpoch()));
                    throw new IOException("Error: Epoch of leader is lower");
                }
                long startTime = Time.currentElapsedTime();
                self.setLeaderAddressAndId(leaderServer.addr, leaderServer.getId());
                //endregion

                self.setZabState(QuorumPeer.ZabState.SYNCHRONIZATION);
                //region 同步阶段
                syncWithLeader(newEpochZxid);
                //endregion

                self.setZabState(QuorumPeer.ZabState.BROADCAST);
                //region 广播阶段 【读取leader消息、处理消息】
                completedSync = true;
                long syncTime = Time.currentElapsedTime() - startTime;
                ServerMetrics.getMetrics().FOLLOWER_SYNC_TIME.add(syncTime);
                if (self.getObserverMasterPort() > 0) {
                    LOG.info("Starting ObserverMaster");

                    om = new ObserverMaster(self, fzk, self.getObserverMasterPort());
                    om.start();
                } else {
                    om = null;
                }
                // create a reusable packet to reduce gc impact
                QuorumPacket qp = new QuorumPacket();
                while (this.isRunning()) {
                    readPacket(qp);
                    processPacket(qp);
                }
                //endregion
            } catch (Exception e) {
                LOG.warn("Exception when following the leader", e);
                closeSocket();
                // clear pending revalidations
                pendingRevalidations.clear();
            }
        } finally {
            if (om != null) {
                om.stop();
            }
            zk.unregisterJMX(this);

            if (connectionTime != 0) {
                long connectionDuration = System.currentTimeMillis() - connectionTime;
                LOG.info(
                        "Disconnected from leader (with address: {}). Was connected for {}ms. Sync state: {}",
                        leaderAddr,
                        connectionDuration,
                        completedSync);
                messageTracker.dumpToLog(leaderAddr.toString());
            }
        }
        //endregion
    }

    /**
     * 处理leader消息
     * Examine the packet received in qp and dispatch based on its contents.
     *
     * @param qp
     * @throws IOException
     */
    protected void processPacket(QuorumPacket qp) throws Exception {
        switch (qp.getType()) {

            //region PING 【探活并获取该learner客户端连接信息】
            case Leader.PING: {
                ping(qp);
                break;
            }
            //endregion

            //region 同步提案 【记录提案日志】
            case Leader.PROPOSAL: {
                ServerMetrics.getMetrics().LEARNER_PROPOSAL_RECEIVED_COUNT.add(1);
                TxnLogEntry logEntry = SerializeUtils.deserializeTxn(qp.getData());
                TxnHeader hdr = logEntry.getHeader();
                Record txn = logEntry.getTxn();
                TxnDigest digest = logEntry.getDigest();
                if (hdr.getZxid() != lastQueued + 1) {
                    LOG.warn(
                            "Got zxid 0x{} expected 0x{}",
                            Long.toHexString(hdr.getZxid()),
                            Long.toHexString(lastQueued + 1));
                }
                lastQueued = hdr.getZxid();

                if (hdr.getType() == OpCode.reconfig) {
                    SetDataTxn setDataTxn = (SetDataTxn) txn;
                    QuorumVerifier qv = self.configFromString(new String(setDataTxn.getData(), UTF_8));
                    self.setLastSeenQuorumVerifier(qv, true);
                }

                fzk.logRequest(hdr, txn, digest);
                if (hdr != null) {
                    /*
                     * Request header is created only by the leader, so this is only set
                     * for quorum packets. If there is a clock drift, the latency may be
                     * negative. Headers use wall time, not CLOCK_MONOTONIC.
                     */
                    long now = Time.currentWallTime();
                    long latency = now - hdr.getTime();
                    if (latency >= 0) {
                        ServerMetrics.getMetrics().PROPOSAL_LATENCY.add(latency);
                    }
                }
                if (om != null) {
                    final long startTime = Time.currentElapsedTime();
                    om.proposalReceived(qp);
                    ServerMetrics.getMetrics().OM_PROPOSAL_PROCESS_TIME.add(Time.currentElapsedTime() - startTime);
                }
                break;
            }
            //endregion

            //region 提交提案 【提案已复制到多数节点、可以提交应用】
            case Leader.COMMIT: {
                ServerMetrics.getMetrics().LEARNER_COMMIT_RECEIVED_COUNT.add(1);
                fzk.commit(qp.getZxid());
                if (om != null) {
                    final long startTime = Time.currentElapsedTime();
                    om.proposalCommitted(qp.getZxid());
                    ServerMetrics.getMetrics().OM_COMMIT_PROCESS_TIME.add(Time.currentElapsedTime() - startTime);
                }
                break;
            }
            //endregion

            //region 提交并执行配置变更提案 【若leader变更需连接到新leader】
            case Leader.COMMITANDACTIVATE:
                // get the new configuration from the request
                Request request = fzk.pendingTxns.element();
                SetDataTxn setDataTxn = (SetDataTxn) request.getTxn();
                QuorumVerifier qv = self.configFromString(new String(setDataTxn.getData(), UTF_8));

                // get new designated leader from (current) leader's message
                ByteBuffer buffer = ByteBuffer.wrap(qp.getData());
                long suggestedLeaderId = buffer.getLong();
                final long zxid = qp.getZxid();
                boolean majorChange = self.processReconfig(qv, suggestedLeaderId, zxid, true);
                // commit (writes the new config to ZK tree (/zookeeper/config)
                fzk.commit(zxid);

                if (om != null) {
                    om.informAndActivate(zxid, suggestedLeaderId);
                }
                if (majorChange) {
                    throw new Exception("changes proposed in reconfig");
                }
                break;
            //endregion

            //region 同步完成 【广播阶段不会收到此类型消息】
            case Leader.UPTODATE:
                LOG.error("Received an UPTODATE message after Follower started");
                break;
            //endregion

            case Leader.REVALIDATE:
                if (om == null || !om.revalidateLearnerSession(qp)) {
                    revalidate(qp);
                }
                break;
            case Leader.SYNC:
                fzk.sync();
                break;
            default:
                LOG.warn("Unknown packet type: {}", LearnerHandler.packetToString(qp));
                break;

        }
    }

    /**
     * The zxid of the last operation seen
     *
     * @return zxid
     */
    public long getZxid() {
        synchronized (fzk) {
            return fzk.getZxid();
        }
    }

    /**
     * The zxid of the last operation queued
     *
     * @return zxid
     */
    protected long getLastQueued() {
        return lastQueued;
    }

    public Integer getSyncedObserverSize() {
        return om == null ? null : om.getNumActiveObservers();
    }

    public Iterable<Map<String, Object>> getSyncedObserversInfo() {
        if (om != null && om.getNumActiveObservers() > 0) {
            return om.getActiveObservers();
        }
        return Collections.emptySet();
    }

    public void resetObserverConnectionStats() {
        if (om != null && om.getNumActiveObservers() > 0) {
            om.resetObserverConnectionStats();
        }
    }

    @Override
    public void shutdown() {
        LOG.info("shutdown Follower");
        super.shutdown();
    }

}
