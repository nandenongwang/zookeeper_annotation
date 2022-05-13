package org.apache.zookeeper.server.quorum;

import org.apache.zookeeper.server.ZooKeeperCriticalThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * learner【follower、observer】向leader发包线程 【持续从缓冲区中取出、写入到leader输出流】
 */
public class LearnerSender extends ZooKeeperCriticalThread {
    private static final Logger LOG = LoggerFactory.getLogger(LearnerSender.class);

    private final LinkedBlockingQueue<QuorumPacket> queuedPackets = new LinkedBlockingQueue<>();
    private final QuorumPacket proposalOfDeath = new QuorumPacket();

    Learner learner;

    public LearnerSender(Learner learner) {
        super("LearnerSender:" + learner.zk.getServerId(), learner.zk.getZooKeeperServerListener());
        this.learner = learner;
    }

    @Override
    public void run() {
        while (true) {
            try {
                QuorumPacket p = queuedPackets.poll();
                if (p == null) {
                    learner.bufferedOutput.flush();
                    p = queuedPackets.take();
                }

                if (p == proposalOfDeath) {
                    // Packet of death!
                    break;
                }

                learner.messageTracker.trackSent(p.getType());
                learner.leaderOs.writeRecord(p, "packet");
            } catch (IOException e) {
                handleException(this.getName(), e);
                break;
            } catch (InterruptedException e) {
                handleException(this.getName(), e);
                break;
            }
        }

        LOG.info("LearnerSender exited");
    }

    public void queuePacket(QuorumPacket pp) throws IOException {
        if (pp == null) {
            learner.bufferedOutput.flush();
        } else {
            queuedPackets.add(pp);
        }
    }

    public void shutdown() {
        LOG.info("Shutting down LearnerSender");
        queuedPackets.clear();
        queuedPackets.add(proposalOfDeath);
    }
}
