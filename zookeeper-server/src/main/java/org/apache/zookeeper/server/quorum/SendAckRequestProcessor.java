package org.apache.zookeeper.server.quorum;

import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.server.Request;
import org.apache.zookeeper.server.RequestProcessor;
import org.apache.zookeeper.server.ServerMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Flushable;
import java.io.IOException;

/**
 * follower向leader确认收到提案
 */
public class SendAckRequestProcessor implements RequestProcessor, Flushable {

    private static final Logger LOG = LoggerFactory.getLogger(SendAckRequestProcessor.class);

    Learner learner;

    SendAckRequestProcessor(Learner peer) {
        this.learner = peer;
    }

    @Override
    public void processRequest(Request si) {
        if (si.type != OpCode.sync) {
            QuorumPacket qp = new QuorumPacket(Leader.ACK, si.getHdr().getZxid(), null, null);
            try {
                si.logLatency(ServerMetrics.getMetrics().PROPOSAL_ACK_CREATION_LATENCY);

                learner.writePacket(qp, false);
            } catch (IOException e) {
                LOG.warn("Closing connection to leader, exception during packet send", e);
                try {
                    if (!learner.sock.isClosed()) {
                        learner.sock.close();
                    }
                } catch (IOException e1) {
                    // Nothing to do, we are shutting things down, so an exception here is irrelevant
                    LOG.debug("Ignoring error closing the connection", e1);
                }
            }
        }
    }

    @Override
    public void flush() throws IOException {
        try {
            learner.writePacket(null, true);
        } catch (IOException e) {
            LOG.warn("Closing connection to leader, exception during packet send", e);
            try {
                if (!learner.sock.isClosed()) {
                    learner.sock.close();
                }
            } catch (IOException e1) {
                // Nothing to do, we are shutting things down, so an exception here is irrelevant
                LOG.debug("Ignoring error closing the connection", e1);
            }
        }
    }

    @Override
    public void shutdown() {
        // Nothing needed
    }

}
