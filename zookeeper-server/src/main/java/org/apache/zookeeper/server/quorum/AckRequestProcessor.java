package org.apache.zookeeper.server.quorum;

import org.apache.zookeeper.server.Request;
import org.apache.zookeeper.server.RequestProcessor;
import org.apache.zookeeper.server.ServerMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a very simple RequestProcessor that simply forwards a request from a
 * previous stage to the leader as an ACK.
 */
class AckRequestProcessor implements RequestProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(AckRequestProcessor.class);
    Leader leader;

    AckRequestProcessor(Leader leader) {
        this.leader = leader;
    }

    /**
     * Forward the request as an ACK to the leader
     */
    @Override
    public void processRequest(Request request) {
        QuorumPeer self = leader.self;
        if (self != null) {
            request.logLatency(ServerMetrics.getMetrics().PROPOSAL_ACK_CREATION_LATENCY);
            leader.processAck(self.getId(), request.zxid, null);
        } else {
            LOG.error("Null QuorumPeer");
        }
    }

    @Override
    public void shutdown() {
        // TODO No need to do anything
    }

}
