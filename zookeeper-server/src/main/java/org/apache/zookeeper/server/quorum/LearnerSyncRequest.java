package org.apache.zookeeper.server.quorum;

import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.server.Request;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * learner转发sync请求
 */
public class LearnerSyncRequest extends Request {

    LearnerHandler fh;

    public LearnerSyncRequest(
            LearnerHandler fh, long sessionId, int xid, int type, ByteBuffer bb, List<Id> authInfo) {
        super(null, sessionId, xid, type, bb, authInfo);
        this.fh = fh;
    }

}
