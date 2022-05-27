package org.apache.zookeeper.server.quorum;

import org.apache.zookeeper.server.SessionTrackerImpl;
import org.apache.zookeeper.server.ZooKeeperServerListener;

import java.util.Set;
import java.util.concurrent.ConcurrentMap;

/**
 * 本地session管理器 【标记为管理本地session使用】
 * Local session tracker.
 */
public class LocalSessionTracker extends SessionTrackerImpl {
    public LocalSessionTracker(SessionExpirer expirer, ConcurrentMap<Long, Integer> sessionsWithTimeouts, int tickTime, long id, ZooKeeperServerListener listener) {
        super(expirer, sessionsWithTimeouts, tickTime, id, listener);
    }

    public boolean isLocalSession(long sessionId) {
        return isTrackingSession(sessionId);
    }

    public boolean isGlobalSession(long sessionId) {
        return false;
    }

    @Override
    public long createSession(int sessionTimeout) {
        long sessionId = super.createSession(sessionTimeout);
        commitSession(sessionId, sessionTimeout);
        return sessionId;
    }

    @Override
    public Set<Long> localSessions() {
        return sessionsWithTimeout.keySet();
    }
}
