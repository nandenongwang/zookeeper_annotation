package org.apache.zookeeper.client;

import java.net.InetSocketAddress;
import java.util.Collection;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * 服务端地址提供者接口
 * A set of hosts a ZooKeeper client should connect to.
 *
 * Classes implementing this interface must guarantee the following:
 *
 * * Every call to next() returns an InetSocketAddress. So the iterator never
 * ends.
 *
 * * The size() of a HostProvider may never be zero.
 *
 * A HostProvider must return resolved InetSocketAddress instances on next() if the next address is resolvable.
 * In that case, it's up to the HostProvider, whether it returns the next resolvable address in the list or return
 * the next one as UnResolved.
 *
 * Different HostProvider could be imagined:
 *
 * * A HostProvider that loads the list of Hosts from an URL or from DNS
 * * A HostProvider that re-resolves the InetSocketAddress after a timeout.
 * * A HostProvider that prefers nearby hosts.
 */
@InterfaceAudience.Public
public interface HostProvider {

    int size();

    /**
     * The next host to try to connect to.
     *
     * For a spinDelay of 0 there should be no wait.
     *
     * @param spinDelay
     *            Milliseconds to wait if all hosts have been tried once.
     */
    InetSocketAddress next(long spinDelay);

    /**
     * Notify the HostProvider of a successful connection.
     *
     * The HostProvider may use this notification to reset it's inner state.
     */
    void onConnected();

    /**
     * Update the list of servers. This returns true if changing connections is necessary for load-balancing, false otherwise.
     * @param serverAddresses new host list
     * @param currentHost the host to which this client is currently connected
     * @return true if changing connections is necessary for load-balancing, false otherwise
     */
    boolean updateServerList(Collection<InetSocketAddress> serverAddresses, InetSocketAddress currentHost);

}
