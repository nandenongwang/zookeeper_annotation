package org.apache.zookeeper.client;

import org.apache.zookeeper.common.NetUtils;
import org.apache.zookeeper.common.PathUtils;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import static org.apache.zookeeper.common.StringUtils.split;

/**
 * 连接字符串解析器、从类似ip1:port1,ip2:port2,ip3:port3/home解析出zkServer地址列表【ip1:port1,ip2:port2,ip3:port3】和rootPath【/home】
 * A parser for ZooKeeper Client connect strings.
 * <p>
 * This class is not meant to be seen or used outside of ZooKeeper itself.
 * <p>
 * The chrootPath member should be replaced by a Path object in issue
 * ZOOKEEPER-849.
 *
 * @see org.apache.zookeeper.ZooKeeper
 */
public final class ConnectStringParser {

    private static final int DEFAULT_PORT = 2181;

    private final String chrootPath;

    private final ArrayList<InetSocketAddress> serverAddresses = new ArrayList<>();

    /**
     * Parse host and port by spliting client connectString
     * with support for IPv6 literals
     *
     * @throws IllegalArgumentException for an invalid chroot path.
     */
    public ConnectStringParser(String connectString) {
        // parse out chroot, if any
        int off = connectString.indexOf('/');
        if (off >= 0) {
            String chrootPath = connectString.substring(off);
            // ignore "/" chroot spec, same as null
            if (chrootPath.length() == 1) {
                this.chrootPath = null;
            } else {
                PathUtils.validatePath(chrootPath);
                this.chrootPath = chrootPath;
            }
            connectString = connectString.substring(0, off);
        } else {
            this.chrootPath = null;
        }

        List<String> hostsList = split(connectString, ",");
        for (String host : hostsList) {
            int port = DEFAULT_PORT;
            String[] hostAndPort = NetUtils.getIPV6HostAndPort(host);
            if (hostAndPort.length != 0) {
                host = hostAndPort[0];
                if (hostAndPort.length == 2) {
                    port = Integer.parseInt(hostAndPort[1]);
                }
            } else {
                int pidx = host.lastIndexOf(':');
                if (pidx >= 0) {
                    // otherwise : is at the end of the string, ignore
                    if (pidx < host.length() - 1) {
                        port = Integer.parseInt(host.substring(pidx + 1));
                    }
                    host = host.substring(0, pidx);
                }
            }
            serverAddresses.add(InetSocketAddress.createUnresolved(host, port));
        }
    }

    public String getChrootPath() {
        return chrootPath;
    }

    public ArrayList<InetSocketAddress> getServerAddresses() {
        return serverAddresses;
    }

}
