package org.apache.zookeeper.server.util;

/**
 * zxid工具类
 */
public class ZxidUtils {

    /**
     * 取出zxid高32位的任期号
     */
    public static long getEpochFromZxid(long zxid) {
        return zxid >> 32L;
    }

    /**
     * 取出zxid低32位日志序号
     */
    public static long getCounterFromZxid(long zxid) {
        return zxid & 0xffffffffL;
    }

    /**
     * 通过任期和日志序号生产zxid
     */
    public static long makeZxid(long epoch, long counter) {
        return (epoch << 32L) | (counter & 0xffffffffL);
    }

    /**
     * zxid输出字符串
     */
    public static String zxidToString(long zxid) {
        return Long.toHexString(zxid);
    }

}
