package org.apache.zookeeper;

import org.apache.jute.BinaryInputArchive;
import org.apache.zookeeper.ClientCnxn.Packet;
import org.apache.zookeeper.client.ZKClientConfig;
import org.apache.zookeeper.common.Time;
import org.apache.zookeeper.common.ZKConfig;
import org.apache.zookeeper.proto.ConnectResponse;
import org.apache.zookeeper.server.ByteBufferInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.text.MessageFormat;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A ClientCnxnSocket does the lower level communication with a socket
 * implementation.
 * <p>
 * This code has been moved out of ClientCnxn so that a Netty implementation can
 * be provided as an alternative to the NIO socket code.
 */
abstract class ClientCnxnSocket {

    private static final Logger LOG = LoggerFactory.getLogger(ClientCnxnSocket.class);

    protected boolean initialized;

    /**
     * 消息长度buffer
     * This buffer is only used to read the length of the incoming message.
     */
    protected final ByteBuffer lenBuffer = ByteBuffer.allocateDirect(4);

    /**
     * 消息内容buffer
     * After the length is read, a new incomingBuffer is allocated in
     * readLength() to receive the full message.
     */
    protected ByteBuffer incomingBuffer = lenBuffer;
    protected final AtomicLong sentCount = new AtomicLong(0L);
    protected final AtomicLong recvCount = new AtomicLong(0L);

    /**
     * 上次接收消息时间
     */
    protected long lastHeard;

    /**
     * 上次发送消息时间
     */
    protected long lastSend;

    protected long now;

    /**
     * 发送线程
     */
    protected ClientCnxn.SendThread sendThread;

    /**
     * 发送缓冲
     */
    protected LinkedBlockingDeque<Packet> outgoingQueue;
    protected ZKClientConfig clientConfig;
    private int packetLen = ZKClientConfig.CLIENT_MAX_PACKET_LENGTH_DEFAULT;

    /**
     * The sessionId is only available here for Log and Exception messages.
     * Otherwise the socket doesn't need to know it.
     */
    protected long sessionId;

    void introduce(ClientCnxn.SendThread sendThread, long sessionId, LinkedBlockingDeque<Packet> outgoingQueue) {
        this.sendThread = sendThread;
        this.sessionId = sessionId;
        this.outgoingQueue = outgoingQueue;
    }

    void updateNow() {
        now = Time.currentElapsedTime();
    }

    int getIdleRecv() {
        return (int) (now - lastHeard);
    }

    int getIdleSend() {
        return (int) (now - lastSend);
    }

    long getSentCount() {
        return sentCount.get();
    }

    long getRecvCount() {
        return recvCount.get();
    }

    void updateLastHeard() {
        this.lastHeard = now;
    }

    void updateLastSend() {
        this.lastSend = now;
    }

    void updateLastSendAndHeard() {
        this.lastSend = now;
        this.lastHeard = now;
    }

    /**
     * 分配读取内容buffer
     */
    void readLength() throws IOException {
        int len = incomingBuffer.getInt();
        if (len < 0 || len > packetLen) {
            throw new IOException("Packet len " + len + " is out of range!");
        }
        incomingBuffer = ByteBuffer.allocate(len);
    }

    /**
     * 处理创建连接结果 【读取sessionId、sessionPwd完成会话建立并发送事件通知默认监听器】
     */
    void readConnectResult() throws IOException {
        if (LOG.isTraceEnabled()) {
            StringBuilder buf = new StringBuilder("0x[");
            for (byte b : incomingBuffer.array()) {
                buf.append(Integer.toHexString(b)).append(",");
            }
            buf.append("]");
            if (LOG.isTraceEnabled()) {
                LOG.trace("readConnectResult {} {}", incomingBuffer.remaining(), buf.toString());
            }
        }

        ByteBufferInputStream bbis = new ByteBufferInputStream(incomingBuffer);
        BinaryInputArchive bbia = BinaryInputArchive.getArchive(bbis);
        ConnectResponse conRsp = new ConnectResponse();
        conRsp.deserialize(bbia, "connect");

        // read "is read-only" flag
        boolean isRO = false;
        try {
            isRO = bbia.readBool("readOnly");
        } catch (IOException e) {
            // this is ok -- just a packet from an old server which
            // doesn't contain readOnly field
            LOG.warn("Connected to an old server; r-o mode will be unavailable");
        }

        this.sessionId = conRsp.getSessionId();
        sendThread.onConnected(conRsp.getTimeOut(), this.sessionId, conRsp.getPasswd(), isRO);
    }

    abstract boolean isConnected();

    abstract void connect(InetSocketAddress addr) throws IOException;

    /**
     * Returns the address to which the socket is connected.
     */
    abstract SocketAddress getRemoteSocketAddress();

    /**
     * Returns the address to which the socket is bound.
     */
    abstract SocketAddress getLocalSocketAddress();

    /**
     * Clean up resources for a fresh new socket.
     * It's called before reconnect or close.
     */
    abstract void cleanup();

    /**
     * new packets are added to outgoingQueue.
     */
    abstract void packetAdded();

    /**
     * connState is marked CLOSED and notify ClientCnxnSocket to react.
     */
    abstract void onClosing();

    /**
     * Sasl completes. Allows non-priming packgets to be sent.
     * Note that this method will only be called if Sasl starts and completes.
     */
    abstract void saslCompleted();

    /**
     * being called after ClientCnxn finish PrimeConnection
     */
    abstract void connectionPrimed();

    /**
     * Do transportation work:
     * - read packets into incomingBuffer.
     * - write outgoing queue packets.
     * - update relevant timestamp.
     *
     * @param waitTimeOut  timeout in blocking wait. Unit in MilliSecond.
     * @param pendingQueue These are the packets that have been sent and
     *                     are waiting for a response.
     * @param cnxn
     * @throws IOException
     * @throws InterruptedException
     */
    abstract void doTransport(int waitTimeOut, Queue<Packet> pendingQueue, ClientCnxn cnxn) throws IOException, InterruptedException;

    /**
     * Close the socket.
     */
    abstract void testableCloseSocket() throws IOException;

    /**
     * Close this client.
     */
    abstract void close();

    /**
     * Send Sasl packets directly.
     * The Sasl process will send the first (requestHeader == null) packet,
     * and then block the doTransport write,
     * finally unblock it when finished.
     */
    abstract void sendPacket(Packet p) throws IOException;

    /**
     * 初始化相关默认配置 【最大包长度】
     */
    protected void initProperties() throws IOException {
        try {
            packetLen = clientConfig.getInt(
                    ZKConfig.JUTE_MAXBUFFER,
                    ZKClientConfig.CLIENT_MAX_PACKET_LENGTH_DEFAULT);
            LOG.info("{} value is {} Bytes", ZKConfig.JUTE_MAXBUFFER, packetLen);
        } catch (NumberFormatException e) {
            String msg = MessageFormat.format(
                    "Configured value {0} for property {1} can not be parsed to int",
                    clientConfig.getProperty(ZKConfig.JUTE_MAXBUFFER),
                    ZKConfig.JUTE_MAXBUFFER);
            LOG.error(msg);
            throw new IOException(msg);
        }
    }

}
