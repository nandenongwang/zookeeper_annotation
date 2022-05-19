package org.apache.zookeeper.server;

/**
 * 请求处理链接口
 * RequestProcessors are chained together to process transactions. Requests are
 * always processed in order. The standalone server, follower, and leader all
 * have slightly different RequestProcessors chained together.
 *
 * Requests always move forward through the chain of RequestProcessors. Requests
 * are passed to a RequestProcessor through processRequest(). Generally method
 * will always be invoked by a single thread.
 *
 * When shutdown is called, the request RequestProcessor should also shutdown
 * any RequestProcessors that it is connected to.
 */
public interface RequestProcessor {

    @SuppressWarnings("serial")
    class RequestProcessorException extends Exception {

        public RequestProcessorException(String msg, Throwable t) {
            super(msg, t);
        }

    }

    void processRequest(Request request) throws RequestProcessorException;

    void shutdown();

}
