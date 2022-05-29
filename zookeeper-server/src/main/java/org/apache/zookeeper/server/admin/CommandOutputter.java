package org.apache.zookeeper.server.admin;

import java.io.PrintWriter;

/**
 * 输出格式化器
 * CommandOutputters are used to format the responses from Commands.
 *
 * @see Command
 * @see JettyAdminServer
 */
public interface CommandOutputter {

    /** The MIME type of this output (e.g., "application/json") */
    String getContentType();

    void output(CommandResponse response, PrintWriter pw);

}
