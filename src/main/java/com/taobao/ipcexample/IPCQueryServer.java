package com.taobao.ipcexample;

import com.taobao.minirpc.RPC;
import com.taobao.minirpc.Server;
import org.apache.log4j.Level;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.PatternLayout;

/**
 * Created by shili on 14-2-28.
 */
public class IPCQueryServer {
    public static final int IPC_PORT = 32121;
    public static final long IPC_VER = 5473L;

    public static void main(String[] args) {
        try {
            ConsoleAppender append=new ConsoleAppender(new PatternLayout(PatternLayout.TTCC_CONVERSION_PATTERN));
            append.setThreshold(Level.DEBUG);
            BasicConfigurator.configure();

            IPCQueryStatusImpl queryService=new IPCQueryStatusImpl();

            Server server = RPC.getServer(queryService, "0.0.0.0", IPC_PORT, 1, true);
            server.start();

            System.out.println("Server ready, press any key to stop");
            System.in.read();

            server.stop();
            System.out.println("Server stopped");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
