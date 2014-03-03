package com.taobao.ipcexample;

import com.taobao.minirpc.RPC;
import com.taobao.minirpc.Server;
import org.apache.log4j.*;

/**
 * Created by shili on 14-2-28.
 */
public class IPCQueryServer {
    public static final int IPC_PORT = 32121;
    public static final long IPC_VER = 5473L;

    public static void main(String[] args) {
        try {
            //ConsoleAppender append=new ConsoleAppender(new PatternLayout(PatternLayout.TTCC_CONVERSION_PATTERN));
//            FileAppender append = new FileAppender(new PatternLayout(PatternLayout.TTCC_CONVERSION_PATTERN), "minirpc.log");
//            append.setThreshold(Level.DEBUG);
//            BasicConfigurator.configure();

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
