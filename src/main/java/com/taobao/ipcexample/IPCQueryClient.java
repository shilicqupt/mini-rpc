package com.taobao.ipcexample;

import com.taobao.minirpc.RPC;

import java.net.InetSocketAddress;

/**
 * Created by shili on 14-2-28.
 */
public class IPCQueryClient {
    public static void main(String[] args) {
        try {
            System.out.println("Interface name: "+IPCQueryStatus.class.getName());
            System.out.println("Interface name: "+IPCQueryStatus.class.getMethod("getFileStatus", String.class).getName());

            InetSocketAddress addr = new InetSocketAddress("localhost", IPCQueryServer.IPC_PORT);
            IPCQueryStatus query = (IPCQueryStatus) RPC.getProxy(IPCQueryStatus.class, IPCQueryServer.IPC_VER, addr);
            IPCFileStatus status = query.getFileStatus("/Users/shili/antx.properties");
            System.out.println(status);
            RPC.stopProxy(query);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
