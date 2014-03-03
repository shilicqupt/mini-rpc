package com.taobao.ipcexample;

import com.taobao.minirpc.RPC;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * Created by shili on 14-3-3.
 */
public class IPCQueryParaClient extends Thread{
    public void run(){
        InetSocketAddress addr = new InetSocketAddress("10.74.156.106", IPCQueryServer.IPC_PORT);
        IPCQueryStatus query = null;
        try {
            query = (IPCQueryStatus) RPC.getProxy(IPCQueryStatus.class, IPCQueryServer.IPC_VER, addr);
        } catch (IOException e) {
            e.printStackTrace();
        }
        IPCFileStatus status = query.getFileStatus("/Users/shili/antx.properties");
        System.out.println(status);
        RPC.stopProxy(query);
    }

    public static void main(String[] args){
        for (int i=0; i<10; i++){
            IPCQueryParaClient ipcQueryParaClient = new IPCQueryParaClient();
            ipcQueryParaClient.start();
        }
    }
}
