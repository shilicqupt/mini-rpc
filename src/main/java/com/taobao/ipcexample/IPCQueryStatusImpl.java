package com.taobao.ipcexample;

import java.io.IOException;

/**
 * Created by shili on 14-2-28.
 */
public class IPCQueryStatusImpl implements IPCQueryStatus {
    protected IPCQueryStatusImpl() {
    }

    @Override
    public IPCFileStatus getFileStatus(String filename) {
        IPCFileStatus status=new IPCFileStatus(filename);
        return status;
    }

    @Override
    public long getProtocolVersion(String protocol, long clientVersion) throws IOException {
        System.out.println("protocol: "+protocol);
        System.out.println("clientVersion: "+clientVersion);
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return IPCQueryServer.IPC_VER;
    }
}
