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
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return status;
    }

    @Override
    public long getProtocolVersion(String protocol, long clientVersion) throws IOException {
        System.out.println("protocol: "+protocol);
        System.out.println("clientVersion: "+clientVersion);
        return IPCQueryServer.IPC_VER;
    }
}
