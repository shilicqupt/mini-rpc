package com.taobao.ipcexample;

import com.taobao.minirpc.VersionedProtocol;

/**
 * Created by shili on 14-2-28.
 */
public interface IPCQueryStatus extends VersionedProtocol {
    IPCFileStatus getFileStatus(String filename);
}
