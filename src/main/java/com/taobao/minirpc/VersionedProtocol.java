package com.taobao.minirpc;

import java.io.IOException;

/**
 * Created by shili on 14-2-27.
 */
public interface VersionedProtocol {

    /**
     * 返回协议接口相关的协议版本号
     * @param protocol The classname of the protocol interface
     * @param clientVersion The version of the protocol that the client speaks
     * @return the version that the server will speak
     */
    public long getProtocolVersion(String protocol,
                                   long clientVersion) throws IOException;
}
