package com.taobao.minirpc;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by shili on 14-2-27.
 */
/**
 * TODO:remove hadoop relay org.apache.hadoop.io.Text
 */
class ConnectionHeader implements Writable {
    public static final Log LOG = LogFactory.getLog(ConnectionHeader.class);

    private String protocol;

    public ConnectionHeader() {}

    public ConnectionHeader(String protocol) {
        this.protocol = protocol;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        protocol = Text.readString(in);
        if (protocol.isEmpty()) {
            protocol = null;
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, (protocol == null) ? "" : protocol);
    }

    public String getProtocol() {
        return protocol;
    }

    public String toString() {
        return protocol;
    }
}
