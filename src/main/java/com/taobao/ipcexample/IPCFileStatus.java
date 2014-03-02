package com.taobao.ipcexample;

import com.taobao.minirpc.Writable;
import com.taobao.utils.WritableFactories;
import com.taobao.utils.WritableFactory;
import org.apache.hadoop.io.Text;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Date;

/**
 * Created by shili on 14-2-28.
 */
public class IPCFileStatus implements Writable {
    private String filename;
    private long time;

    static {   // register IPCFileStatus
        WritableFactories.setFactory
                (IPCFileStatus.class,
                        new WritableFactory() {
                            public Writable newInstance() {
                                return new IPCFileStatus();
                            }
                        });
    }

    public IPCFileStatus() {
    }

    public IPCFileStatus(String filename) {
        this.filename=filename;
        this.time=(new Date()).getTime();
    }

    public String getFilename() {
        return filename;
    }

    public void setFilename(String filename) {
        this.filename = filename;
    }

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }

    public String toString() {
        return "File: "+filename+" Create at "+(new Date(time));
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.filename = Text.readString(in);
        this.time = in.readLong();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, filename);
        out.writeLong(time);
    }
}
