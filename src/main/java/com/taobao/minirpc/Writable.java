package com.taobao.minirpc;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by shili on 14-2-27.
 */


public interface Writable {

    void write(DataOutput out) throws IOException;

    void readFields(DataInput in) throws IOException;
}