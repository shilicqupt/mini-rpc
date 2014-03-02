package com.taobao.minirpc;

import com.taobao.utils.Configuration;

/**
 * Created by shili on 14-2-27.
 */
public interface Configurable {

    /** Set the configuration to be used by this object. */
    void setConf(Configuration conf);

    /** Return the configuration used by this object. */
    Configuration getConf();
}
