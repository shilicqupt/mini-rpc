package com.taobao.utils;


import com.taobao.minirpc.Configurable;

/**
 * Created by shili on 14-2-27.
 */
public class Configured implements Configurable {

    private Configuration conf;

    /** Construct a Configured. */
    public Configured() {
        this(null);
    }

    /** Construct a Configured. */
    public Configured(Configuration conf) {
        setConf(conf);
    }

    // inherit javadoc
    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    // inherit javadoc
    public Configuration getConf() {
        return conf;
    }

}
