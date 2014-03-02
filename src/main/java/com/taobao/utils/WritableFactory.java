package com.taobao.utils;

import com.taobao.minirpc.Writable;

/**
 * Created by shili on 14-2-27.
 */
public interface WritableFactory {
    /** Return a new instance. */
    Writable newInstance();
}