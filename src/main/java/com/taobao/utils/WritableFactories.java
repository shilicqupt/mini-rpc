package com.taobao.utils;


import com.taobao.minirpc.Configurable;
import com.taobao.minirpc.Writable;

import java.util.HashMap;

/**
 * Created by shili on 14-2-27.
 */
public class WritableFactories {
    private static final HashMap<Class, WritableFactory> CLASS_TO_FACTORY =
            new HashMap<Class, WritableFactory>();

    private WritableFactories() {}                  // singleton

    /** Define a factory for a class. */
    public static synchronized void setFactory(Class c, WritableFactory factory) {
        CLASS_TO_FACTORY.put(c, factory);
    }

    /** Define a factory for a class. */
    public static synchronized WritableFactory getFactory(Class c) {
        return CLASS_TO_FACTORY.get(c);
    }

    /** Create a new instance of a class with a defined factory. */
    public static Writable newInstance(Class<? extends Writable> c) {
        WritableFactory factory = WritableFactories.getFactory(c);
        if (factory != null) {
            Writable result = (Writable) factory.newInstance();
            return result;
        } else {
            return ReflectionUtils.newInstance(c);
        }
    }
}
