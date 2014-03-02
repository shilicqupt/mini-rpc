package com.taobao.utils;

/**
 * Created by shili on 14-2-27.
 */

import org.apache.hadoop.io.serializer.*;

/**
 * <p>
 * Encapsulates a {@link org.apache.hadoop.io.serializer.Serializer}/{@link org.apache.hadoop.io.serializer.Deserializer} pair.
 * </p>
 * @param <T>
 */
public interface Serialization<T> {

    /**
     * Allows clients to test whether this {@link Serialization}
     * supports the given class.
     */
    boolean accept(Class<?> c);

    /**
     * @return a {@link org.apache.hadoop.io.serializer.Serializer} for the given class.
     */
    org.apache.hadoop.io.serializer.Serializer<T> getSerializer(Class<T> c);

    /**
     * @return a {@link Deserializer} for the given class.
     */
    Deserializer<T> getDeserializer(Class<T> c);
}
