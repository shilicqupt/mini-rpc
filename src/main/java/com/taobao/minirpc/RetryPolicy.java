package com.taobao.minirpc;

/**
 * Created by shili on 14-2-27.
 */
public interface RetryPolicy {

    public boolean shouldRetry(Exception e, int retries) throws Exception;
}