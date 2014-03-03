package com.taobao.config;

import com.netflix.curator.RetryPolicy;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.retry.ExponentialBackoffRetry;

import java.net.InetSocketAddress;

/**
 * Created by shili on 14-3-3.
 */
public class ConfigServer {

    public ConfigServer(){
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        this.client = CuratorFrameworkFactory.newClient("10.74.156.106", retryPolicy);
    }

    public void start(){
        this.client.start();
    }

    public void stop(){
        this.client.close();
    }

//    public InetSocketAddress get(String serviceName) throws Exception {
//        String path = "/mini-rpc/" + serviceName;
//        client.getData().forPath(path);
//    }

    private CuratorFramework client;
}
