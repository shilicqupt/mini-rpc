package com.taobao.minirpc;

/**
 * Created by shili on 14-2-27.
 */
enum Status {
    SUCCESS (0),
    ERROR (1),
    FATAL (-1);

    int state;
    private Status(int state) {
        this.state = state;
    }
}
