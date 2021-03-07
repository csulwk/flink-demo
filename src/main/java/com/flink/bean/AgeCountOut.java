package com.flink.bean;

import com.alibaba.fastjson.JSONObject;

/**
 * @author kai
 * @date 2021-03-07 15:33
 */
public class AgeCountOut {
    private int age;
    private long cnt;

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public long getCnt() {
        return cnt;
    }

    public void setCnt(long cnt) {
        this.cnt = cnt;
    }

    public AgeCountOut() {}

    public AgeCountOut(int age, long cnt) {
        this.age = age;
        this.cnt = cnt;
    }

    @Override
    public String toString() {
        return JSONObject.toJSONString(this);
    }
}
