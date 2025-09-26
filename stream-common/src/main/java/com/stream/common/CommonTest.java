package com.stream.common;

import com.stream.common.utils.ConfigUtils;

/**
 * @Package com.stream.common.CommonTest
 * @Author liao.jianjun
 * @Date 2025/09/26 10:41
 * @description: Test
 */
public class CommonTest {

    public static void main(String[] args) {
        String kafka_err_log = ConfigUtils.getString("kafka.err.log");
        System.err.println(kafka_err_log);
    }


}
