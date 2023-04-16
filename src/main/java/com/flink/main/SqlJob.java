package com.flink.main;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author kai
 * @date 2023-04-16 14:13
 */
public class SqlJob {
    private static Logger LOG = LoggerFactory.getLogger(SqlJob.class);
    private static final String JOB_NAME = "SqlJob";

    public static void main(String[] args) {
        LOG.info("SqlJob...");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tabEnv = StreamTableEnvironment.create(env);
        tabEnv.getConfig().getConfiguration().setString("pipeline.name", "SqlJob");

        tabEnv.executeSql("CREATE TABLE source_table ( src_id STRING, src_num BIGINT ) \n" +
                "  with ( 'connector' = 'datagen' \n" +
                "    , 'rows-per-second' = '2' \n" +
                "    , 'fields.src_id.length' = '2' \n" +
                "    , 'fields.src_num.min' = '1' \n" +
                "    , 'fields.src_num.max' = '1000' \n" +
                "  )");

        tabEnv.executeSql("CREATE TABLE sink_table ( des_id STRING, des_cnt BIGINT )\n" +
                "  with('connector' = 'print' \n" +
                "  )");

        tabEnv.executeSql("insert into sink_table select src_id, (src_num + 10000) as cnt from source_table");
    }
}
