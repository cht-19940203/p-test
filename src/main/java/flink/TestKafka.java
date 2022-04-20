package flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class TestKafka {


    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.executeSql("create table user_log(" +
                "user_id BIGINT," +
                "item_id BIGINT," +
                "category_id BIGINT," +
                "behavior varchar," +
                "ts varchar)" +
                "with(" +
                "'connector'='kafka'," +
                "'topic'='flink-test'," +
                "'properties.group.id' = 'testGroup_1'," +
                "'properties.bootstrap.servers'='10.88.102.94:9092'," +
                "'scan.startup.mode'='earliest-offset'," +
                "'format'='json')");
        tableEnv.executeSql("CREATE TABLE user_log_mysql (" +
                "    user_id BIGINT," +
                "    item_id BIGINT," +
                "    category_id BIGINT," +
                "    behavior varchar," +
                "    ts varchar" +
                " ) WITH (" +
                "'connector.type' = 'jdbc', " +
                "'connector.url' = 'jdbc:mysql://10.88.102.94:3306/test'," +
                "'connector.table' = 'user_log_mysql', " +
                "'connector.username' = 'hive', " +
                "'connector.password' = 'YNiGxJXaodSf3o2m', " +
                "'connector.write.flush.max-rows' = '1' " +
                " )");

        tableEnv.executeSql("INSERT INTO user_log_mysql select *from  user_log");
    }

}
