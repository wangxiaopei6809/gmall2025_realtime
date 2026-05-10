package com.atguigu.gmall.realtime.dim;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.bean.TableProcessDim;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.util.Properties;

public class DimApp {
    public static void main(String[] args) throws Exception {

//        // 创建 Hadoop Configuration 对象
//        Configuration conf = new Configuration();
//        conf.set("fs.defaultFS", "hdfs://ns1");
//        conf.set("dfs.nameservices", "ns1");
//        conf.set("dfs.ha.namenodes.ns1", "nn1,nn2");
//        conf.set("dfs.namenode.rpc-address.ns1.nn1", "hadoop102:8020");
//        conf.set("dfs.namenode.rpc-address.ns1.nn2", "hadoop103:8020");
//        conf.set("dfs.client.failover.proxy.provider.ns1",
//                "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");


        //TODO 1.基本环境准备
        //1.1 制定流处理环境
        // 1.2 设置并行度
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

        //TODO 2.检查点相关的设置
        //2.1 开启检查点
        //2.2 设置检查点超时时间
        //2.3 设置job取消后检查点是否保留
        //2.4 设置两个检查点之间最小时间间隔
        //2.5 设置重启策略
        //2.6 设置状态后端以及检查点存储路径
        //2.7 设置操作Hadoop的用户
        env.enableCheckpointing(10000l, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000l);
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000l);
//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,3000l));
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(30),Time.seconds(3)));
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://ns1/checkpoint");
//        env.getCheckpointConfig().setCheckpointStorage(new HadoopFileSystem(conf, new org.apache.hadoop.fs.Path("hdfs://ns1/checkpoint")));
        System.setProperty("HADOOP_USER_NAME","root");


        //TODO 3.从Kafka的topic_db主题中读取业务数据
        //3.1 声明消费的主题以及消费者组
        //3.2 创建消费者对象
        //3.3 消费数据封装为流

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(Constant.KAFKA_BROKERS)
                .setTopics(Constant.TOPIC_DB)
                .setGroupId("dim_app_group")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setValueOnlyDeserializer(new DeserializationSchema<String>() {
                    @Override
                    public String deserialize(byte[] message) throws IOException {
                        if(message != null){
                            return new String(message);
                        }
                        return null;
                    }

                    @Override
                    public boolean isEndOfStream(String s) {
                        return false;
                    }

                    @Override
                    public TypeInformation<String> getProducedType() {
                        return TypeInformation.of(String.class);
                    }
                })
                .build();

        DataStreamSource<String> kafkaStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        //TODO 4.对业务流中数据类型进行转换 jsonStr -> jsonObj
        SingleOutputStreamOperator<JSONObject> jsonobjDs = kafkaStream.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String value, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                JSONObject jsonObject = JSON.parseObject(value);
                String db = jsonObject.getString("database");
                String type = jsonObject.getString("type");
                String data = jsonObject.getString("data");
                if ("gmall".equals(db)
                        && ("insert".equals(type)
                        || "update".equals(type)
                        || "delete".equals(type)
                        || "bootstrap-insert".equals(type))
                        && data != null
                        && data.length() > 2) {
                    out.collect(jsonObject);
                }

            }
        });

        Properties props = new Properties();
        props.setProperty("useSSL", "false");
        props.setProperty("allowPublicKeyRetrieval", "true");

        //TODO 5.使用FlinkCDC 读取配置表中的配置信息
        //5.1 创建MySQLSource对象
        //5.2 读取数据封装为流
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname(Constant.MYSQL_HOST)
                .port(Constant.MYSQL_PORT)
                .username(Constant.MYSQL_USER_NAME)
                .password(Constant.MYSQL_PASSWORD)
                .databaseList("gmall_config")
                .tableList("gmall_config.table_process_dim")
                .deserializer(new JsonDebeziumDeserializationSchema())
                .startupOptions(StartupOptions.initial())
                .jdbcProperties(props)
                .build();

        DataStreamSource<String> mysqlStream = env
                .fromSource(mySqlSource,WatermarkStrategy.noWatermarks(),"mysqlsource")
                .setParallelism(1);

        //TODO 6.对配置流中的数据类型进行转换 jsonStr -> jsonObj
        SingleOutputStreamOperator<TableProcessDim> jsonobjDS = mysqlStream.map(new MapFunction<String, TableProcessDim>() {
            @Override
            public TableProcessDim map(String s) throws Exception {
                JSONObject jsonObject = JSON.parseObject(s);
                TableProcessDim table = null;
                String op = jsonObject.getString("op");
                if("d".equals(op)){
                    table = jsonObject.getObject("before", TableProcessDim.class);
                }else {
                    table = jsonObject.getObject("after", TableProcessDim.class);
                }
                table.setOp(op);
                return table;
            }
        }).setParallelism(1);

        //TODO 7.根据配置表中的配置信息构建 HBase 中执行建表或者删除表操作

        //TODO 8.将配置流中的配置信息进行广播 - broadcast

        //TODO 9.将主流的业务数据和广播流中的配置信息进行关联 -connect

        //TODO 10. 处理关联后的数据（判断是否为维度）
        //processElement: 处理主流业务数据
        //processBroadcastElement: 处理广播流配置信息

        //TODO 11. 将维度数据同步到Hbase表中


        env.execute();
    }
}
