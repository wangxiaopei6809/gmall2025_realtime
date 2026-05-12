package com.atguigu.gmall.realtime.dim.test;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.bean.TableProcessDim;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.HBaseUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Connection;

import java.io.IOException;
import java.util.*;

public class demo2 {
    public static void main(String[] args) throws Exception {


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
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(30), Time.seconds(3)));
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://ns1/checkpoint");
//        env.getCheckpointConfig().setCheckpointStorage(new HadoopFileSystem(conf, new org.apache.hadoop.fs.Path("hdfs://ns1/checkpoint")));
        System.setProperty("HADOOP_USER_NAME", "root");


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
                        if (message != null) {
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

        //TODO 5.使用FlinkCDC 读取配置表中的配置信息
        //5.1 创建MySQLSource对象
        //5.2 读取数据封装为流

        Properties props = new Properties();
        props.setProperty("useSSL", "false");
        props.setProperty("allowPublicKeyRetrieval", "true");

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
                .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mysqlsource")
                .setParallelism(1);

        //TODO 6.对配置流中的数据类型进行转换 jsonStr -> jsonObj
        SingleOutputStreamOperator<TableProcessDim> tpDS = mysqlStream.map(new MapFunction<String, TableProcessDim>() {
            @Override
            public TableProcessDim map(String s) throws Exception {
                JSONObject jsonObject = JSON.parseObject(s);
                TableProcessDim table = null;
                String op = jsonObject.getString("op");
                if ("d".equals(op)) {
                    table = jsonObject.getObject("before", TableProcessDim.class);
                } else {
                    table = jsonObject.getObject("after", TableProcessDim.class);
                }
                table.setOp(op);
                return table;
            }
        }).setParallelism(1);

        //TODO 7.根据配置表中的配置信息构建 HBase 中执行建表或者删除表操作
        SingleOutputStreamOperator<TableProcessDim> tpDs = tpDS.map(new RichMapFunction<TableProcessDim, TableProcessDim>() {
            Connection connection = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                connection = HBaseUtil.getHBaseConnection();
            }

            @Override
            public void close() throws Exception {
                HBaseUtil.closeHBaseConnection(connection);
            }

            @Override
            public TableProcessDim map(TableProcessDim tp) throws Exception {
                String op = tp.getOp();
                String sinkTable = tp.getSinkTable();
                String[] sinkFamilys = tp.getSinkFamily().split(",");
                if ("d".equals(op)) {
                    HBaseUtil.dropHBaseTable(connection, Constant.HBASE_NAMESPACE, sinkTable);
                } else if ("c".equals(op) || "r".equals(op)) {
                    HBaseUtil.createHBaseTable(connection, Constant.HBASE_NAMESPACE, sinkTable, sinkFamilys);
                } else {
                    HBaseUtil.dropHBaseTable(connection, Constant.HBASE_NAMESPACE, sinkTable);
                    HBaseUtil.createHBaseTable(connection, Constant.HBASE_NAMESPACE, sinkTable, sinkFamilys);
                }
                return tp;
            }
        });

        //TODO 8.将配置流中的配置信息进行广播 - broadcast
        MapStateDescriptor<String, TableProcessDim> mapStateDescriptor = new MapStateDescriptor<String, TableProcessDim>("MapStateDescriptor", String.class, TableProcessDim.class);
        BroadcastStream<TableProcessDim> broadcastDS = tpDs.broadcast(mapStateDescriptor);

        //TODO 9.将主流的业务数据和广播流中的配置信息进行关联 -connect
        BroadcastConnectedStream<JSONObject, TableProcessDim> connectDS = jsonobjDs.connect(broadcastDS);

        //TODO 10. 处理关联后的数据（判断是否为维度）
        //processElement: 处理主流业务数据
        //processBroadcastElement: 处理广播流配置信息
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> dimDS = connectDS.process(new BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>>() {
            @Override
            public void processElement(JSONObject value, BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>>.ReadOnlyContext ctx, Collector<Tuple2<JSONObject, TableProcessDim>> out) throws Exception {
                String table = value.getString("table");
                ReadOnlyBroadcastState<String, TableProcessDim> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
                TableProcessDim tableProcessDim = broadcastState.get(table);
                if (tableProcessDim != null) {
                    JSONObject data = value.getJSONObject("data");
                    List<String> columnList = Arrays.asList(tableProcessDim.getSinkColumns().split(","));
                    Set<Map.Entry<String, Object>> entrySet = data.entrySet();
                    entrySet.removeIf(entry-> !columnList.contains(entry.getKey()));
                    data.put("type", value.getString("type"));
                    out.collect(Tuple2.of(data, tableProcessDim));
                }
            }

            @Override
            public void processBroadcastElement(TableProcessDim tp, BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>>.Context ctx, Collector<Tuple2<JSONObject, TableProcessDim>> out) throws Exception {
                String op = tp.getOp();
                String sourceTable = tp.getSourceTable();
                BroadcastState<String, TableProcessDim> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
                if ("d".equals(op)) {
                    broadcastState.remove(sourceTable);
                } else {
                    broadcastState.put(sourceTable, tp);
                }
            }
        });
        dimDS.print();
        //TODO 11. 将维度数据同步到Hbase表中
        DataStreamSink<Tuple2<JSONObject, TableProcessDim>> hBaseStreamSink = dimDS.addSink(new RichSinkFunction<Tuple2<JSONObject, TableProcessDim>>() {
            Connection connection = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                connection = HBaseUtil.getHBaseConnection();
            }

            @Override
            public void close() throws Exception {
                HBaseUtil.closeHBaseConnection(connection);
            }

            @Override
            public void invoke(Tuple2<JSONObject, TableProcessDim> value) throws Exception {
                JSONObject jsonObject = value.f0;
                TableProcessDim tableProcessDim = value.f1;
                String type = jsonObject.getString("type");

                String RowKey = jsonObject.getString(tableProcessDim.getSinkRowKey());
                jsonObject.remove("type");
                if ("delete".equals(type)) {
                    HBaseUtil.delRow(connection, Constant.HBASE_NAMESPACE, tableProcessDim.getSinkTable(), RowKey);
                } else {
                    HBaseUtil.putRow(connection, Constant.HBASE_NAMESPACE, tableProcessDim.getSinkTable(), RowKey, tableProcessDim.getSinkFamily(), jsonObject);
                }
            }
        });

        env.execute();
    }
}
