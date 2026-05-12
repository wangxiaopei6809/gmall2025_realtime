package com.atguigu.gmall.realtime.dim.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.base.BaseApp;
import com.atguigu.gmall.realtime.common.bean.TableProcessDim;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.FlinkSourceUtil;
import com.atguigu.gmall.realtime.common.util.HBaseUtil;
import com.atguigu.gmall.realtime.dim.function.BroadCastFunction;
import com.atguigu.gmall.realtime.dim.function.HBaseSinkFunction;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;

import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Connection;


public class DimApp extends BaseApp {
    public static void main(String[] args) {
        new DimApp().start(34005,3,"dim_app",Constant.TOPIC_DB);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStream) {

        //TODO 清洗Kafka业务数据
        SingleOutputStreamOperator<JSONObject> jsonobjDs = getJsonobjDs(kafkaStream);

        //TODO 使用FlinkCDC 读取 MySQL 配置表中的配置信息
        SingleOutputStreamOperator<TableProcessDim> tpDS = getTableProcess(env);

        //TODO 根据配置表中的配置信息构建 HBase 中执行建表或者删除表操作
        tpDS = createHbaseTable(tpDS);

        //TODO 8.将配置流中的配置信息进行广播 - broadcast
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> dimDS = connectDS(jsonobjDs, tpDS);

        dimDS.print();
        //TODO 11. 将维度数据同步到Hbase表中
        dimDS.addSink(new HBaseSinkFunction());
    }

    private static SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> connectDS(SingleOutputStreamOperator<JSONObject> jsonobjDs, SingleOutputStreamOperator<TableProcessDim> tpDS) {
        MapStateDescriptor<String, TableProcessDim> mapStateDescriptor = new MapStateDescriptor<String, TableProcessDim>("MapStateDescriptor", String.class, TableProcessDim.class);
        BroadcastStream<TableProcessDim> broadcastDS = tpDS.broadcast(mapStateDescriptor);

        //TODO 9.将主流的业务数据和广播流中的配置信息进行关联 -connect
        BroadcastConnectedStream<JSONObject, TableProcessDim> connectDS = jsonobjDs.connect(broadcastDS);

        //TODO 10. 处理关联后的数据（判断是否为维度）
        //processElement: 处理主流业务数据
        //processBroadcastElement: 处理广播流配置信息
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> dimDS = connectDS.process(new BroadCastFunction(mapStateDescriptor));
        return dimDS;
    }

    private static SingleOutputStreamOperator<TableProcessDim> createHbaseTable(SingleOutputStreamOperator<TableProcessDim> tpDS) {
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
        return tpDs;
    }

    private static SingleOutputStreamOperator<TableProcessDim> getTableProcess(StreamExecutionEnvironment env) {
        //TODO 5.使用FlinkCDC 读取配置表中的配置信息
        //5.1 创建MySQLSource对象
        //5.2 读取数据封装为流
        MySqlSource<String> mySqlSource = FlinkSourceUtil.getMysqlSource("gmall_config","table_process_dim");

        DataStreamSource<String> mysqlStream = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mysqlsource").setParallelism(1);

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
        return tpDS;
    }

    private static SingleOutputStreamOperator<JSONObject> getJsonobjDs(DataStreamSource<String> kafkaStream) {

        return kafkaStream.process(new ProcessFunction<String, JSONObject>() {
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
    }
}
