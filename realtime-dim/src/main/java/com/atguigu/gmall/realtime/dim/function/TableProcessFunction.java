package com.atguigu.gmall.realtime.dim.function;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.bean.TableProcessDim;
import com.atguigu.gmall.realtime.common.util.JdbcUtil;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.util.*;

public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>> {
    private MapStateDescriptor<String, TableProcessDim> mapStateDescriptor;
    private Map<String,TableProcessDim> configMap = new HashMap<>();

    public TableProcessFunction(MapStateDescriptor<String, TableProcessDim> mapStateDescriptor) {
        this.mapStateDescriptor = mapStateDescriptor;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        //将配置表中的配置信息预加载到程序configMap中
        Connection mySQLConnection = JdbcUtil.getMySQLConnection();
        List<TableProcessDim> tableProcessDimList = JdbcUtil.queryList(mySQLConnection, "select * from gmall_config.table_process_dim", TableProcessDim.class, true);
        for (TableProcessDim tableProcessDim : tableProcessDimList) {
            configMap.put(tableProcessDim.getSourceTable(),tableProcessDim);
        }
        JdbcUtil.closeMySQLConnection(mySQLConnection);
    }

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
}
