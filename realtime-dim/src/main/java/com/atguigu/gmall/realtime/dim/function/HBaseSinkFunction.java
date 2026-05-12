package com.atguigu.gmall.realtime.dim.function;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.bean.TableProcessDim;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.HBaseUtil;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.client.Connection;

public class HBaseSinkFunction extends RichSinkFunction<Tuple2<JSONObject, TableProcessDim>> {

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
}
