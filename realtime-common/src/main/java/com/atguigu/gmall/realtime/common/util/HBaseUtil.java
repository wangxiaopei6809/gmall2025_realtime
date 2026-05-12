package com.atguigu.gmall.realtime.common.util;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;
import java.util.Set;

public class HBaseUtil {

    public static Connection getHBaseConnection() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", Constant.ZOOKEEPER_QUORUM);

        return ConnectionFactory.createConnection(conf);
    }

    public static void closeHBaseConnection(Connection connection) throws IOException {
        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
    }

    public static void createNamespace(Connection connection, String namespace) throws IOException {
        Admin admin = connection.getAdmin();
        try {
            NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(namespace).build();
            admin.createNamespace(namespaceDescriptor);
        } catch (Exception ignored) {
        } finally {
            admin.close();
        }
    }

    public static boolean tableExists(Connection connection, String namespace, String tableName) throws IOException {
        Admin admin = connection.getAdmin();
        try {
            return admin.tableExists(TableName.valueOf(namespace, tableName));
        } finally {
            admin.close();
        }
    }

    public static void createHBaseTable(Connection connection, String namespace, String tableName, String... columeFailies) {
        if (columeFailies == null || columeFailies.length == 0) {
            throw new IllegalArgumentException("At least one column family is required");
        }
        try (Admin admin = connection.getAdmin()) {
            TableName tableNameObj = TableName.valueOf(namespace, tableName);
            if (admin.tableExists(tableNameObj)) {
//                System.out.println("表空间" + namespace + "下的表" + tableName + "已创建");
                return;
            }
            TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableNameObj);
            for (String column :columeFailies){
                ColumnFamilyDescriptor familyDescriptor = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(column)).build();
                tableDescriptorBuilder.setColumnFamily(familyDescriptor);
            }
            admin.createTable(tableDescriptorBuilder.build());
            System.out.println("表空间" + namespace + "下的表" + tableName + "创建成功");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    public static void dropHBaseTable(Connection connection, String namespace, String tableName) throws IOException {

        try(Admin admin = connection.getAdmin()) {
            TableName hBaseTableName = TableName.valueOf(namespace, tableName);
            if (!admin.tableExists(hBaseTableName)) {
                System.out.println("要删除的表空间" + namespace + "下的表" + tableName + "不存在");
                return;
            }
            admin.disableTable(hBaseTableName);
            admin.deleteTable(hBaseTableName);
            System.out.println("表空间" + namespace + "下的表" + tableName + "删除成功");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 向表中put数据
     *
     * @param hbaseConn 连接对象
     * @param namespace 表空间
     * @param tableName 表名
     * @param rowKey    rowkey
     * @param family    列族
     * @param jsonObj   要put的数据
     */
    public static void putRow(Connection hbaseConn, String namespace, String tableName, String rowKey, String family, JSONObject jsonObj) {
        TableName tableNameObj = TableName.valueOf(namespace, tableName);
        try (Table table = hbaseConn.getTable(tableNameObj)) {
            Put put = new Put(Bytes.toBytes(rowKey));
            Set<String> columns = jsonObj.keySet();
            for (String column : columns) {
                String value = jsonObj.getString(column);
                if (StringUtils.isNotEmpty(value)) {
                    put.addColumn(Bytes.toBytes(family), Bytes.toBytes(column), Bytes.toBytes(value));
                }
            }
            table.put(put);
            System.out.println("向表空间" + namespace + "下的表" + tableName + "中put数据"+rowKey+"成功");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    //从表中删除数据
    public static void delRow(Connection hbaseConn, String namespace, String tableName, String rowKey) {
        TableName tableNameObj = TableName.valueOf(namespace, tableName);
        try (Table table = hbaseConn.getTable(tableNameObj)) {
            Delete delete = new Delete(Bytes.toBytes(rowKey));
            table.delete(delete);
            System.out.println("从表空间" + namespace + "下的表" + tableName + "中删除数据"+rowKey+"成功");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 根据rowkey从Hbase表中查询一行数据
     * @param hbaseConn             hbase连接对象
     * @param namespace             表空间
     * @param tableName             表名
     * @param rowKey                rowkey
     * @param clz                   将查询的一行数据 封装的类型
     * @param isUnderlineToCamel    是否将下划线转换为驼峰命名
     * @return
     * @param <T>
     */
    public static <T>T getRow(Connection hbaseConn, String namespace, String tableName, String rowKey,Class<T> clz,boolean... isUnderlineToCamel){
        boolean defaultIsUToC = false;  // 默认不执行下划线转驼峰

        if (isUnderlineToCamel.length > 0) {
            defaultIsUToC = isUnderlineToCamel[0];
        }

        TableName tableNameObj = TableName.valueOf(namespace, tableName);
        try (Table table = hbaseConn.getTable(tableNameObj)){
            Get get = new Get(Bytes.toBytes(rowKey));
            Result result = table.get(get);
            List<Cell> cells = result.listCells();
            if(cells != null && cells.size() > 0){
                //定义一个对象，用于封装查询出来的一行数据
                T obj = clz.newInstance();
                for (Cell cell : cells) {
                    String columnName = Bytes.toString(CellUtil.cloneQualifier(cell));
                    String columnValue = Bytes.toString(CellUtil.cloneValue(cell));
                    if(defaultIsUToC){
                        columnName = CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL,columnName);
                    }
                    BeanUtils.setProperty(obj,columnName,columnValue);
                }
                return obj;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return null;
    }

    /**
     * 以异步的方式 从HBase维度表中查询维度数据
     * @param asyncConn     异步操作HBase的连接
     * @param namespace     表空间
     * @param tableName     表名
     * @param rowKey        rowkey
     * @return
     */
    public static JSONObject readDimAsync(AsyncConnection asyncConn,String namespace, String tableName, String rowKey){
        try {
            TableName tableNameObj = TableName.valueOf(namespace, tableName);
            AsyncTable<AdvancedScanResultConsumer> asyncTable = asyncConn.getTable(tableNameObj);
            Get get = new Get(Bytes.toBytes(rowKey));
            Result result = asyncTable.get(get).get();
            List<Cell> cells = result.listCells();
            if(cells != null && cells.size() > 0){
                JSONObject jsonObj = new JSONObject();
                for (Cell cell : cells) {
                    String columnName = Bytes.toString(CellUtil.cloneQualifier(cell));
                    String columnValue = Bytes.toString(CellUtil.cloneValue(cell));
                    jsonObj.put(columnName,columnValue);
                }
                return jsonObj;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return null;
    }


}
