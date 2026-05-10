package com.atguigu.gmall.realtime.dim.test;

public class Demo {
    public static void main(String[] args) {

        //TODO 1.基本环境准备
        //1.1 制定流处理环境
        // 1.2 设置并行度

        //TODO 2.检查点相关的设置
        //2.1 开启检查点
        //2.2 设置检查点超时时间
        //2.3 设置job取消后检查点是否保留
        //2.4 设置两个检查点之间最小时间间隔
        //2.5 设置重启策略
        //2.6 设置状态后端以及检查点存储路径
        //2.7 设置操作Hadoop的用户

        //TODO 3.从Kafka的topic_db主题中读取业务数据
        //3.1 声明消费的主题以及消费者组
        //3.2 创建消费者对象
        //3.3 消费数据封装为流

        //TODO 4.对业务流中数据类型进行转换 jsonStr -> jsonObj

        //TODO 5.使用FlinkCDC 读取配置表中的配置信息
        //5.1 创建MySQLSource对象
        //5.2 读取数据封装为流

        //TODO 6.对配置流中的数据类型进行转换 jsonStr -> jsonObj

        //TODO 7.根据配置表中的配置信息构建 HBase 中执行建表或者删除表操作

        //TODO 8.将配置流中的配置信息进行广播 - broadcast

        //TODO 9.将主流的业务数据和广播流中的配置信息进行关联 -connect

        //TODO 10. 处理关联后的数据（判断是否为维度）
        //processElement: 处理主流业务数据
        //processBroadcastElement: 处理广播流配置信息

        //TODO 11. 将维度数据同步到Hbase表中

    }
}
