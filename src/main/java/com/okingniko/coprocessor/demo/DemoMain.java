package com.okingniko.coprocessor.demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static com.okingniko.coprocessor.demo.GetRowCount.GetRowCountService.newBlockingStub;

public class DemoMain {
    // 根据表名创建HBase table, 默认单列族(c1), 并添加demo中的Observer和endpoint
    void createTable(String tableName) {
        try {
            Configuration config = HBaseConfiguration.create();
            Connection connection = ConnectionFactory.createConnection(config);
            Admin admin = connection.getAdmin();
            TableName tName = TableName.valueOf(tableName);
            HTableDescriptor tableDesc = new HTableDescriptor(tName);
            if (admin.tableExists(tName) == true) {
                admin.disableTable(tName);
                admin.deleteTable(tName);
            }
            tableDesc.addFamily(new HColumnDescriptor("c1")); //add column family
            tableDesc.addCoprocessor("com.okingniko.coprocessor.demo.GetRowCountObserver");
            //tableDesc.addCoprocessor("com.okingniko.coprocessor.demo.GetRowCountEndpoint");
            admin.createTable(tableDesc);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // 产生1000 - rowCount 行测试数据, 列族为c1, qualifier为qual1
    void populateTestRows(String tableName, int rowCount) {
        try {
            Configuration config = HBaseConfiguration.create();
            Connection connection = ConnectionFactory.createConnection(config);
            TableName tName = TableName.valueOf(tableName);
            Table tbl = connection.getTable(tName);
            //insert 1000
            for (int i = 0; i < 1000; i++) {
                String rowkey = "row" + Integer.toString(i);
                Put put = new Put(rowkey.getBytes());
                put.addColumn("c1".getBytes(), "qual1".getBytes(), ("zhuoran-test" + i).getBytes());
                tbl.put(put);
            }
            for (int i = 0; i < 1000 - rowCount; i++) {
                String rowkey = "row" + Integer.toString(i);
                Delete d = new Delete(rowkey.getBytes());
                tbl.delete(d);
            }

            tbl.close();
            connection.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    // 获取单个Region的rowCount
    long singleRegionCount(String tableName, String rowkey, boolean reCount) {
        long rowCount = 0;
        try {
            Configuration config = HBaseConfiguration.create();
            Connection connection = ConnectionFactory.createConnection(config);
            TableName tName = TableName.valueOf(tableName);
            Table tbl = connection.getTable(tName);

            // 获取rpc channel
            CoprocessorRpcChannel channel = tbl.coprocessorService(rowkey.getBytes());
            GetRowCount.GetRowCountService.BlockingInterface service = newBlockingStub(channel);

            // 设置RPC入口参数
            GetRowCount.GetRowCountRequest.Builder request = GetRowCount.GetRowCountRequest.newBuilder();
            request.setReCount(reCount);

            // 调用RPC
            GetRowCount.GetRowCountResponse ret = service.getRowCount(null, request.build());

            // 解析返回结果
            rowCount = ret.getRowCount();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return rowCount;
    }

    public static void main(String[] args) {
        System.out.println("Hello HBase Coprocessor Demo!");
        int argSize = args.length;
        System.out.println("input " + argSize + " arguments");
        if (argSize < 3) return;
        String tblName = args[0];
        boolean fastMethod = (Integer.parseInt(args[1]) == 1);
        int num = Integer.parseInt(args[2]);
        String userRowKey = "row900"; //default one
        if (argSize == 4) {
            userRowKey = args[3];
        }

        DemoMain demo = new DemoMain();
        System.out.println("create table " + tblName);
        demo.createTable(tblName);
        demo.populateTestRows(tblName, num);

        long singleCount = demo.singleRegionCount(tblName, userRowKey, fastMethod);
        System.out.println("Get single count: " + singleCount);

        System.out.println("bye!");
    }
}
