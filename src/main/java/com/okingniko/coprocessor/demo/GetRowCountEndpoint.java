package com.okingniko.coprocessor.demo;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.CoprocessorException;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class GetRowCountEndpoint extends GetRowCount.GetRowCountService
    implements CoprocessorService, Coprocessor {
    private RegionCoprocessorEnvironment env;
    private static final Log LOG = LogFactory.getLog(GetRowCountEndpoint.class);

    // 这两个类成员是后续代码用来操作Zookeeper的，在start()中初始化
    private String zNodePath = "/hbase/coprocessor/demo";
    private ZooKeeperWatcher zkw = null;

    public GetRowCountEndpoint() {

    }

    //返回实现了GetRowCountService接口的(本)对象的引用
    public Service getService() {
        return this;
    }

    //@Override method in Coprocessor
    public void start(CoprocessorEnvironment envi) throws IOException {
        if (envi instanceof RegionCoprocessorEnvironment) {
            this.env = (RegionCoprocessorEnvironment) envi;
            RegionCoprocessorEnvironment re = (RegionCoprocessorEnvironment)envi;
            RegionServerServices rss = re.getRegionServerServices();
            zkw = rss.getZooKeeper();
            zNodePath = zNodePath + re.getRegionInfo().getRegionNameAsString();

            try {
                if(ZKUtil.checkExists(zkw, zNodePath) == -1) {
                    System.out.println("DEMO: create znode: " + zNodePath);
                    ZKUtil.createWithParents(zkw, zNodePath);
                } else {
                    System.out.println("DEMO: Oops, znode exist");
                }
            } catch (KeeperException ke) {
                ke.printStackTrace();
            }
        } else {
            throw new CoprocessorException("Must be loaded on a table region");
        }
    }

    //@Override method in Coprocessor
    public void stop(CoprocessorEnvironment env) throws IOException {
        // do nothing, bro!
    }

    //实现Protobuf中定义的RPC方法，每一个 RPC 函数的参数列表都是固定的，有三个参数。
    // 第一个参数 RpcController 是固定的，所有 RPC 的第一个参数都是它，这是 HBase 的 Protobuf RPC 协议定义的；
    // 第二个参数为 RPC 的入口参数；第三个参数为返回参数。
    // 入口和返回参数分别由对应proto 文件中的(message) GetRowCountRequest 和 GetRowCountResponse 定义。
    public void getRowCount(RpcController controller, GetRowCount.GetRowCountRequest request,
                            RpcCallback<GetRowCount.GetRowCountResponse> done) {
        boolean reCount = request.getReCount();
        long rowCount = 0;
        if (reCount) {
            InternalScanner scanner = null;
            try {
                //Use Scan method to get row count.
                Scan scan = new Scan();
                scanner = env.getRegion().getScanner(scan);
                List<Cell> results = new ArrayList<Cell>();
                boolean hasMore = false;

                do {
                    hasMore = scanner.next(results);
                    rowCount++;
                } while(hasMore);

            } catch (IOException ioe) {
                ioe.printStackTrace();
            } finally {
                if (scanner != null) {
                    try {
                        scanner.close();
                    } catch (IOException ignored) {
                        ignored.printStackTrace();
                    }
                }
            }
        } else {
            // Directly Use Observer coprocessor to get row count.
            try {
                byte[] data = ZKUtil.getData(zkw, zNodePath);
                rowCount = Bytes.toLong(data);
                System.out.println("DEMO: get row count = " + rowCount);
            } catch (Exception e) {
                System.out.println("Exception during zk getData");
            }
        }

        //rpc complete, send response.
        GetRowCount.GetRowCountResponse.Builder responseBuilder = GetRowCount.GetRowCountResponse.newBuilder();
        responseBuilder.setRowCount(rowCount);
        done.run(responseBuilder.build());
    }

}
