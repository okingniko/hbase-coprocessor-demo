package com.okingniko.coprocessor.demo;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class GetRowCountObserver extends BaseRegionObserver {
    RegionCoprocessorEnvironment env;
    private static final Log LOG = LogFactory.getLog(GetRowCountObserver.class);

    private String zNodePath = "/hbase/coprocessor/demo";
    private ZooKeeperWatcher zkw = null;

    private long myRowCount = 0;
    private boolean initCount = false;

    // @Override method in BaseRegionObserver
    public void start(CoprocessorEnvironment envi) throws IOException {
        env = (RegionCoprocessorEnvironment) envi;
        RegionCoprocessorEnvironment re = (RegionCoprocessorEnvironment)envi;
        RegionServerServices rss = re.getRegionServerServices();
        zNodePath = zNodePath + re.getRegionInfo().getRegionNameAsString();
        zkw = rss.getZooKeeper();
        myRowCount = 0;
        initCount = false;

        try {
            if(ZKUtil.checkExists(zkw, zNodePath) == -1) {
                ZKUtil.createWithParents(zkw, zNodePath);
                System.out.println("DEMO: create znode: " + zNodePath);
            } else {
                System.out.println("DEMO: Oops, znode exist");
            }
        } catch (KeeperException ke) {
            ke.printStackTrace();
        }
    }


    // @Override method in BaseRegionObserver
    public void stop(CoprocessorEnvironment envi) throws IOException {
        // do nothing!
    }

    // @Overide method in RegionObserver
    public void postDelete(ObserverContext<RegionCoprocessorEnvironment> e,
                          Delete delete,
                          WALEdit edit,
                          Durability durability) throws IOException {
        myRowCount--;
        try {
            ZKUtil.setData(zkw, zNodePath, Bytes.toBytes(myRowCount));
        } catch(Exception ee) {
            System.out.println("DEMO: setData exception");
        }
    }


    // @Overide method in RegionObserver
    public void postPut(ObserverContext<RegionCoprocessorEnvironment> e,
                        Put put,
                        WALEdit edit,
                        Durability durability) throws IOException {
        myRowCount++;
        try {
            ZKUtil.setData(zkw, zNodePath, Bytes.toBytes(myRowCount));
        } catch(Exception ee) {
            System.out.println("DEMO: setData exception");
        }
    }

    // 对Region的行数值进行初始化，使用postOpen钩子函数
    // Called after the region is reported as open to the master.
    public void postOpen(ObserverContext<RegionCoprocessorEnvironment> e) {
       System.out.println("DEMO: post open invoked");
       long count = 0;
       try {
           if (!initCount) {
              Scan scan = new Scan();
              InternalScanner scanner = null;
              scanner = env.getRegion().getScanner(scan);
              List<Cell> results = new ArrayList<Cell>();
              boolean hasMore = false;

              do {
                  hasMore = scanner.next(results);
                  if (results.size() > 0)
                      count++;
              } while(hasMore);
           }
           initCount = true;
       } catch(Exception ee) {
           ee.printStackTrace();
       }
    }
}
