package org.apache.hadoop.hbase.a_wkl;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.ipc.BlockingRpcClient;
import org.apache.hadoop.hbase.ipc.SimpleRpcServer;
import org.apache.hadoop.hbase.a_wkl.util.Logger;

import java.io.IOException;

import static org.apache.hadoop.hbase.ipc.RpcClientFactory.CUSTOM_RPC_CLIENT_IMPL_CONF_KEY;
import static org.apache.hadoop.hbase.ipc.RpcServerFactory.CUSTOM_RPC_SERVER_IMPL_CONF_KEY;

public abstract class MiniServerBase {

  public HBaseTestingUtility hbaseTestUtil;
  public TableName tableName = TableName.valueOf("test");
  public String cfName = "cf";

  @SuppressWarnings("deprecation")
  public MiniServerBase() {
    hbaseTestUtil = new HBaseTestingUtility();

    Configuration config = hbaseTestUtil.getConfiguration();

    config.set(CUSTOM_RPC_SERVER_IMPL_CONF_KEY, SimpleRpcServer.class.getName());
    config.set(CUSTOM_RPC_CLIENT_IMPL_CONF_KEY, BlockingRpcClient.class.getName());
    config.setInt(HConstants.HBASE_RPC_TIMEOUT_KEY, 600 * 1000);
  }

  protected abstract void doTest(HBaseTestingUtility hbaseTestUtil);


  public void run(int numSlaves) throws Exception {
    run(numSlaves, 2000);
  }

  public void run(int numSlaves, long sleepTime) throws Exception {
    System.setProperty("hadoop.home.dir", "/home/klw/envs/bigdata/hadoop-2.10.2");

    hbaseTestUtil.startMiniCluster(numSlaves);
    Logger.error(MiniServerBase.class, "MiniCluster 启动成功！");

    doTest(hbaseTestUtil);

    Thread.sleep(sleepTime);

    hbaseTestUtil.shutdownMiniCluster();
    Logger.error(MiniServerBase.class, "MiniCluster 关闭！");
  }

  public Connection getConnection() throws IOException {
    return ConnectionFactory.createConnection(hbaseTestUtil.getConfiguration());
  }

  /**
   * 创建表，并返回表连接
   *
   * @return
   * @throws IOException
   */
  public void createTable(Admin admin) throws IOException {

    if (!admin.tableExists(tableName)) {
      TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableName);
      tableDescriptorBuilder.setColumnFamily(ColumnFamilyDescriptorBuilder.of(cfName));
      admin.createTable(tableDescriptorBuilder.build());
      Logger.info(MiniServerBase.class, "Table created: " + tableName);
    }

  }


}

