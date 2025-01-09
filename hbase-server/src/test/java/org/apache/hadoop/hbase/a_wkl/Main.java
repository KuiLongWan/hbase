package org.apache.hadoop.hbase.a_wkl;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.a_wkl.util.Logger;

import java.io.IOException;

public class Main extends MiniServerBase {

  @Override protected void doTest(HBaseTestingUtility hbaseTestUtil) {

    try (Connection conn = getConnection();
      Table table = hbaseTestUtil.createTable(tableName, cfName);
      Admin admin = conn.getAdmin()) {

      byte[] row = Bytes.toBytes("00000001");
      byte[] qualifier = Bytes.toBytes("name");

      Logger.info(Main.class, "Put Start");
      table.put(createPut(row, qualifier));
      admin.flush(tableName);
      Logger.info(Main.class, "Put End");

      Logger.info(Main.class, "Get Start");
      Get get = new Get(row);
      Result result = table.get(get);

      byte[] resultRow = result.getRow();
      byte[] resultValue = result.getValue(Bytes.toBytes(cfName), qualifier);

      Logger.info(Main.class,
        "Get End, row:" + Bytes.toString(resultRow) + ", value:" + Bytes.toString(resultValue));

    } catch (IOException e) {
      throw new RuntimeException(e);
    }

  }

  private Put createPut(byte[] row, byte[] qualifier) {
    byte[] value = Bytes.toBytes("zhangsan");
    return new Put(row).addColumn(Bytes.toBytes(cfName), qualifier, value);
  }

  public static void main(String[] args) throws Exception {
    Main main = new Main();
    main.run(1, 5000);
  }
}
