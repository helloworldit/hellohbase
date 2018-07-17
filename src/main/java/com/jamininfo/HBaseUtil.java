package com.jamininfo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;

/**
 *
 * HBase操作工具，无连接池
 * @author xiongmz
 */
public class HBaseUtil {
	private static final String ZK_HOST = "nd1,nd2,nd3";
	private static final String ZK_PORT = "2181";
	private static Configuration conf = null;
	private static HBaseAdmin hBaseAdmin = null;
	private static HConnection hConnection = null;

	/**
	 * 获取全局唯一的Configuration实例
	 * @return
	 */
	private static synchronized Configuration getConfiguration() {
		if (conf == null) {
			conf = HBaseConfiguration.create();
			conf.set("hbase.zookeeper.quorum", ZK_HOST);
			conf.set("hbase.zookeeper.property.clientPort", ZK_PORT);
		}
		return conf;
	}

	/**
	 * 获取全局唯一的HConnection实例
	 * @return
	 * @throws ZooKeeperConnectionException
	 */
	private static synchronized HConnection getHConnection() {
		if (hConnection == null) {
			try {
				hConnection = HConnectionManager.createConnection(getConfiguration());
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return hConnection;
	}

	/**
	 * 获取全局唯一的HBaseAdmin实例
	 * @return
	 * @throws ZooKeeperConnectionException
	 */
	private static synchronized HBaseAdmin getHBaseAdmin() {
		if (hBaseAdmin == null) {
			try {
				hBaseAdmin = new HBaseAdmin(getConfiguration());
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return hBaseAdmin;
	}

	/**
	 * 关闭HConnection
	 * @throws Exception
	 */
	private static void closeHConnection() throws Exception {
		if (hConnection != null) {
			try {
				hConnection.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * 关闭HBaseAdmin
	 * @throws Exception
	 */
	private static void closeHBaseAdmin() throws Exception {
		if (hBaseAdmin != null) {
			try {
				hBaseAdmin.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * 关闭HBase表连接
	 * @param hTable
	 * @throws Exception
	 */
	private static void closeHTable(HTableInterface hTable) throws Exception {
		if (hTable != null) {
			try {
				hTable.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * 创建表,如表已存在则报错
	 * @param tableDesc
	 * @throws Exception
	 */
	public static void createTbl(HTableDescriptor tableDesc) throws Exception {
		createTbl(tableDesc, false);
	}

	/**
	 * 创建表
	 * @param tableDesc
	 * @param autoDrop
	 * @throws Exception
	 */
	public static void createTbl(HTableDescriptor tableDesc, Boolean autoDrop) throws Exception {
		if (tableDesc == null || tableDesc.getTableName() == null || StringUtils.isBlank(tableDesc.getTableName().toString())) {
			throw new RuntimeException(" Incorrect parameter tableDesc ");
		}
		try {
			getHBaseAdmin();
			TableName tableName = tableDesc.getTableName();
			Boolean tableExists = hBaseAdmin.tableExists(tableName);
			if (tableExists && !autoDrop) {
				throw new RuntimeException(" tableExists ");
			} else if (tableExists && autoDrop) {
				// 删除表之前必须先禁用，否则删除不了
				hBaseAdmin.disableTable(tableName);
				hBaseAdmin.deleteTable(tableName);
			}
			hBaseAdmin.createTable(tableDesc);
		} catch (Exception e) {
			throw new Exception(e);
		} finally {
			closeHBaseAdmin();
		}
	}

	/**
	 * 表中插入数据
	 * @param tableName
	 * @param putList
	 * @throws Exception
	 */
	public static void insert(TableName tableName, List<Put> putList) throws Exception {
		getHConnection();
		HTableInterface hTable = hConnection.getTable(tableName);
		try {
			for (int i = 0; i < putList.size(); i++) {
				hTable.put(putList.get(i));
			}
			hTable.flushCommits();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			closeHTable(hTable);
			closeHConnection();
		}
	}

	/**
	 * 根据rowkey精准查询，GET方式
	 * @param tableName
	 * @param rowKey
	 * @param columns
	 * @return List<Cell>
	 * @throws Exception
	 */
	public List<Cell> queryGet(TableName tableName, String rowKey, List<Map<String, byte[]>> columns) throws Exception {
		List<Cell> rtn = new ArrayList<>();
		HTableInterface hTable = null;
		try {
			getHConnection();
			hTable = hConnection.getTable(tableName);
			Get get = new Get(rowKey.getBytes());
			// get.addColumn 用于指定查询的列，默认是查询全部列，基于性能考虑，一般都建议指定列来查询
			for (int i = 0; i < columns.size(); i++) {
				get.addColumn(columns.get(i).get("family"), columns.get(i).get("qualifier"));
			}
			Result rs = hTable.get(get);
			for (int i = 0; i < columns.size(); i++) {
				rtn.add(rs.getColumnLatestCell(columns.get(i).get("family"), columns.get(i).get("qualifier")));
			}
		} catch (Exception e) {
			throw new Exception(e);
		} finally {
			closeHTable(hTable);
			closeHConnection();
		}
		return rtn;
	}

	/**
	 * 范围查询，Scan方式
	 * @param tableName
	 * @param scan
	 * @param columns
	 * @return List<List<Cell>>
	 * @throws Exception
	 */
	public List<List<Cell>> queryScan(TableName tableName, Scan scan, List<Map<String, byte[]>> columns) throws Exception {
		HTableInterface hTable = null;
		List<List<Cell>> rtn = new ArrayList<>();
		try {
			getHConnection();
			hTable = hConnection.getTable(tableName);
			ResultScanner resultScanner = hTable.getScanner(scan);

			for (Result rs : resultScanner) {
				List<Cell> cells = new ArrayList<>();
				for (int i = 0; i < columns.size(); i++) {
					cells.add(rs.getColumnLatestCell(columns.get(i).get("family"), columns.get(i).get("qualifier")));
				}
				rtn.add(cells);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			closeHTable(hTable);
			closeHConnection();
		}
		return rtn;
	}

	public static void main(String[] args) {
		HBaseUtil hBaseUtil = new HBaseUtil();
		try {
			System.out.println("开始Main执行");

			hBaseUtil.demoQueryScanWithFilter();

			System.out.println("结束Main执行");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 示例代码：创建表
	 * @throws Exception
	 */
	public void demoCreateTbl() throws Exception {
		TableName tableName = TableName.valueOf("xmztest");
		HTableDescriptor desc = new HTableDescriptor(tableName);
		HColumnDescriptor family = new HColumnDescriptor("cf1");
		family.setBlockCacheEnabled(true);
		family.setInMemory(true);
		// 最大版本数，1代表只有最新的一条数据，历史版本的数据在合并时会清除掉
		family.setMaxVersions(1);
		desc.addFamily(family);

		createTbl(desc, true);
	}

	/**
	 * 示例代码：表写入数据
	 * @throws Exception
	 */
	public void demoInsert() throws Exception {
		TableName tableName = TableName.valueOf("xmztest");
		List<Put> putList = new ArrayList<>();
		// hbase中rowkey是按字典排序升序排的
		// ***实现降序的方案  rowKey = key + (Long.MAX_VALUE - dateLong);
		// 根据业务查询需要设置数据的组合（手机号_时间戳），便于查询
		String rowKey = "18612341234_20170518181818";
		Put put = new Put(rowKey.getBytes());
		// 主叫和被叫类型
		put.add("cf1".getBytes(), "type".getBytes(), "1".getBytes());
		// 通话时长
		put.add("cf1".getBytes(), "time".getBytes(), "15".getBytes());
		// 对方手机号
		put.add("cf1".getBytes(), "targetPhoneNo".getBytes(), "18712341111".getBytes());
		putList.add(put);

		rowKey = "18612345678_20170618181818";
		put = new Put(rowKey.getBytes());
		put.add("cf1".getBytes(), "type".getBytes(), "1".getBytes());
		put.add("cf1".getBytes(), "time".getBytes(), "20".getBytes());
		put.add("cf1".getBytes(), "targetPhoneNo".getBytes(), "18712342222".getBytes());
		putList.add(put);

		insert(tableName, putList);
	}

	/**
	 * 示例代码：精准查询Get
	 * @throws Exception
	 */
	public void demoQueryGet() throws Exception {
		TableName tableName = TableName.valueOf("xmztest");
		// 精准查询，按rowKey
		String rowKey = "18612345678_20170618181818";
		List<Map<String, byte[]>> columns = new ArrayList<>();
		Map<String, byte[]> column1 = new HashMap<>();
		column1.put("family", "cf1".getBytes());
		column1.put("qualifier", "type".getBytes());
		columns.add(column1);

		Map<String, byte[]> column2 = new HashMap<>();
		column2.put("family", "cf1".getBytes());
		column2.put("qualifier", "time".getBytes());
		columns.add(column2);

		List<Cell> cells = queryGet(tableName, rowKey, columns);
		for (int i = 0; i < cells.size(); i++) {
			System.out.println(new String(columns.get(i).get("qualifier")) + ":" + new String(CellUtil.cloneValue(cells.get(i))));
		}
	}

	/**
	 * 示例代码：范围查询Scan
	 * @throws Exception
	 */
	public void demoQueryScan() throws Exception {
		TableName tableName = TableName.valueOf("xmztest");
		Scan scan = new Scan();
		String startRow = "18612341234_20170518181818";
		scan.setStartRow(startRow.getBytes());
		// stopRow 匹配数据时是< 不是<=，因此要注意对最后一行的值要相对加1点
		String stopRow = "18612345678_20170618181819";
		scan.setStopRow(stopRow.getBytes());

		List<Map<String, byte[]>> columns = new ArrayList<>();
		Map<String, byte[]> column1 = new HashMap<>();
		column1.put("family", "cf1".getBytes());
		column1.put("qualifier", "type".getBytes());
		columns.add(column1);

		Map<String, byte[]> column2 = new HashMap<>();
		column2.put("family", "cf1".getBytes());
		column2.put("qualifier", "time".getBytes());
		columns.add(column2);

		Map<String, byte[]> column3 = new HashMap<>();
		column3.put("family", "cf1".getBytes());
		column3.put("qualifier", "targetPhoneNo".getBytes());
		columns.add(column3);

		List<List<Cell>> cellList = queryScan(tableName, scan, columns);
		for (int i = 0; i < cellList.size(); i++) {
			List<Cell> cells = cellList.get(i);
			for (int j = 0; j < cells.size(); j++) {
				System.out.println(new String(columns.get(j).get("qualifier")) + ":" + new String(CellUtil.cloneValue(cells.get(j))));
			}
		}
	}

	/**
	 * 示例代码：范围查询，且增加匹配条件
	 * @throws Exception
	 */
	public void demoQueryScanWithFilter() throws Exception {
		TableName tableName = TableName.valueOf("xmztest");
		// 以1861234开头的主叫手机通话详单
		Scan scan = new Scan();
		String rowkeyPrefix = "1861234";

		// Filters：
		FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
		// PrefixFilter指rowkey以什么字符开头，不能以非开头字符作为条件
		PrefixFilter prefixFilter = new PrefixFilter(rowkeyPrefix.getBytes());
		// singleColumnValueFilter 指其中的某个列需要匹配某个值 
		SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter("cf1".getBytes(), "targetPhoneNo".getBytes(), CompareFilter.CompareOp.EQUAL,
				"18712341111".getBytes());
		filterList.addFilter(prefixFilter);
		filterList.addFilter(singleColumnValueFilter);
		scan.setFilter(filterList);

		List<Map<String, byte[]>> columns = new ArrayList<>();
		Map<String, byte[]> column1 = new HashMap<>();
		column1.put("family", "cf1".getBytes());
		column1.put("qualifier", "type".getBytes());
		columns.add(column1);

		Map<String, byte[]> column2 = new HashMap<>();
		column2.put("family", "cf1".getBytes());
		column2.put("qualifier", "time".getBytes());
		columns.add(column2);

		Map<String, byte[]> column3 = new HashMap<>();
		column3.put("family", "cf1".getBytes());
		column3.put("qualifier", "targetPhoneNo".getBytes());
		columns.add(column3);

		List<List<Cell>> cellList = queryScan(tableName, scan, columns);
		for (int i = 0; i < cellList.size(); i++) {
			List<Cell> cells = cellList.get(i);
			for (int j = 0; j < cells.size(); j++) {
				System.out.println(new String(columns.get(j).get("qualifier")) + ":" + new String(CellUtil.cloneValue(cells.get(j))));
			}
		}
	}
}
