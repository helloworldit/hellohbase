package cn.xiongmz.hbase;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class HBaseDemo {

	HBaseAdmin hBaseAdmin;
	String tableName = "phone";
	HTable hTable;
	Random r = new Random();

	@Before
	public void begin() throws Exception {
		Configuration conf = new Configuration();
		conf.set("hbase.zookeeper.quorum", "nd1,nd2,nd3");// 伪分布式就写一个，完全分布式就写多个值。放在配置文件中
		hBaseAdmin = new HBaseAdmin(conf);

		hTable = new HTable(conf, tableName);
	}

	@After
	public void end() {
		// 像JDBC连接一样最后都需要关闭
		if (hBaseAdmin != null) {
			try {
				hBaseAdmin.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		if (hTable != null) {
			try {
				hTable.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * 创建表
	 */
	@Test
	public void createTbl() throws Exception {
		if (hBaseAdmin.tableExists(tableName)) {
			hBaseAdmin.disableTable(tableName);// 删除表之前必须先禁用，否则删除不了
			hBaseAdmin.deleteTable(tableName);
		}
		HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(tableName));
		HColumnDescriptor family = new HColumnDescriptor("cf1");
		family.setBlockCacheEnabled(true);
		family.setInMemory(true);
		family.setMaxVersions(1);// 最大版本数，1代表只有最新的一条数据，历史版本的数据在合并时会清除掉
		desc.addFamily(family);
		hBaseAdmin.createTable(desc);
	}

	@Test
	public void insert() throws Exception {
		// hbase中rowkey是按字典排序升序排的
		String rowKey = "18612341234_20170518181818";// 根据业务查询需要设置数据的组合（手机号_时间戳），便于查询
		Put put = new Put(rowKey.getBytes());
		put.add("cf1".getBytes(), "type".getBytes(), "1".getBytes());// 主叫和被叫类型
		put.add("cf1".getBytes(), "time".getBytes(), "15".getBytes());// 通话时长
		put.add("cf1".getBytes(), "targetPhoneNo".getBytes(), "18712341234".getBytes());// 对方手机号
		hTable.put(put);
	}

	/**
	 * 插入10个手机号，每个100条记录 目标：按时间降序存
	 * 
	 * @throws Exception
	 */
	@Test
	public void insert2() throws Exception {
		List<Put> puts = new ArrayList<>();
		for (int i = 0; i < 10; i++) {
			String rowKey = "";
			String phoneNo = getPhoneNo("186");
			for (int j = 0; j < 100; j++) {
				String phoneDate = getDate("2017");
				SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
				Long dateLong = sdf.parse(phoneDate).getTime();
				rowKey = phoneNo + (Long.MAX_VALUE - dateLong);// ***实现降序
				System.out.println("rowKey:" + rowKey);
				Put put = new Put(rowKey.getBytes());
				put.add("cf1".getBytes(), "type".getBytes(), (r.nextInt(2) + "").getBytes());
				put.add("cf1".getBytes(), "time".getBytes(), (phoneDate).getBytes());
				put.add("cf1".getBytes(), "targetPhoneNo".getBytes(), (getPhoneNo("187")).getBytes());
				puts.add(put);
			}
		}
		hTable.put(puts);
	}

	public String getPhoneNo(String prefix) {
		return prefix + String.format("%08d", r.nextInt(99999999));

	}

	public String getDate(String year) {
		return year + String.format("%02d%02d%02d%02d%02d", new Object[] { r.nextInt(12) + 1, r.nextInt(30) + 1, r.nextInt(24), r.nextInt(60), r.nextInt(60) });
	}

	// 根据rowkey精准查询，GET方式
	@Test
	public void queryGet() throws Exception {
		Get get = new Get("18612341234_20170518181818".getBytes());
		// get.addColumn 用于指定查询的列，默认是查询全部列，基于性能考虑，一般都建议指定列来查询
		get.addColumn("cf1".getBytes(), "type".getBytes());
		get.addColumn("cf1".getBytes(), "time".getBytes());
		Result rs = hTable.get(get);
		Cell cell = rs.getColumnLatestCell("cf1".getBytes(), "type".getBytes());
		System.out.println("type:" + new String(CellUtil.cloneValue(cell)));
		cell = rs.getColumnLatestCell("cf1".getBytes(), "time".getBytes());
		System.out.println("time:" + new String(CellUtil.cloneValue(cell)));
	}

	/**
	 * 查询某个手机号，某个月份的详单 使用scan，不能用get了
	 * 
	 * @throws Exception
	 */
	@Test
	public void queryScan() throws Exception {
		// 18725819633 2017年06月的详单
		Scan scan = new Scan();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
		String startRow = "18725819633" + (Long.MAX_VALUE - sdf.parse("20170631235959").getTime());
		scan.setStartRow(startRow.getBytes());
		String stopRow = "18725819633" + (Long.MAX_VALUE - sdf.parse("20170601000000").getTime());
		scan.setStopRow(stopRow.getBytes());
		ResultScanner resultScanner = hTable.getScanner(scan);
		Cell cell;
		for (Result rs : resultScanner) {
			cell = rs.getColumnLatestCell("cf1".getBytes(), "type".getBytes());
			String type = new String(CellUtil.cloneValue(cell));
			cell = rs.getColumnLatestCell("cf1".getBytes(), "time".getBytes());
			String time = new String(CellUtil.cloneValue(cell));
			cell = rs.getColumnLatestCell("cf1".getBytes(), "targetPhoneNo".getBytes());
			String targetPhoneNo = new String(CellUtil.cloneValue(cell));
			System.out.println("type:" + type + ",time:" + time + ",targetPhoneNo:" + targetPhoneNo);
		}
	}

	/**
	 * 查询某个手机号，type=0的数据 用过滤器，增加rowkey之外的where条件
	 * 
	 * @throws Exception
	 */
	@Test
	public void queryScanWithFilter() throws Exception {
		String rowkey = "18699801068";
		FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
		PrefixFilter prefixFilter = new PrefixFilter(rowkey.getBytes());
		SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter("cf1".getBytes(), "type".getBytes(), CompareOp.EQUAL, "0".getBytes());
		filterList.addFilter(prefixFilter);
		filterList.addFilter(singleColumnValueFilter);

		Scan scan = new Scan();
		scan.setFilter(filterList);
		ResultScanner resultScanner = hTable.getScanner(scan);
		Cell cell;
		for (Result rs : resultScanner) {
			cell = rs.getColumnLatestCell("cf1".getBytes(), "type".getBytes());
			String type = new String(CellUtil.cloneValue(cell));
			cell = rs.getColumnLatestCell("cf1".getBytes(), "time".getBytes());
			String time = new String(CellUtil.cloneValue(cell));
			cell = rs.getColumnLatestCell("cf1".getBytes(), "targetPhoneNo".getBytes());
			String targetPhoneNo = new String(CellUtil.cloneValue(cell));
			System.out.println("type:" + type + ",time:" + time + ",targetPhoneNo:" + targetPhoneNo);
		}

	}
}
