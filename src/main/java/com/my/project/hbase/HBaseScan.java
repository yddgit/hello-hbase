package com.my.project.hbase;

import static com.my.project.hbase.ConfigUtils.getColumnFamily;
import static com.my.project.hbase.ConfigUtils.getFilterValue;
import static com.my.project.hbase.ConfigUtils.getPort;
import static com.my.project.hbase.ConfigUtils.getQualifierFilter;
import static com.my.project.hbase.ConfigUtils.getFilterQualifier;
import static com.my.project.hbase.ConfigUtils.getTableName;
import static com.my.project.hbase.ConfigUtils.getZookeeper;
import static com.my.project.hbase.ConfigUtils.getTimeout;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.exceptions.HBaseException;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

/**
 * scan 'TABLE1', {COLUMN=>'cf',FILTER=>"(SingleColumnValueFilter('cf','name',=,'binary:TOM'))"}
 * scan 'TABLE1', {COLUMN=>'cf',FILTER=>"(SingleColumnValueFilter('cf','name',=,'binary:TOM')) AND (QualifierFilter(= 'binary:id'))"}
 * scan 'TABLE1', {COLUMN=>'cf',FILTER=>"(SingleColumnValueFilter('cf','name',=,'binary:TOM')) AND (QualifierFilter(= 'regexstring:(name|id)'))"}
 */
public class HBaseScan implements Closeable {

	private static final Logger LOGGER = Logger.getLogger(HBaseScan.class);
	private static final Integer HBASE_HTABLE_THREADS_MAX = 50;

	private final String table;
	private final String family;
	private final Connection connection;

	static{
		String osName = System.getProperty("os.name");
		if (osName.startsWith("Windows")) {
			try {
				String hadoop = HBaseScan.class.getResource("/hadoop").getPath();
				if(hadoop == null || hadoop.contains("!")){
					String userdir = System.getProperty("user.dir");
					hadoop = "/" + userdir +"/config/hadoop";
				}
				hadoop = java.net.URLDecoder.decode(hadoop,"UTF-8");
				System.setProperty("hadoop.home.dir",hadoop);
			} catch (Exception e) {
				LOGGER.error("set hadoop.home error",e);
			}
		}
	}

	public static void main(String[] args) throws Exception {
		try(HBaseScan scanner = new HBaseScan(getZookeeper(), getPort(), getTableName(), getColumnFamily(), getTimeout())) {
			long start = System.currentTimeMillis();
			AtomicLong count = new AtomicLong(0);
			scanner.scan(getFilterQualifier(), getFilterValue(), getQualifierFilter(), result -> {

				Iterator<Cell> iterator = result.listCells().iterator();
				while(iterator.hasNext()){
					Cell cell = iterator.next();
					String name = Bytes.toString(CellUtil.cloneQualifier(cell));
					String value = Bytes.toString(CellUtil.cloneValue(cell));
					System.out.println(name + "=" + value);
				}

				count.incrementAndGet();
			});
			long end = System.currentTimeMillis();
			LOGGER.info("Scanned " + count.get() + " records, use time: " + (end-start) + "ms");
		}
	}

	public HBaseScan(String zookeeper, String port, String table, String family, String timeout) throws HBaseException{
		try {
			this.table = table;
			this.family = family;
			Configuration config = HBaseConfiguration.create();
			config.set("hbase.zookeeper.quorum",zookeeper);
		    config.set("hbase.zookeeper.property.clientPort", port);
		    config.setInt("hbase.htable.threads.max", HBASE_HTABLE_THREADS_MAX);
		    config.set("hbase.rpc.timeout", timeout);
		    config.set("hbase.client.scanner.timeout.period", timeout);
		    this.connection = ConnectionFactory.createConnection(config);
			LOGGER.info("HBase connection success");
		} catch (Exception e) {
			throw new HBaseException("connect error", e);
		}
	}
	
	public Table getTable() throws HBaseException{
		try {
			return connection.getTable(TableName.valueOf(table));
		} catch (Exception e) {
			throw new HBaseException("HBase getTable exception", e);
		}
	}
	
	public boolean check() throws HBaseException, IOException {
		Table table = getTable();
		try {
			table.exists(new Get(Bytes.toBytes("NULL")));
			return true;
		}finally{
			if(table != null){ closeTable(table); }
		}
	}
	
	public boolean ping() {
		Table table = null;
		try {
			table = getTable();
			return table.exists(new Get(Bytes.toBytes("NULL")));
		} catch (Exception e) {
			return false;
		}finally{
			if(table != null){ closeTable(table); }
		}
	}
	
	/**
	 * 是否存在
	 */
	public boolean exist(String key) throws HBaseException {
		Table table = getTable();
		try {
			Get get = new Get(Bytes.toBytes(key));
			return table.exists(get);
		} catch (Exception e) {
			throw new HBaseException("exist error:" + key ,e);
		}finally{
			closeTable(table);
		}
	}

	public void scan(String qualifier, String value, String qualifierFilter, Consumer<Result> consumer) throws HBaseException {
		Table table = getTable();
		try {
			Scan scan = new Scan();
			FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
			if(qualifier != null && !"".equals(qualifier.trim())
					&& value != null && !"".equals(value.trim())){
				SingleColumnValueFilter scvf = new SingleColumnValueFilter(  
				        Bytes.toBytes(family),   
				        Bytes.toBytes(qualifier),   
				        CompareOp.EQUAL,
				        Bytes.toBytes(value));
				scvf.setFilterIfMissing(true);  
				scvf.setLatestVersionOnly(true);
				filterList.addFilter(scvf);
			}
			if(qualifierFilter != null && !"".equals(qualifierFilter)) {
				if(!qualifierFilter.contains(",")) {
					filterList.addFilter(new QualifierFilter(CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes(qualifierFilter))));
				} else {
					filterList.addFilter(new QualifierFilter(CompareOp.EQUAL, new RegexStringComparator("(" + qualifierFilter.replace(",", "|") + ")")));
				}
			}
			scan.setFilter(filterList);
			scan.setMaxResultSize(Long.MAX_VALUE);
			scan.addFamily(Bytes.toBytes(family));
			scan.setCaching(10000);
			ResultScanner scanner = table.getScanner(scan);
			scanner.forEach(consumer);
		} catch (Exception e) {
			throw new HBaseException("list data error" ,e);
		}finally{
			closeTable(table);
		}
	}

	/**
	 * 关闭到表的连接
	 */
	protected void closeTable(Table table){
		try {
			if(table != null){ table.close(); }
		} catch (Exception e) {
			LOGGER.error("HBase close exception", e);
		}
	}
	
	@Override
	public void close() throws IOException {
		try {
			if(connection != null){
				connection.close();
				LOGGER.info("Shutdown HBase Connection");
			}
		} catch (Exception e) {
			LOGGER.error("HBase close exception", e);
		}
	}
}
