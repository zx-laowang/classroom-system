package cn.zxing.examle_hbase;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nullable;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;

public class Test 
{
	//初始化配置信息
	public static Configuration conf;
	static {
		//创建一个HBaseConfiguration对象
		conf=HBaseConfiguration.create();
		String file="hbase-site.xml";
		Path path=new Path(file);
		//加载配置文件
		conf.addResource(path);
	}
	
	/**
	 * 创建表
	 * @param tName
	 * @throws Exception
	 */
	public static void createTable(String tName) throws Exception{
		//提供管理HBASE数据库表元数据一般管理功能的接口。使用HbaseDmin创建、删除、list、启用和禁用表。使用它也可以添加和删除表列族。
		HBaseAdmin admin=new HBaseAdmin(conf);
		if (admin.tableExists(tName)) {
			admin.disableTable(tName);
			admin.deleteTable(tName);
			System.out.println("table is delelte");
		}
		//HTableDescriptor包含关于HBase表的详细信息，如所有列族的描述符，表是目录表，-ROOT-或hbase:meta，如果表是只读的，那么memstore的最大大小，应该发生分割时，与其相关联的协处理器等等…… 
		HTableDescriptor hTableDescriptor =new HTableDescriptor(tName);
		hTableDescriptor.addFamily(new HColumnDescriptor("cf1"));
		//创建表
		//admin.createTable(hTableDescriptor);
		
		//要么向表增加新行 (如果key是新的) 或更新行 (如果key已经存在)。 
		//Puts 通过 HTable.put (writeBuffer) 或 HTable.batch (non-writeBuffer)执行。
		Put put=new Put("1".getBytes());
		//添加数据
		put.add("cf1".getBytes(), "column1".getBytes(), "value1".getBytes());
		put.add("cf1".getBytes(), "column2".getBytes(), "value2".getBytes());
		put.add("cf1".getBytes(), "column3".getBytes(), "value3".getBytes());
		
		Put put1=new Put("2".getBytes());
		put1.add("cf1".getBytes(), "column1".getBytes(), "value4".getBytes());
		put1.add("cf1".getBytes(), "column2".getBytes(), "value5".getBytes());
		put1.add("cf1".getBytes(), "column3".getBytes(), "value6".getBytes());
		//关闭日志写入
		put1.setWriteToWAL(false);
		//添加put对象
		HTable table=new HTable(conf, tName);
		table.setAutoFlush(false);
		table.setWriteBufferSize(1000000);
		table.put(put);
		table.put(put1);
		
	}
	/**
	 * 删除行
	 * @param tName
	 * @param rk1
	 * @param rk2
	 * @throws Exception
	 */
	public static void deleteRows(String tName,String rk1,String rk2) throws Exception {
		HTable table=new HTable(conf, tName);
//		Delete delete =new Delete(rk1.getBytes());
		Delete delete1 =new Delete(rk1.getBytes());
		Delete delete2 =new Delete(rk2.getBytes());
		//删除单行
//		table.delete(delete);
		List<Delete> list=new ArrayList<Delete>();
		list.add(delete1);
		list.add(delete2);
		//删除多行
		table.delete(list);
		table.close();
	}
	
	
	/***
	 * get 根据rowkey单行查询 select * from ...where id=?
	 * @param tName
	 * @param rowKey
	 * @throws Exception
	 */
	public static void getRow(String tName,String rowKey) throws Exception{
		HTable table=new HTable(conf, tName);
		Get get=new Get(rowKey.getBytes());
		Result re=table.get(get);
		for(KeyValue keyValue:re.raw()) {
			System.out.println(new String(keyValue.getFamily())+":"+new String(keyValue.getRow())+":"+new String(keyValue.getQualifier())+"="+new String(keyValue.getValue()));
		}
		
	}
	/***
	 * scan 全表或者范围查询
	 * @param tName
	 * @param startRowKey
	 * @param stopRowkey
	 * @throws Exception
	 */
	public static void scanRows(String tName,String startRowKey,String stopRowkey)throws Exception {
		HTable table=new HTable(conf, tName);
		Scan scan=new Scan();
		scan.setStartRow(startRowKey.getBytes());
		scan.setStopRow(stopRowkey.getBytes());
		//设置查询出特定的字段
		scan.addColumn("cf1".getBytes(), "column1".getBytes());
		scan.addColumn("cf1".getBytes(), "column2".getBytes());
		ResultScanner rs=table.getScanner(scan);
		//getFamily获取列族
		//getRow获取rowkey
		//getQualifier获取列
		//getValue获取列值
		for(Result re:rs) {
			for(KeyValue keyValue:re.raw()) {
				System.out.println(new String(keyValue.getFamily())+":"+
			new String(keyValue.getRow())+":"+
						new String(keyValue.getQualifier())+"="+new String(keyValue.getValue()));
			}
		}
	}
	/***
	 * 根据非rowkey字段查询
	 * @param tName
	 * @param column
	 * @param columnValue
	 */
	public static void query(String tName,String column,String columnValue)throws Exception {
		HTable table=new HTable(conf, tName);
		Scan scan=new Scan();
		//设置查询出特定的字段
		scan.addColumn("cf1".getBytes(), "column1".getBytes());
		scan.addColumn("cf1".getBytes(), "column2".getBytes());
		//设置过滤器
		Filter filter=new SingleColumnValueFilter("cf1".getBytes(), column.getBytes(), CompareOp.EQUAL, columnValue.getBytes());
		scan.setFilter(filter);
		
		ResultScanner rs=table.getScanner(scan);
		for(Result re:rs) {
			for(KeyValue keyValue:re.raw()) {
				System.out.println(new String(keyValue.getFamily())+":"+
			new String(keyValue.getRow())+":"+
						new String(keyValue.getQualifier())+"="+new String(keyValue.getValue()));
			}
		}
	}
	/***
	 * 根据rowkey字段查询
	 * @param tName
	 * @param column
	 * @param columnValue
	 */
	public static void queryByRowkey(String tName,String rowkeyGex)throws Exception {
		HTable table=new HTable(conf, tName);
		Scan scan=new Scan();
		//设置查询出特定的字段
		scan.addColumn("cf1".getBytes(), "column1".getBytes());
		scan.addColumn("cf1".getBytes(), "column2".getBytes());
		//设置过滤器
		RowFilter filter=new RowFilter(CompareOp.EQUAL, new RegexStringComparator(rowkeyGex));
		//前置过滤器
//		PrefixFilter filter=new PrefixFilter(rowkeyGex.getBytes());
		scan.setFilter(filter);
		
		ResultScanner rs=table.getScanner(scan);
		for(Result re:rs) {
			for(KeyValue keyValue:re.raw()) {
				System.out.println(new String(keyValue.getFamily())+":"+
						new String(keyValue.getRow())+":"+
						new String(keyValue.getQualifier())+"="+new String(keyValue.getValue()));
			}
		}
	}
	
	/***
	 * select ....from ...where col=? and col1 in(value1,value2)
	 * @param tName
	 * @param column
	 * @param columnValue
	 */
	public static void queryByOrAnd(String tName,String column1,String column1value1,String column2,String column2Value1,String column2Value2)throws Exception {
		HTable table=new HTable(conf, tName);
		Scan scan=new Scan();
		//设置查询出特定的字段
		scan.addColumn("cf1".getBytes(), "column1".getBytes());
		scan.addColumn("cf1".getBytes(), "column2".getBytes());
		//设置过滤器
		//FirstKeyOnlyFilter  KeyOnlyFilter
		Filter filter1=new SingleColumnValueFilter("cf1".getBytes(), column1.getBytes(), CompareOp.EQUAL, column1value1.getBytes());
		Filter filter2=new SingleColumnValueFilter("cf1".getBytes(), column2.getBytes(), CompareOp.EQUAL, column2Value1.getBytes());
		Filter filter3=new SingleColumnValueFilter("cf1".getBytes(), column2.getBytes(), CompareOp.EQUAL, column2Value2.getBytes());
		FilterList list=new FilterList(Operator.MUST_PASS_ONE);
		list.addFilter(filter2);
		list.addFilter(filter3);
		FilterList listall=new FilterList();
		listall.addFilter(filter1);
		listall.addFilter(list);
		scan.setFilter(listall);
		
		ResultScanner rs=table.getScanner(scan);
		int num=0;
		for(Result re:rs) {
			num++;
			for(KeyValue keyValue:re.raw()) {
				System.out.println(new String(keyValue.getFamily())+":"+
			new String(keyValue.getRow())+":"+
						new String(keyValue.getQualifier())+"="+new String(keyValue.getValue()));
			}
		}
		System.out.println(num);
	}
	
	/***
	 * 根据关联表查询，其中子表的rowkey是以pid_id的格式
	 * @param tName
	 * @param column
	 * @param columnValue
	 */
	public static void queryParent(String tName,String parent_id)throws Exception {
		String rowkeyGex=StringUtils.reverse(parent_id)+"_";
		HTable table=new HTable(conf, tName);
		Scan scan=new Scan();
		//设置查询出特定的字段
		scan.addColumn("cf1".getBytes(), "column1".getBytes());
		scan.addColumn("cf1".getBytes(), "column2".getBytes());
		//设置过滤器
//		RowFilter filter=new RowFilter(CompareOp.EQUAL, new RegexStringComparator(rowkeyGex));
		//前置过滤器
		PrefixFilter filter=new PrefixFilter(rowkeyGex.getBytes());
		scan.setFilter(filter);
		
		ResultScanner rs=table.getScanner(scan);
		for(Result re:rs) {
			for(KeyValue keyValue:re.raw()) {
				System.out.println(new String(keyValue.getFamily())+":"+
						new String(keyValue.getRow())+":"+
						new String(keyValue.getQualifier())+"="+new String(keyValue.getValue()));
			}
		}
	}
    public static void main( String[] args ) throws Exception{
    	createTable("student");
//    	deleteRows("test", "1","2");
//    	getRow("test", "1");
//    	scanRows("test", "1", "3");
//    	query("test", "column1", "value1");
//    	queryByRowkey("test", "1*");
//    	queryByOrAnd("test", "column1", "value1", "column2", "value2", "value3");
    }
}
