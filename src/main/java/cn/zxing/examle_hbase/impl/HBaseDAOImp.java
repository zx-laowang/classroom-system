package cn.zxing.examle_hbase.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.PrefixFilter;

import cn.zxing.examle_hbase.HBaseDAO;
/***
 * 
 * @author Administrator
 *生产环境的用法
 */

public class HBaseDAOImp implements HBaseDAO{

	//访问数据库hbase的connection连接池，对hbase的链接，内部进行了很大的优化，性能比HBaseConfiguration要好
	HConnection hTablePool = null;
	public HBaseDAOImp()
	{
		Configuration conf = new Configuration();
		//zookeeper 集群多台机器就要写多台，如果其中一个不可用，hbase可以访问其他的机器
		String zk_list = "192.168.247.31:2181,192.168.247.32:2181,192.168.247.33:2181" ;
		conf.set("hbase.zookeeper.quorum", zk_list);
		try {
			hTablePool = HConnectionManager.createConnection(conf) ;
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	/***
	 * 在调用save方法的时候要封装put对象
	 */
	public void save(Put put, String tableName) {
		// TODO Auto-generated method stub
		HTableInterface table = null;
		try {
			table = hTablePool.getTable(tableName) ;
			table.put(put) ;
			
		} catch (Exception e) {
			e.printStackTrace() ;
		}finally{
			try {
				//关闭表，释放资源
				table.close() ;
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	/***
	 * 在调用insert方法的时候在里面封装put对象，插入单列
	 */
	public void insert(String tableName, String rowKey, String family,
			String quailifer, String value) {
		// TODO Auto-generated method stub
		HTableInterface table = null;
		try {
			table = hTablePool.getTable(tableName) ;
			Put put = new Put(rowKey.getBytes());
			put.add(family.getBytes(), quailifer.getBytes(), value.getBytes()) ;
			table.put(put);
		} catch (Exception e) {
			e.printStackTrace();
		}finally
		{
			try {
				table.close() ;
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	/***
	 * 插入多列，多列用数组表示
	 */
	public void insert(String tableName,String rowKey,String family,String quailifer[],String value[])
	{
		HTableInterface table = null;
		try {
			
			table = hTablePool.getTable(tableName) ;
			Put put = new Put(rowKey.getBytes());
			// 批量添加
			for (int i = 0; i < quailifer.length; i++) {
				String col = quailifer[i];
				String val = value[i];
				put.add(family.getBytes(), col.getBytes(), val.getBytes());
			}
			table.put(put);
		} catch (Exception e) {
			e.printStackTrace();
		}finally
		{
			try {
				table.close() ;
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	/***
	 * 插入多行数据
	 */
	public void save(List<Put> Put, String tableName) {
		// TODO Auto-generated method stub
		HTableInterface table = null;
		try {
			table = hTablePool.getTable(tableName) ;
			table.put(Put) ;
		}
		catch (Exception e) {
			// TODO: handle exception
		}finally
		{
			try {
				table.close() ;
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
	}

	/***
	 * 通过rk查询单行数据，可以把列名传进去，否则就把所有列查询出来
	 */
	public Result getOneRow(String tableName, String rowKey) {
		// TODO Auto-generated method stub
		HTableInterface table = null;
		Result rsResult = null;
		try {
			table = hTablePool.getTable(tableName) ;
			Get get = new Get(rowKey.getBytes()) ;
			rsResult = table.get(get) ;
		} catch (Exception e) {
			e.printStackTrace() ;
		}
		finally
		{
			try {
				table.close() ;
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return rsResult;
	}

	/***
	 * 使用rk的左匹配来查询，返回所有列
	 */
	public List<Result> getRows(String tableName, String rowKeyLike) {
		// TODO Auto-generated method stub
		HTableInterface table = null;
		List<Result> list = null;
		try {
			table = hTablePool.getTable(tableName) ;
			PrefixFilter filter = new PrefixFilter(rowKeyLike.getBytes());
			Scan scan = new Scan();
			scan.setFilter(filter);
			ResultScanner scanner = table.getScanner(scan) ;
			list = new ArrayList<Result>() ;
			for (Result rs : scanner) {
				list.add(rs) ;
			}
		} catch (Exception e) {
			e.printStackTrace() ;
		}
		finally
		{
			try {
				table.close() ;
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return list;
	}
	/***
	 * 返回指定的列，这个方法用的较多
	 */
	public List<Result> getRows(String tableName, String rowKeyLike ,String cols[]) {
		// TODO Auto-generated method stub
		HTableInterface table = null;
		List<Result> list = null;
		try {
			table = hTablePool.getTable(tableName) ;
			PrefixFilter filter = new PrefixFilter(rowKeyLike.getBytes());
			Scan scan = new Scan();
			for (int i = 0; i < cols.length; i++) {
				scan.addColumn("cf".getBytes(), cols[i].getBytes()) ;
			}
			scan.setFilter(filter);
			ResultScanner scanner = table.getScanner(scan) ;
			list = new ArrayList<Result>() ;
			for (Result rs : scanner) {
				list.add(rs) ;
			}
		} catch (Exception e) {
			e.printStackTrace() ;
		}
		finally
		{
			try {
				table.close() ;
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return list;
	}
	/***
	 * rk的范围查询 
	 */
	public List<Result> getRows(String tableName,String startRow,String stopRow)
	{
		HTableInterface table = null;
		List<Result> list = null;
		try {
			table = hTablePool.getTable(tableName) ;
			Scan scan = new Scan() ;
			scan.setStartRow(startRow.getBytes()) ;
			scan.setStopRow(stopRow.getBytes()) ;
			ResultScanner scanner = table.getScanner(scan) ;
			list = new ArrayList<Result>() ;
			for (Result rsResult : scanner) {
				list.add(rsResult) ;
			}
			
		}catch (Exception e) {
			e.printStackTrace() ;
		}
		finally
		{
			try {
				table.close() ;
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return list;
	}
	/***
	 * 删除根据rk左匹配
	 */
	public void deleteRecords(String tableName, String rowKeyLike){
		HTableInterface table = null;
		try {
			table = hTablePool.getTable(tableName) ;
			PrefixFilter filter = new PrefixFilter(rowKeyLike.getBytes());
			Scan scan = new Scan();
			scan.setFilter(filter);
			ResultScanner scanner = table.getScanner(scan) ;
			List<Delete> list = new ArrayList<Delete>() ;
			for (Result rs : scanner) {
				Delete del = new Delete(rs.getRow());
				list.add(del) ;
			}
			table.delete(list);
		}
		catch (Exception e) {
			e.printStackTrace() ;
		}
		finally
		{
			try {
				table.close() ;
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
	}
	
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		HBaseDAO dao = new HBaseDAOImp();
		List<Put> list = new ArrayList<Put>();
		Put put = new Put("r1".getBytes());
		put.add("cf1".getBytes(), "name".getBytes(), "zhaoliu1".getBytes()) ;
		list.add(put) ;
		dao.save(put, "person") ;
//		put.add("cf".getBytes(), "addr".getBytes(), "shanghai1".getBytes()) ;
//		list.add(put) ;
//		put.add("cf".getBytes(), "age".getBytes(), "30".getBytes()) ;
//		list.add(put) ;
//		put.add("cf".getBytes(), "tel".getBytes(), "13567882341".getBytes()) ;
//		list.add(put) ;
//		
//		dao.save(list, "test");
//		dao.save(put, "test") ;
//		dao.insert("test", "testrow", "cf", "age", "35") ;
//		dao.insert("test", "testrow", "cf", "cardid", "12312312335") ;
//		dao.insert("test", "testrow", "cf", "tel", "13512312345") ;
//		List<Result> list = dao.getRows("state", "2014-01",new String[]{"pv_count"}) ;
//		for(Result rs : list)
//		{
//			for(KeyValue keyValue : rs.raw())
//			{
//				System.out.println("rowkey:"+ new String(keyValue.getRow()));
//				System.out.println("Qualifier:"+ new String(keyValue.getQualifier()));
//				System.out.println("Value:"+ new String(keyValue.getValue()));
//				System.out.println("----------------");
//			}
//		}
//		Result rs = dao.getOneRow("test", "testrow");
		
		
	}

}
