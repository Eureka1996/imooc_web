package com.wufuqiang.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @ author wufuqiang
 * @ date 2019/3/16/016 - 15:12
 **/
public class HBaseUtil {
    public static Configuration conf ;
    private static Connection connection ;
    static{
        conf = HBaseConfiguration.create() ;
        try{

            connection =ConnectionFactory.createConnection(conf) ;
        }catch (Exception e){
            System.out.println("连接HBase失败") ;
        }
    }

    private HBaseUtil(){
        if(connection == null){
            try {
                connection = ConnectionFactory.createConnection(conf) ;
            } catch (IOException e) {
                System.out.println("连接HBase失败");
            }
        }
    }



    //    添加一行数据
    public static void addRow(String tableName , String rowKey , String cf , String column , String value)throws IOException{
        Table table = connection.getTable(TableName.valueOf(tableName))  ;
        Put put = new Put(Bytes.toBytes(rowKey)) ;
        put.addColumn(Bytes.toBytes(cf),Bytes.toBytes(column),Bytes.toBytes(value)) ;
        table.put(put) ;
        System.out.println("添加数据成功。");
        table.close() ;
    }

    public static void incrementColumnValue(String tableName , String rowKey,String cf , String column,Long value)throws IOException{
        Table table = connection.getTable(TableName.valueOf(tableName)) ;
        table.incrementColumnValue(Bytes.toBytes(rowKey),Bytes.toBytes(cf),Bytes.toBytes(column),value) ;
        table.close();
        System.out.println("数据累加成功") ;
    }

    public static void addRow(String tableName ,List<Put> puts)throws IOException{
        Table table = connection.getTable(TableName.valueOf(tableName)) ;
        table.put(puts) ;
        table.close() ;
    }

    public static void deleteRows(String tableName , List<Delete> deletes)throws IOException{
        Table table = connection.getTable(TableName.valueOf(tableName) ) ;
        table.delete(deletes) ;
        table.close() ;
    }

    public static void getAllRows(String tableName)throws IOException{
        Table table = connection.getTable(TableName.valueOf(tableName)) ;
        Scan scan = new Scan() ;
        ResultScanner resultScanner = table.getScanner(scan) ;
        for(Result result : resultScanner){
            Cell[] cells = result.rawCells() ;
            for(Cell cell : cells){
                System.out.println("行健："+Bytes.toString(CellUtil.cloneRow(cell))) ;
                System.out.println("列族："+Bytes.toString(CellUtil.cloneFamily(cell))) ;
                System.out.println("列："+Bytes.toString(CellUtil.cloneQualifier(cell))) ;
                System.out.println("值："+Bytes.toString(CellUtil.cloneValue(cell))) ;

            }
            System.out.println("---------------------------------------------");

        }
        table.close() ;
    }

    public static Map<String,Long> query(String tableName, String day)throws IOException{
        Map<String,Long> map = new HashMap<String,Long>() ;
        Table table = connection.getTable(TableName.valueOf(tableName)) ;
        String cf = "info" ;
        String qualifier = "click_count" ;
        Scan scan = new Scan() ;

        Filter filter = new PrefixFilter(Bytes.toBytes(day)) ;
        scan.setFilter(filter) ;

        ResultScanner resultScanner = table.getScanner(scan) ;
        for(Result result : resultScanner){
            String row = Bytes.toString(result.getRow()) ;
            long clickCount = Bytes.toLong(result.getValue(cf.getBytes(),qualifier.getBytes())) ;
            map.put(row,clickCount) ;


        }
        table.close() ;
        return map ;
    }

    public static List<Result> getRows(String tableName , RowFilter filter) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName)) ;
        Scan scan = new Scan() ;
        scan.setFilter(filter) ;
        ResultScanner resultScanner = table.getScanner(scan) ;


        List<Result> results = new ArrayList<Result>() ;
        for(Result result : resultScanner){
            results.add(result) ;
        }
        table.close() ;
        return results ;
    }

    public static Result[] getRows(String tableName,List<Get> gets)throws IOException{
        Table table = connection.getTable(TableName.valueOf(tableName)) ;
        Result[] results = table.get(gets) ;
        table.close() ;
        return results ;

    }

    public static Result getARowByRowKey(String tableName,String rowKey,String cf)throws IOException{
        Table table = connection.getTable(TableName.valueOf(tableName)) ;

        Get get = new Get(Bytes.toBytes(rowKey)) ;
        get.setMaxVersions(5) ;
        get.addFamily(Bytes.toBytes(cf)) ;
        Result result = table.get(get) ;
        table.close() ;
        return result ;
    }


    public static void close() throws IOException{
        if(connection != null){
            connection.close() ;
        }
    }

    public static void main(String[] args)throws Exception{
        Map<String,Long> result = query("imooc_course_clickcount","20190321") ;
        for(Map.Entry<String,Long> entry:result.entrySet()){
            System.out.println(entry.getKey()+"-----"+entry.getValue()) ;

        }
    }
}
