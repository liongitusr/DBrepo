package com.frest;

import java.sql.Array;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;


public class QueryRunner {
ArrayList<String> ls = new ArrayList<String>();	
int threadcount;	
public void executeprintQuery (Connection conn, String sql, String msg)
{
try  {
	PreparedStatement ps = conn.prepareStatement(sql);
 ResultSet rs=ps.executeQuery();
	while (rs.next())
			{System.out.println(msg + " " + rs.getString(1)); }
}
catch (SQLException se)
{System.out.println(se.getMessage());}
}

public void executeprintQuery2 (Connection conn, String sql, String msg)
{
try {PreparedStatement ps = conn.prepareStatement(sql);
 ResultSet rs=ps.executeQuery();
 System.out.println(msg);
	while (rs.next())
			{System.out.println(rs.getString(1) + ": " + rs.getInt(2)); }
}
catch (SQLException se)
{System.out.println(se.getMessage());}
}
public ArrayList<String> getDDL (Connection conn, String type)
{ 
if (type.equals("TABLE")) 	
try {
	ls.removeAll(ls);
String sql = "SELECT 'CREATE TABLE ' || LOWER(table_name) || ' (' || LISTAGG('\"' || LOWER(COLUMN_NAME) || '\"' || ' ' || "
   							+"DECODE(DATA_TYPE,"
   							+"'VARCHAR2',"
   							+"'varchar',"
   							+" 'CLOB',"
   							+"'text',"
   							+"'BLOB',"
   							+"'bytea',"
   							+"'NUMBER',"
   							+"'numeric',"
   							+"'LONG',"
   							+"'text',"
   							+"'raw()',"
   							+"'text',"
   							+"'CHAR',"
   							+"'char',"
   							+"'text') || DECODE(DATA_TYPE,"
   							+"'CHAR',"
   							+"'(' || DATA_LENGTH || ')',"
   							+"'VARCHAR2',"
   							+"'(' || DATA_LENGTH || ')',"
   							+"'NUMBER',"
   							+"CASE "
   							+"WHEN DATA_PRECISION || DATA_SCALE IS NOT NULL THEN"
   							+"'(' || DATA_PRECISION || ',' || DATA_SCALE || ')'"
   							+"ELSE"
   							+"' '"
   							+"END,"
   							+"' ') ||"
   							+"DECODE(NULLABLE, 'N', ' NOT NULL', ' '),"
   							+"',') WITHIN GROUP(ORDER BY column_id desc) || ')'"
   							+"FROM user_tab_columns "
   							+"GROUP BY TABLE_NAME"; 	
/** Getting tables DDL*/
PreparedStatement ps = conn.prepareStatement(sql);
ResultSet rs=ps.executeQuery();
	while (rs.next())
			{ls.add(rs.getString(1));}

}
catch (SQLException se)
{System.out.println(se.getMessage());}

if (type.equals("INDEX")) 
	try {ls.removeAll(ls);
		String sql = " select ddlstr||'('||listagg(LOWER(t2.column_name),',') within group (order by column_position)||')'"+ 
			 "from (select index_name, table_name, 'CREATE'||DECODE(UNIQUENESS,'UNIQUE', ' UNIQUE','')||' INDEX '||LOWER(index_name)||' ON '||LOWER(table_name) ddlstr from user_indexes) t1, user_ind_columns t2 where t1.index_name=t2.index_name "+
			 "group by t1.ddlstr";	
	
			/** Getting indexes DDL*/
			 PreparedStatement ps = conn.prepareStatement(sql);
			 ResultSet rs=ps.executeQuery();
			 while (rs.next())
				{ls.add(rs.getString(1));}
			 }
	catch (SQLException se)
	{System.out.println(se.getMessage());}

return ls;}

public void applyDDL (Connection conn, String type)
{
	System.out.println(new Date()+ ": Starting "+type+" creation ...");
	for (String i : ls)
	try
	{	/** applying DDL*/
	PreparedStatement ps = conn.prepareStatement(i);
	System.out.println(i);
	ResultSet rs = ps.executeQuery();
	}
	catch (SQLException se)
	{	if (!se.getMessage().equals("No results were returned by the query.")) 
		System.out.println(se.getMessage());
}
	System.out.println(new Date()+ ": Finishing "+type+" creation ...");
	
}

public void loadData (Connection orclconn, Connection pgconn,String pgendpoint,String pgdb,String pguser, String pgpass, int parallel) {
	try
	{	System.out.println("Starting loading data");
		class Tristr 
		{String st1;
		 String st2;
		 String st3;
		 Tristr(String st1,String st2, String st3)
		 {this.st1=st1;
		 this.st2=st2;
		 this.st3=st3;}}
	
		/** Getting tables */
	List<Tristr> ls = new ArrayList<Tristr>();	
	String sql = "SELECT table_name, 'SELECT * FROM '||table_name, 'INSERT INTO '||table_name||' ('||listagg(column_name,',') within group (order by column_id desc)||')'||' SELECT '||listagg('cast(? as '||DECODE(DATA_TYPE,'VARCHAR2','varchar', 'CLOB','text','BLOB','bytea','NUMBER','numeric','LONG','text','raw()','text','CHAR','char','text') || DECODE(DATA_TYPE,'CHAR','(' || DATA_LENGTH || ')','VARCHAR2','(' || DATA_LENGTH || ')','NUMBER',CASE WHEN DATA_PRECISION || DATA_SCALE IS NOT NULL THEN '(' || DATA_PRECISION || ',' || DATA_SCALE || ')' ELSE '' END ,'')||')',',') within group (order by column_id desc)||' ' FROM user_tab_columns group by table_name";	
	PreparedStatement ps = orclconn.prepareStatement(sql);
	ResultSet rs = ps.executeQuery();
	while (rs.next())
	{ls.add(new Tristr(rs.getString(1), rs.getString(2), rs.getString(3)));}
	for (Tristr i: ls)
	{	
	 ps = orclconn.prepareStatement(i.st2);
	 System.out.println(i.st2);
	 rs = ps.executeQuery();
	 ResultSetMetaData rsmd = rs.getMetaData();
	 int columncnt = rsmd.getColumnCount();
	List<ArrayList<Object>> rows = new ArrayList<ArrayList<Object>>();
	
	rs.setFetchSize(10000);
	 while (rs.next()) {
		 
	   ArrayList<Object> columns = new ArrayList<Object>();

	     for (int k = 1; k <= columncnt; k++) {
	         columns.add(rs.getObject(k));
	     } 
	     rows.add(columns);
		
	 }
	
	 if (rows.size()>100000)
	  for (int k =1; k<=parallel; k++ )
	 { 	 int add ;
		 if (k==parallel) {add =0; }
	    else {add =1;};
	    
		int idxstart = (int) Math.round(  (rows.size()*(k-1))/ (double) parallel );
		int idxend = (int) Math.round( (rows.size()*k)/(double) parallel -add ) ;
	
		label1 : 
			while (threadcount>=parallel)			
		try {
			System.out.println("All threads are occupied, waiting 30 sec for resource");
		    Thread.sleep(30*1000);    
		    //1000 milliseconds is one second.
		   continue label1;
		} catch(InterruptedException ex) {
		    System.out.println(ex.getMessage());
		}
		
		threadcount+=1;	
		System.out.println("Thread count is : "+threadcount);
		//System.out.println("idx start is "+idxstart + " idx end is "+idxend);
		Connection conn = DriverManager.getConnection("jdbc:postgresql://"+pgendpoint+"/"+pgdb+"?rewriteBatchedStatements=true", pguser, pgpass);
		Thread t = new Thread(new ThreadedLoader(rows.subList(idxstart, idxend), conn, i.st3, i.st1, columncnt, this,threadcount ));
		t.start();}
		
	try { int Batchcount=100000;
	 int r = 0;
	 ps = pgconn.prepareStatement(i.st3);
	 for (List<Object> obj : rows)
	 {
	
     {for (int colcnt = columncnt; colcnt>0; colcnt--)
     {
	 ps.setObject(colcnt, obj.get(colcnt-1));
	 }
    
     ps.addBatch();
     if (++r==Batchcount) 
    	try  {int[] rs1 = ps.executeBatch();
    	 
    	 System.out.println(new Date() + " : Table "+ i.st1 + " loaded with "+rs1.length + " rows");
    	 r=0;}
     finally
     {int[] rs1 = ps.executeBatch();
	 
	 System.out.println(new Date() + " : Table "+ i.st1 + " loaded with "+rs1.length + " rows");}
     
	 }
	 } 
	}
	catch 
	(SQLException se)
	{System.out.println(se.getMessage());}
	}
	}
	
	catch (SQLException se)
	{System.out.println(se.getMessage());
}	
System.out.println(new Date()+ " : Finishing loading data ...");}

	 
}

