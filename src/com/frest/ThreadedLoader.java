package com.frest;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class ThreadedLoader implements Runnable{
	List<ArrayList<Object>> rows;
	PreparedStatement ps;
	String tablename;
	int columncnt;
	QueryRunner qr;
	Connection conn;
	String sqlstr;
	 Runtime runtime = Runtime.getRuntime();
	 ThreadedLoader(List<ArrayList<Object>> rows ,Connection conn, String sqlstr, String tablename, int columncnt,QueryRunner qr)
	{
		this.rows=rows;
		this.conn=conn;
		this.sqlstr=sqlstr;
		this.tablename=tablename;
		this.columncnt=columncnt;
		this.qr=qr;
		
	}
	
public void run ()
	{try { System.out.println("Starting thread for loading into "+tablename);
	int Batchcount=100000;
	int r = 0;
	PreparedStatement ps = conn.prepareStatement(sqlstr);
	for (List<Object> obj : rows)
	{

	{for (int colcnt = columncnt; colcnt>0; colcnt--)
	{
	ps.setObject(colcnt, obj.get(colcnt-1));
	}

	ps.addBatch();

	if (++r==Batchcount) 
	 {int[] rs1 = ps.executeBatch();
	 System.out.println(r);
	 System.out.println(new Date() + " : Table "+ tablename + " loaded with "+rs1.length + " rows");
	 r=0;}

}

	}	
	int[] rs1 = ps.executeBatch();
	System.out.println(new Date() + " : Table "+ tablename + " loaded with "+rs1.length + " rows");
	qr.threadcount-=1;
	System.out.println("Used Memory:"
	            + (runtime.totalMemory() - runtime.freeMemory()) / (1024*1024));
	System.out.println("Total Memory "+runtime.totalMemory()/ (1024*1024));
	  System.out.println("Free Memory:"
	            + runtime.freeMemory() / (1024*1024));
}

catch (SQLException se)
{ System.out.println(se.getNextException());
	System.out.println(se.getMessage());
}
}

}