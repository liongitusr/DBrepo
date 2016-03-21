package com.frest;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;



public class Frest {
	static int parallel ; 
	static String orclendpoint;
	static String orcluser;
	static String orclpass;
	static String orclinst;
	
	static String pgendpoint;
	static String pguser;
	static String pgpass;
	static String pgdb;
public static void main (String[] args)
{
	try{
		
	 
	orclendpoint=args[0];
	orcluser= args[1].substring(0,args[1].indexOf("/"));
	orclpass= args[1].substring(args[1].indexOf("/")+1,args[1].indexOf("@"));
	orclinst= args[1].substring(args[1].indexOf("@")+1);
	
	pgendpoint=args[2];
	pguser= args[3].substring(0,args[3].indexOf("/"));
	pgpass= args[3].substring(args[3].indexOf("/")+1,args[3].indexOf("@"));
	pgdb= args[3].substring(args[3].indexOf("@")+1);
	parallel = Integer.parseInt(args[4]);
	//System.out.println(orclendpoint + orcluser + orclpass + orclinst + pgendpoint + pguser + pgpass + pgdb);
	}

	catch (Exception e)
	{ System.out.println("Please specify parameters correctly: Oracle_endpoint USER/PASSWORD@INSTANCE Postgre_endpoint USER/PASSWORD@DB parallel_count");
	  System.exit(0);}
	
	
	try {
	 Locale.setDefault(new Locale("en","US"));
	 //String formateddate = (new SimpleDateFormat("yyyy.MM.dd hh-mm-ss")).format(new Date());
	 //System.setOut(new PrintStream(new FileOutputStream(orclendpoint.replace(':', '-') +"-"+orclinst+" to "+ pgendpoint.replace(':', '-')+"-"+pgdb + " " + formateddate +".log")));
	 Connection orclconn = DriverManager.getConnection("jdbc:oracle:thin:@//"+orclendpoint+"/"+orclinst, orcluser, orclpass);
	 QueryRunner qr = new QueryRunner();
	 qr.executeprintQuery(orclconn , "select banner from V$version where rownum<=1", "Connected to");
	 //qr.executeprintQuery(orclconn, "SELECT ROUND(SUM(bytes)/1024/1024,2) col1 FROM dba_segments WHERE owner='"+orcluser.toUpperCase()+"'", "Oracle DB: "+orclinst+" data size (MB) in schema is");
	 //qr.executeprintQuery2(orclconn ,"with t as (select  count(decode(PARTITIONED,'YES',1,NULL)) partitioned, count(decode(TEMPORARY,'Y',1,NULL)) temporary, count(decode(IOT_TYPE, 'IOT',1,NULL)) iot, count(decode(IOT_TYPE, 'IOT_OVERFLOW',1,NULL)) iot_overflow, count(decode(NESTED, 'YES',1,NULL)) nested, count(CASE WHEN TEMPORARY||SECONDARY||NESTED||PARTITIONED='NNNONO' AND IOT_TYPE IS NULL THEN 1 ELSE NULL END ) heap from dba_tables where owner='"+orcluser.toUpperCase()+"') select * from t unpivot(  counts  for table_type in (\"HEAP\", \"PARTITIONED\",\"TEMPORARY\",\"IOT\",\"IOT_OVERFLOW\", \"NESTED\"))", "Table counts");
	 //qr.executeprintQuery2(orclconn , "SELECT INDEX_TYPE, COUNT(*) FROM dba_indexes WHERE OWNER='"+orcluser.toUpperCase()+"' GROUP BY INDEX_TYPE ORDER BY 2 DESC", "Index counts");
	 Connection pgconn = DriverManager.getConnection("jdbc:postgresql://"+pgendpoint+"/"+pgdb+"?rewriteBatchedStatements=true", pguser, pgpass);
	 qr.executeprintQuery(pgconn , " Select * FROM version()", "Connected to");
	 qr.getDDL(orclconn, "TABLE");
	 qr.applyDDL(pgconn, "TABLE");
	 qr.loadData(orclconn, pgconn, parallel);
	 qr.getDDL(orclconn, "INDEX");
	 qr.applyDDL(pgconn, "INDEX");
	}
catch(SQLException se)
{System.out.println(se.getMessage());}
   
}
}
