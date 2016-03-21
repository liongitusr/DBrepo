# DBrepo
Frest java tool for conversion between Oracle and Postgres databases. 

Usage java - jar frest.jar Orclhost:port ORCLUSER/ORCLPASS@TNS_NAME Pghost:port PGUSER/PGPASS@PGDB parallel
Example : java - jar frest.jar orcl1.c8fdnue2icnf.us-west-2.rds.amazonaws.com:1521 APPUSER1/APPUSERPASS@ORCL pginst1.c8fdnue2icnf.us-west-2.rds.amazonaws.com:5432 PGUSER/PGPASS@PGDB 4
