package com.example.config;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class DucklakeConnectionManager 
{
	public static Connection duckdbConnection() throws SQLException
	{
		Connection connection=DriverManager.getConnection("jdbc:duckdb:"+ConfigLoader.get("duckdb.db.path"));
		
		Statement stmt = connection.createStatement();

		// Install + load DuckLake extension
		stmt.execute("INSTALL " + ConfigLoader.get("ducklake.extension"));
		stmt.execute("LOAD " + ConfigLoader.get("ducklake.extension"));
		
		//Attach ducklake catalog
		stmt.execute("ATTACH 'ducklake:duckdbdemo.ducklake' AS mydl (data_path 's3://warehouse/wh')");

		// MinIO S3 config for DuckDB
		stmt.execute("SET s3_region='" + ConfigLoader.get("duckdb.s3.region") + "'");
		stmt.execute("SET s3_endpoint='" + ConfigLoader.get("duckdb.s3.endpoint") + "'");
		stmt.execute("SET s3_access_key_id='" + ConfigLoader.get("duckdb.s3.access_key_id") + "'");
		stmt.execute("SET s3_secret_access_key='" + ConfigLoader.get("duckdb.s3.secret_access_key") + "'");
		stmt.execute("SET s3_url_style='" + ConfigLoader.get("duckdb.s3.url_style") + "'");
		stmt.execute("SET s3_use_ssl = " + ConfigLoader.get("duckdb.s3.use_ssl"));
		
		return connection;
	}
	
}
