package com.example.config;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * This class creates a DuckDB connection and sets up the DuckLake extension.
 * 
 * It:
 * - Connects to DuckDB.
 * - Installs and loads the DuckLake extension.
 * - Attaches the DuckLake catalog.
 * - Sets S3 storage settings for MinIO or S3.
 */
public class DucklakeConnectionManager 
{
	/**
     * Opens a DuckDB connection and configures DuckLake.
     *
     * @return A ready-to-use DuckDB {@link Connection}.
     * @throws SQLException if something goes wrong.
     */
	public static Connection duckdbConnection() throws SQLException
	{
		//Connect to the duckdb database
		Connection connection=DriverManager.getConnection("jdbc:duckdb:"+ConfigLoader.get("duckdb.db.path"));
		
		Statement stmt = connection.createStatement();

		// Install and load DuckLake extension
		stmt.execute("INSTALL " + ConfigLoader.get("ducklake.extension"));
		stmt.execute("LOAD " + ConfigLoader.get("ducklake.extension"));
		
		// Attach ducklake catalog
		String catalogPath = ConfigLoader.get("ducklake.catalog.path");
		String alias = ConfigLoader.get("ducklake.catalog.alias");
		String dataPath = ConfigLoader.get("ducklake.data.path");

		String attachSql = String.format(
		    "ATTACH '%s' AS %s (data_path '%s')",
		    catalogPath, alias, dataPath
		);
		
		stmt.execute(attachSql);

		//stmt.execute("ATTACH " + ConfigLoader.get("ducklake.catalog.path ") + ConfigLoader.get("ducklake.catalog.alias") + ConfigLoader.get("ducklake.data.path"));

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
