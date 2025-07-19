package com.example.MigrationProject1;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.Test;
import org.apache.iceberg.*;
import org.apache.iceberg.io.FileIO;

import com.example.writer.DucklakeSnapshotWriter;

/**
 * Unit test for {@link DucklakeSnapshotWriter}.
 * This test verifies that snapshots are written to the DuckLake metadata tables
 * and that duplicate snapshot checks work as expected.
 */
public class DucklakeSnapshotWriterTest 
{
	private Connection connection;
	private DucklakeSnapshotWriter writer;
	private Snapshot testSnapshot;
	
	// Setup: creates in-memory DuckDB tables and a test snapshot.
	@BeforeEach
	void setup() throws SQLException
	{
		connection = DriverManager.getConnection("jdbc:duckdb:memory:");
		writer=new DucklakeSnapshotWriter();
		
	      //create a sample ducklake_table for test
	      try (Statement stmt = connection.createStatement()) 
	      {
	    	  stmt.execute("CREATE SCHEMA IF NOT EXISTS __ducklake_metadata_mydl");
	    	  stmt.execute("CREATE TABLE IF NOT EXISTS __ducklake_metadata_mydl.ducklake_snapshot ("
	    	    + "snapshot_id BIGINT,"
	    	    + "snapshot_time TIMESTAMP,"
	    	    + "schema_version BIGINT,"
	    	    + "next_catalog_id BIGINT,"
	    	    + "next_file_id BIGINT)");
	    	  
	    	  //  create a sample ducklake_table for test
	    	  stmt.execute("CREATE TABLE IF NOT EXISTS __ducklake_metadata_mydl.ducklake_table (table_id BIGINT)");
	    	  
	    	 //  create a sample ducklake_table for test
	    	  stmt.execute("CREATE TABLE IF NOT EXISTS __ducklake_metadata_mydl.ducklake_data_file (data_file_id BIGINT,begin_snapshot BIGINT)");
	    }

	      // Create a sample snapshot for test
	    testSnapshot = new Snapshot() 
	    {
	    	@Override public long snapshotId() { return 1234L; }
		    @Override public long sequenceNumber() { return 0; }
		    @Override public long timestampMillis() { return System.currentTimeMillis(); }
		   	@Override public Integer schemaId() { return (int) 1L; }
			@Override public List<ManifestFile> allManifests(FileIO io) { return null; }
			@Override public List<ManifestFile> dataManifests(FileIO io) { return null; }
			@Override public List<ManifestFile> deleteManifests(FileIO io) { return null; }
			@Override public Iterable<DataFile> addedDataFiles(FileIO io) { return null; }
			@Override public Iterable<DataFile> removedDataFiles(FileIO io) { return null; }
			@Override public String manifestListLocation() { return null; }
			@Override public String operation() { return null; } 
			@Override public Map<String, String> summary() { return null; }
			@Override public Long parentId() { return null; }
	    };
	    
	}
	
	//Clean up the in-memory DuckDB connection after each test.
	@AfterEach
	void cleanup() throws SQLException
	{
		if(connection!=null && !connection.isClosed())
		{
			connection.close();
		}
	}
	
  //Tests that a snapshot is successfully inserted into the metadata table.
  @Test
  void testSnapshotInsertedOrNot() throws Exception 
  {
      writer.snapshotWriter(connection, List.of(testSnapshot));

      // Verify the snapshot was inserted
      try (Statement stmt = connection.createStatement();
           ResultSet rs = stmt.executeQuery(
               "SELECT snapshot_id FROM __ducklake_metadata_mydl.ducklake_snapshot WHERE snapshot_id = 1234")) 
      {
        assertTrue(rs.next());
      }
  }
  
  //Tests that snapshotExists correctly returns true
  @Test 
  void testSnapshotExistsAfterInsert() throws SQLException {
    // Insert
    writer.snapshotWriter(connection, List.of(testSnapshot));
    // 
    boolean exists = writer.snapshotExists(testSnapshot, connection);
    assertTrue(exists);
  }

 }