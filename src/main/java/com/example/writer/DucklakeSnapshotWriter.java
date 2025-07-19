package com.example.writer;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.List;

import org.apache.iceberg.Snapshot;

import com.example.config.ConfigLoader;

/**
 * This class writes snapshot metadata from an Apache Iceberg table 
 * into the DuckLake snapshot table.
 * 
 * It checks if each snapshot already exists.
 * If not, it inserts the snapshoshot.
 */
public class DucklakeSnapshotWriter 
{
	String catalogAlias=ConfigLoader.getAlias();
	/**
	   * Writes the given snapshots to the DuckLake metadata table.
	   *
	   * @param snapshots  A list of Iceberg snapshots to insert.
	   * @param connection An open JDBC connection to the DuckLake database.
	   * @throws SQLException if a database error occurs.
	   */
  public void snapshotWriter(Connection connection,List<Snapshot> snapshots) throws SQLException 
  {
	// SQL to insert a snapshot record.
    String insertSQL = "INSERT INTO __ducklake_metadata_"+catalogAlias+".ducklake_snapshot " 
    		+ "(snapshot_id, snapshot_time, schema_version, next_catalog_id, next_file_id) "
    		+ "VALUES (?, ?, ?, ?, ?)";
    
    // Use PreparedStatement for efficiency and security.
    try (PreparedStatement pstmt = connection.prepareStatement(insertSQL)) 
    {
      for (Snapshot snapshot : snapshots) 
      {
        // If it exists, skip it.
        if (snapshotExists(snapshot,connection)) 
        {
          continue; 
        }
        pstmt.setLong(1, snapshot.snapshotId());
        pstmt.setTimestamp(2, new Timestamp(snapshot.timestampMillis()));
        pstmt.setLong(3, snapshot.schemaId());

        //  Get the next available table ID.(next_catalog_id)
        long table_id = nextAvailableTableId(connection);
        pstmt.setLong(4, table_id);

        // Get the next available data_file_id
        long next_file_id = getNextFileId(snapshot,connection);
        pstmt.setLong(5, next_file_id);
        
        pstmt.addBatch();
      }
      pstmt.executeBatch();
    }
  }
  //Checks if snapshot already exists
  public boolean snapshotExists(Snapshot snapshot,Connection connection) throws SQLException
  {
      boolean exists = false;
      String existsSQL = "SELECT 1 FROM __ducklake_metadata_"+catalogAlias+".ducklake_snapshot WHERE snapshot_id = ?";
      try (PreparedStatement pstmtExists=connection.prepareStatement(existsSQL)) 
      {
      	pstmtExists.setLong(1, snapshot.snapshotId());
          try (ResultSet rs = pstmtExists.executeQuery()) 
          {
	          if (rs.next()) 
	          {
	            exists = true;
	            return exists;
	          }
	          else
	        	  return exists;
          }
      }
  }
  
  //Gets the next available table ID.(next_catalog_id)
  public long nextAvailableTableId(Connection connection) throws SQLException 
  {
	  //long table_id = 1;
	  String getTableIdSql = "SELECT Max(table_id) FROM __ducklake_metadata_"+catalogAlias+".ducklake_table";
	  try (Statement stmt = connection.createStatement();
	       ResultSet rs = stmt.executeQuery(getTableIdSql)) 
	  {
	    if (rs.next() && rs.getLong(1) != 0) 
	      return rs.getLong(1) + 1;
	    else
	    	return 1;
	  }
  }
  
  //Get the next available data_file_id
  private long getNextFileId(Snapshot snapshot,Connection connection) throws SQLException 
  {
	  String getDataFileIdSql = "SELECT Max(data_file_id) FROM __ducklake_metadata_"+catalogAlias+".ducklake_data_file WHERE begin_snapshot = ?";
		try (PreparedStatement pstmt1 = connection.prepareStatement(getDataFileIdSql)) 
		{
			pstmt1.setLong(1, snapshot.snapshotId()); // Iceberg table name
			try (ResultSet rs = pstmt1.executeQuery()) 
			{
				if (rs.next() && rs.getLong(1) != 0) 
		          {
		            return rs.getLong(1)+1;
		          }
				else
					return 1;
			}	  
	  }
  }
  
  
}
