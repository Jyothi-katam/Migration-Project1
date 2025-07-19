package com.example.writer;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.List;

import org.apache.hadoop.thirdparty.protobuf.Field;
import org.apache.iceberg.Schema;
import org.apache.iceberg.*;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types.NestedField;

import com.example.MigrationProject1.DuckLakeTypeMapper;
import com.example.config.ConfigLoader;

/**
 * DucklakeTableWriter writes metadata about Iceberg tables and their columns into DuckLake tables in DuckDB.
 * 
 * It inserts table details, column details, and updates snapshot information.
 * It checks for duplicates before inserting to avoid writing the same table or column again.
 */
public class DucklakeTableWriter 
{
	DucklakeSnapshotWriter ducklakeSnapshotWriter=new DucklakeSnapshotWriter();
	String catalogAlias=ConfigLoader.getAlias();
	/**
     * Inserts a table entry and its columns into DuckLake metadata tables.
     * 
     * If the table already exists, it skips writing.
     * 
     * @param tableId The Iceberg table identifier
     * @param snapshots List of snapshots for the table
     * @param table The Iceberg table object
     * @param connection The database connection
     * @throws SQLException if a database error occurs
     */
	public void TableWriter(Connection connection,TableIdentifier tableId,List<Snapshot> snapshots,Table table) throws SQLException 
	{
		// Check if table with same name + schema_id already exists
	    boolean exists=tableExistsOrNot(connection,table,tableId);
	    if(exists)
	    {
	    	return;//  Exit early if table exists
	    }
		String insertSQL="INSERT INTO __ducklake_metadata_"+catalogAlias+".ducklake_table( table_id,table_uuid,begin_snapshot,end_snapshot,"
				+ "schema_id,table_name,path,path_is_relative)"
				+ "values(?,?,?,?,?,?,?,?)";
		try (PreparedStatement pstmt = connection.prepareStatement(insertSQL)) 
		{
			// Get table_id
	        long table_id = ducklakeSnapshotWriter.nextAvailableTableId(connection);
	        
			pstmt.setLong(1, table_id);
			pstmt.setString(2, table.uuid().toString());
			pstmt.setLong(3, 0);
			pstmt.setNull(4, java.sql.Types.BIGINT);
			pstmt.setLong(5, table.schema().schemaId());
			pstmt.setString(6, tableId.name());
			pstmt.setString(7, tableId.name()+"/");
			pstmt.setBoolean(8, true);
			pstmt.addBatch();
			pstmt.executeBatch();
			columnWriter(table,snapshots,table_id,connection);
		}	
	}
	
	/**
     * Inserts columns for the given table into DuckLake.
     * 
     * Checks if a column already exists before inserting.
     * 
     * @param table The Iceberg table
     * @param snapshots List of snapshots
     * @param table_id The DuckLake table ID
     * @param connection The database connection
     * @throws SQLException if a database error occurs
     */
	public void columnWriter(Table table,List<Snapshot> snapshots, Long table_id, Connection connection) throws SQLException 
	{
		
		String insertSQL="INSERT INTO __ducklake_metadata_"+catalogAlias+".ducklake_column(column_id,begin_snapshot,end_snapshot,table_id,"
				+ "column_order,column_name,column_type,initial_default,default_value,nulls_allowed,parent_column)"
				+ "values(?,?,?,?,?,?,?,?,?,?,?)";
		
		try (PreparedStatement pstmt = connection.prepareStatement(insertSQL)) 
		{
			long columnorder=0;
			List<NestedField> fields=table.schema().columns();
			for(NestedField field:fields)
			{
				//  Does the column already exist for this table?
	            boolean exists = columnExistsOrNot(connection,field,table_id);
	            if (exists) 
	            {
	            	continue; // skip writing again
	            }
				pstmt.setLong(1, field.fieldId());
				pstmt.setLong(2, 0);
				pstmt.setNull(3, java.sql.Types.BIGINT);
				pstmt.setLong(4, table_id);
				pstmt.setLong(5, columnorder);
				pstmt.setString(6, field.name());
				
				String sqlType = DuckLakeTypeMapper.mapIcebergTypeToDuckLake(field.type());
				pstmt.setString(7, sqlType.toString());
				pstmt.setString(8, null);
				pstmt.setString(9, null);
				pstmt.setBoolean(10,field.isOptional());
				pstmt.setString(11, null);
				columnorder++;
				pstmt.addBatch();
			}
			pstmt.executeBatch();
		}		
	}
	
	/**
     * Updates the begin snapshot ID for the given table and its columns if not already set.
     * 
     * @param table_id DuckLake table ID
     * @param snapshotId Snapshot ID to set
     * @param connection Database connection
     * @throws SQLException if a database error occurs
     */
	public void updateBeginSnapshot(long table_id, long snapshotId, Connection connection) throws SQLException 
	{
	    String selectSql = "SELECT begin_snapshot FROM __ducklake_metadata_"+catalogAlias+".ducklake_table WHERE table_id = ?";
	    long beginSnapshot = 0;
	    try (PreparedStatement pstmt = connection.prepareStatement(selectSql)) {
	        pstmt.setLong(1, table_id);
	        try (ResultSet rs = pstmt.executeQuery()) {
	            if (rs.next()) {
	                beginSnapshot = rs.getLong(1);
	            }
	        }
	    }

	    if (beginSnapshot == 0) {
	        // Update table
	        String updateTableSql = "UPDATE __ducklake_metadata_"+catalogAlias+".ducklake_table SET begin_snapshot = ? WHERE table_id = ?";
	        try (PreparedStatement pstmt = connection.prepareStatement(updateTableSql)) {
	            pstmt.setLong(1, snapshotId);
	            pstmt.setLong(2, table_id);
	            pstmt.executeUpdate();
	        }

	        // Update columns
	        String updateColumnSql = "UPDATE __ducklake_metadata_"+catalogAlias+".ducklake_column SET begin_snapshot = ? WHERE table_id = ?";
	        try (PreparedStatement pstmt = connection.prepareStatement(updateColumnSql)) {
	            pstmt.setLong(1, snapshotId);
	            pstmt.setLong(2, table_id);
	            pstmt.executeUpdate();
	        }
	    } 
	}
	
	//Checks if a table row already exists.
	private boolean tableExistsOrNot(Connection connection,Table table,TableIdentifier tableId) throws SQLException
	{
		String checkSql = "SELECT table_id FROM __ducklake_metadata_"+catalogAlias+".ducklake_table WHERE table_name = ? AND schema_id = ?";
	    try (PreparedStatement checkStmt = connection.prepareStatement(checkSql)) 
	    {
	        checkStmt.setString(1, tableId.name());
	        checkStmt.setLong(2, table.schema().schemaId());
	        try (ResultSet rs = checkStmt.executeQuery())
	        {
	            return rs.next();
	        }
	    }
	}
	
	//Checks if a column row already exists.
	private boolean columnExistsOrNot(Connection connection,NestedField field,long table_id) throws SQLException
	{
		String checkSql = "SELECT 1 FROM __ducklake_metadata_"+catalogAlias+".ducklake_column " +
                "WHERE column_id = ? AND table_id = ?";
		try (PreparedStatement checkStmt = connection.prepareStatement(checkSql)) 
		{
		  checkStmt.setLong(1, field.fieldId());
		  checkStmt.setLong(2, table_id);
		  try (ResultSet rs = checkStmt.executeQuery()) 
		  {
		      return rs.next();
		  }
		}
	}
}
