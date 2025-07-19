package com.example.writer;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import org.apache.iceberg.PartitionField;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table; 

import com.example.config.ConfigLoader;

/**
 * DucklakePartitionWriter handles writing partition metadata for an Iceberg table
 * into the DuckLake partition_info and partition_column tables.
 *
 * This class ensures duplicate partition or column entries are not inserted.
 */
public class DucklakePartitionWriter 
{
	String catalogAlias=ConfigLoader.getAlias();
	
	/**
     * Writes partition info for the given Iceberg table into DuckLake metadata.
     * If the partition info for the table and partition ID already exists,
     * it skips inserting again.
     *
     * After inserting partition info, it also writes partition columns.
     *
     * @param connection The database connection
     * @param table The Iceberg table
     * @throws SQLException if a database error occurs
     */
	public void partitionInfoWriter(Connection connection, Table table) throws SQLException
	{
		// Check if this partition_info already exists
	    if (partitionInfoExists(connection, table.spec().specId(), getTableId(connection,table))) 
	    {
	        return; // Skip insert
	    }
		String insertSql="insert into __ducklake_metadata_"+catalogAlias+".ducklake_partition_info(partition_id,table_id,begin_snapshot,end_snapshot) "
							+ "values(?,?,?,?)";
		try(PreparedStatement pstmt=connection.prepareStatement(insertSql))
		{
			
			pstmt.setLong(1, table.spec().specId());
			
			long table_id=getTableId(connection,table);
			pstmt.setLong(2, table_id);
			
			List<Snapshot> snapshots=(List<Snapshot>) table.snapshots();
			long snapshotId=0;
			for(Snapshot snapshot:snapshots) 
			{
				if(table.spec().isPartitioned())
					snapshotId=snapshot.snapshotId();
				break;
			}
			pstmt.setLong(3, snapshotId);
			pstmt.setNull(4, java.sql.Types.BIGINT);
			
			pstmt.addBatch();
			pstmt.executeBatch();
			partitionColumnWriter(connection,table);
		}
	}
	
	/**
     * Writes partition columns for the given table's partition spec.
     * Only columns that do not already exist are inserted.
     *
     * @param connection The database connection
     * @param table The Iceberg table
     * @throws SQLException if a database error occurs
     */
	public void partitionColumnWriter(Connection connection,Table table) throws SQLException
	{
		String inserSql="INSERT INTO __ducklake_metadata_"+catalogAlias+".ducklake_partition_column(partition_id,table_id,partition_key_index,column_id,transform)"
				+ "VALUES(?,?,?,?,?)";
		try(PreparedStatement pstmt=connection.prepareStatement(inserSql))
		{
			pstmt.setLong(1, table.spec().specId());
			long table_id = getTableId(connection, table);

            List<PartitionField> fields = table.spec().fields();
            int index = 0;

            for (PartitionField field : fields) 
            {
            	long column_id = getColumnId(connection, field.name(), table_id);
            	// Check for duplicates
                if (partitionColumnExists(connection, table.spec().specId(), table_id, column_id)) {
                    continue; // Skip insert
                }
                pstmt.setLong(1, table.spec().specId());
                pstmt.setLong(2, table_id);
                pstmt.setInt(3, index);
                pstmt.setLong(4, column_id);
                pstmt.setString(5, field.transform().toString());

                pstmt.addBatch();
                index++;
            }

            pstmt.executeBatch();
		}
	}
	
	
    //Looks up the DuckLake table ID for the given Iceberg table.
	private long getTableId(Connection connection,Table table) throws SQLException
	{
		long table_id=0;
    	String getTableIdSql = "SELECT table_id FROM __ducklake_metadata_"+catalogAlias+".ducklake_table WHERE table_uuid = ?";
    	try (PreparedStatement getTableIdstmt = connection.prepareStatement(getTableIdSql)) 
    	{
    		getTableIdstmt.setString(1, table.uuid().toString()); // Iceberg table name
    		try (ResultSet rs = getTableIdstmt.executeQuery()) 
    		{
    		  if (rs.next()) 
    		  {
    		    return rs.getLong(1);
    		  }
    		  else
    		  {
    		    	return table_id;
    		  }
    		}
    	}
	}
	
	//Looks up the column ID for a column name and table_id.
	private long getColumnId(Connection connection,String columnName,long table_id) throws SQLException
	{
		String getColumnIdSql="SELECT column_id FROM __ducklake_metadata_"+catalogAlias+".ducklake_column WHERE column_name = ? AND table_id = ?";
		try (PreparedStatement stmt = connection.prepareStatement(getColumnIdSql)) {
            stmt.setString(1, columnName);
            stmt.setLong(2, table_id);
            try (ResultSet rs = stmt.executeQuery()) 
            {
                if (rs.next()) 
                {
                    return rs.getLong(1);
                } else {
                    throw new SQLException("Column ID not found for: " + columnName + " in table_id: " + table_id);
                }
            }
        }
	}
	
	//Checks if a partition info row already exists.
	private boolean partitionInfoExists(Connection connection, long partitionId, long tableId) throws SQLException {
	    String checkSql = "SELECT 1 FROM __ducklake_metadata_" + catalogAlias + ".ducklake_partition_info WHERE partition_id = ? AND table_id = ?";
	    try (PreparedStatement stmt = connection.prepareStatement(checkSql)) {
	        stmt.setLong(1, partitionId);
	        stmt.setLong(2, tableId);
	        try (ResultSet rs = stmt.executeQuery()) {
	            return rs.next();
	        }
	    }
	}
	
	
    //Checks if a partition column row already exists.
	private boolean partitionColumnExists(Connection connection, long partitionId, long tableId, long columnId) throws SQLException {
	    String checkSql = "SELECT 1 FROM __ducklake_metadata_" + catalogAlias + ".ducklake_partition_column WHERE partition_id = ? AND table_id = ? AND column_id = ?";
	    try (PreparedStatement stmt = connection.prepareStatement(checkSql)) {
	        stmt.setLong(1, partitionId);
	        stmt.setLong(2, tableId);
	        stmt.setLong(3, columnId);
	        try (ResultSet rs = stmt.executeQuery()) {
	            return rs.next();
	        }
	    }
	}

}
