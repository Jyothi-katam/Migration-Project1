package com.example.writer;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;

import com.example.config.ConfigLoader;

/**
 * DucklakeDataFileWriter writes Iceberg table data file metadata into the DuckLake data file table.
 * 
 * It inserts information about data files added in snapshots and updates data file end snapshots
 * if files are removed.
 */
public class DucklakeDataFileWriter 
{
	DucklakeTableWriter ducklakeTableWriter=new DucklakeTableWriter();
	String catalogAlias=ConfigLoader.getAlias();
	
	/**
     * Writes data file information from an Iceberg table into the DuckLake data file table.
     * 
     * For each snapshot in the table:
     *  - It inserts added data files if they do not already exist.
     *  - It updates end snapshot values for removed files.
     * 
     * @param table The Iceberg table
     * @param tableId The table identifier
     * @param connection The database connection
     * @throws SQLException if a database error occurs
     */
	public void dataFileWriter(Connection connection,Table table, TableIdentifier tableId) throws SQLException 
	{
		String insertSQL="INSERT INTO __ducklake_metadata_"+catalogAlias+".ducklake_data_file( data_file_id ,table_id,begin_snapshot,end_snapshot,file_order,"
				+ " path, path_is_relative,"+ "file_format,record_count,file_size_bytes,footer_size ,row_id_start,partition_id,encryption_key,"
				+ "partial_file_info,mapping_id)"+ "values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
	    try(PreparedStatement pstmt=connection.prepareStatement(insertSQL))
	    {
	    	long data_file_id=getDataFileId(connection);
			List<Snapshot> snapshots=(List<Snapshot>) table.snapshots();
			for(Snapshot snapshot:snapshots)
			{
				List<DataFile> dataFiles = (List<DataFile>) snapshot.addedDataFiles(table.io());
				for (DataFile dataFile: dataFiles) 
				{
					long table_id=getTableId(connection,table);
				    String file_path = dataFile.path().toString();
	                file_path = file_path.substring(file_path.lastIndexOf("data"));
	                
	                boolean exists=dataFileExistsOrNot(connection,file_path,table_id,snapshot);
	                if(exists)
	                {
	                	break;// DataFile already there --> skip writing 
	                }
	                
				    pstmt.setLong(1, data_file_id++); //data_file_id
				    pstmt.setLong(2, table_id); //table_id
				    pstmt.setLong(3, snapshot.snapshotId()); //begin_snapshot
				    pstmt.setNull(4, java.sql.Types.BIGINT); //end_snapshot
				    pstmt.setNull(5, java.sql.Types.BIGINT); //file_order
				    pstmt.setString(6, file_path); //path 
				    pstmt.setBoolean(7, true); //path_is_relative
				    pstmt.setString(8, dataFile.format().toString()); //file_format
				    pstmt.setLong(9, dataFile.recordCount()); //record_count
				    pstmt.setLong(10, dataFile.fileSizeInBytes()); //file_size_bytes
				    pstmt.setNull(11, java.sql.Types.BIGINT); //footer_size
				    pstmt.setLong(12, 0); //row_id_start
				    pstmt.setLong(13,table.spec().specId()); //partition_id
				    pstmt.setString(14, null); //encryption_key
				    pstmt.setString(15, null); //partial_file_info
				    pstmt.setNull(16, java.sql.Types.BIGINT); //mapping_id
				    
				    pstmt.addBatch();
				    	
				    // Update begin snapshot in the DuckLake table and columns
				    ducklakeTableWriter.updateBeginSnapshot(table_id,snapshot.snapshotId(),connection);  	
				}
			pstmt.executeBatch();
				
			// Update end snapshot for removed files
			List<DataFile> removedFiles = (List<DataFile>) snapshot.removedDataFiles(table.io());
			   for (DataFile removedFile : removedFiles) 
			   {
				   	String updateSQL = "UPDATE __ducklake_metadata_"+catalogAlias+".ducklake_data_file "
			                         + "SET end_snapshot = ? WHERE path = ? AND table_id = ?";
				   	try (PreparedStatement updateStmt = connection.prepareStatement(updateSQL)) 
			       {
				   		updateStmt.setLong(1, snapshot.snapshotId());

				   		// Use the relative path logic (same as you do for insert!)
				   		String removedFilePath = removedFile.path().toString();
				   		removedFilePath = removedFilePath.substring(removedFilePath.lastIndexOf("data"));

				   		updateStmt.setString(2, removedFilePath);
				   		long table_id=getTableId(connection,table);
				   		updateStmt.setLong(3, table_id);  // reuse the same table_id you got above
				   		updateStmt.executeUpdate();
			       }
			   }
			}
		} 
	}
	
	//Check if dataFile already exists or not
	private boolean dataFileExistsOrNot(Connection connection,String file_path,long table_id,Snapshot snapshot) throws SQLException
	{
		String checkSql = "SELECT 1 FROM __ducklake_metadata_"+catalogAlias+".ducklake_data_file WHERE table_id = ? AND path = ? AND begin_snapshot = ?";
        try (PreparedStatement checkStmt = connection.prepareStatement(checkSql)) 
        {
        	checkStmt.setLong(1, table_id);
            checkStmt.setString(2, file_path);
            checkStmt.setLong(3, snapshot.snapshotId());
            try (ResultSet rs = checkStmt.executeQuery()) 
            {
            	return rs.next();
            }
        }
	}
	
	//Gets table Id
	private long getTableId(Connection connection,Table table) throws SQLException
	{
		String getTableIdSql = "SELECT table_id FROM __ducklake_metadata_"+catalogAlias+".ducklake_table WHERE table_uuid = ?";
	    try (PreparedStatement pstmt1 = connection.prepareStatement(getTableIdSql)) 
	    {
	    	pstmt1.setString(1, table.uuid().toString()); // Iceberg table UUID
	    	try (ResultSet rs = pstmt1.executeQuery()) 
	    	{
	    		if (rs.next()) 
	    		{
	    		   return rs.getLong(1);
	    		}
	    		else
	    		{
	    			return 0;
	    		}
	    	}
	    }
	}
	
	//Gets Data File ID
	private long getDataFileId(Connection connection) throws SQLException
	{	
    	try (Statement stmt = connection.createStatement();
	             ResultSet rs = stmt.executeQuery("SELECT MAX(data_file_id) FROM __ducklake_metadata_"+catalogAlias+".ducklake_data_file")) 
	        {
	          if (rs.next() && !rs.wasNull()) {
	            return rs.getLong(1) + 1;
	          }
	          else
	          {
	        	  return 1;
	          }
	        }
	}
}
	
            