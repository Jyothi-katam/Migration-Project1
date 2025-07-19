package com.example.writer;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.UUID;

import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types.NestedField;

import com.example.config.ConfigLoader;

/**
 * DucklakeSchemaWriter writes the Iceberg table's namespace schema information
 * into the DuckLake metadata schema table.
 *
 * It ensures that the same schema (namespace) is not written more than once.
 */
public class DucklakeSchemaWriter 
{
	String catalogAlias=ConfigLoader.getAlias();
	
	/**
     * Writes the schema (namespace) of the given Iceberg table to the DuckLake schema table.
     *
     * This method first checks if the schema already exists by schema name.
     * If it does not exist, it inserts a new row with a generated UUID.
     *
     * @param tableId The Iceberg table identifier (contains namespace)
     * @param table The Iceberg table (not directly used here except for context)
     * @param connection The active database connection
     * @throws SQLException if a database access error occurs
     */
	public void schemaWriter(TableIdentifier tableId,Table table, Connection connection) throws SQLException 
	{
		boolean exists = schemaExistsOrNot(connection,tableId.namespace().toString());
        if (exists) 
        {
        	return; // skip writing schema again
        }
		String insertSQL="INSERT INTO __ducklake_metadata_"+catalogAlias+".ducklake_schema( schema_id,schema_uuid,begin_snapshot,end_snapshot,"
						 + "schema_name,path,path_is_relative)" + "values(?,?,?,?,?,?,?)";
		try (PreparedStatement pstmt = connection.prepareStatement(insertSQL)) 
		{
			long schema_id = getSchemaId(connection);
			pstmt.setLong(1,schema_id);
			pstmt.setString(2, UUID.randomUUID().toString());
			pstmt.setLong(3, 0);
			pstmt.setString(4, null);
			pstmt.setString(5,tableId.namespace().toString());
			pstmt.setString(6,tableId.namespace().toString()+"/");
			pstmt.setBoolean(7, true);
			pstmt.addBatch();
			pstmt.executeBatch();
		}
		
	}
	
	//Checks if the schema with the given name already exists in the DuckLake metadata table.
	private boolean schemaExistsOrNot(Connection connection,String schemaName) throws SQLException
	{
		boolean exists=false;
		String checkSql = "SELECT 1 FROM __ducklake_metadata_"+catalogAlias+".ducklake_schema " +
	                "WHERE schema_name = ?";
		try (PreparedStatement checkStmt = connection.prepareStatement(checkSql)) 
		{
			checkStmt.setString(1, schemaName);
			try (ResultSet rs = checkStmt.executeQuery()) 
			{
				if (rs.next()) 
			    {
					exists = true; // Schema with same name already there → skip inserting again
			        return exists;
			    }
			    else
			    {
			    	return exists;
			    }
			}
		}
	}
	
	// Gets the next available schema_id.
	public long getSchemaId(Connection connection) throws SQLException
	{
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT MAX(schema_id) FROM __ducklake_metadata_"+catalogAlias+".ducklake_schema")) {
          if (rs.next()) 
          {
            return rs.getLong(1) + 1;
          }
          else 
          {
        	  return 1;
          }
        }
	}
}
