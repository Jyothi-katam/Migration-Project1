package com.example.reader;

import java.util.List;

import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;

import java.sql.Connection;
import java.sql.SQLException;

import com.example.config.DucklakeConnectionManager;
import com.example.config.IcebergCatalogConnectionManager;
import com.example.writer.DucklakeDataFileWriter;
import com.example.writer.DucklakePartitionWriter;
import com.example.writer.DucklakeSchemaWriter;
import com.example.writer.DucklakeSnapshotWriter;
import com.example.writer.DucklakeTableWriter;

/**
 * This class reads metadata from Iceberg tables and writes it into DuckLake metadata tables in DuckDB.
 * 
 * It connects to DuckDB, loads each Iceberg table, checks if there are any snapshots,
 * and if snapshots exist, it writes schema, table, snapshot, partition, and data file information. 
 */
public class IcebergTableMetadataReader 
{
	private DucklakeDataFileWriter ducklakeDataFileWriter=new DucklakeDataFileWriter();
	private DucklakeSnapshotWriter ducklaeSnapshotWriter=new DucklakeSnapshotWriter();
	private DucklakeTableWriter ducklakeTableWriter=new DucklakeTableWriter();
	private DucklakeSchemaWriter ducklakeSchemaWriter=new DucklakeSchemaWriter();
	private DucklakePartitionWriter ducklakePartitionWriter=new DucklakePartitionWriter();
	
	Catalog icebergCatalog=IcebergCatalogConnectionManager.loadCatalog();
	
	/**
     * Reads metadata for the given Iceberg tables and writes it to DuckDB.
     * 
     * For each table, it checks if snapshots exist.
     * If they do, it writes table info, snapshot info, and data file info.
     * 
     * @param tables List of Iceberg table identifiers
     * @throws SQLException if there is a database error
     */
	public void icebergMetadataReader(List<TableIdentifier> tables) throws SQLException 
	{
		Connection connection=DucklakeConnectionManager.duckdbConnection();
		for(TableIdentifier tableId:tables)
		{
			Table table=icebergCatalog.loadTable(tableId);
			
			List<Snapshot> snapshots=(List<Snapshot>) table.snapshots();
			if(!snapshots.isEmpty())
			{
				ducklakeSchemaWriter.schemaWriter(tableId,table,connection);
				ducklakeTableWriter.TableWriter(connection,tableId,snapshots,table);
				ducklakeDataFileWriter.dataFileWriter(connection,table,tableId);
				ducklakePartitionWriter.partitionInfoWriter(connection,table);
				ducklaeSnapshotWriter.snapshotWriter(connection,snapshots);
			}
			else
			{
				continue;
			}
		}
		System.out.println("........Inserted values........... ");
	}
}
