package com.example.MigrationProject1;

import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;

import com.example.config.IcebergCatalogConnectionManager;
import com.example.reader.IcebergTableMetadataReader;

import java.sql.SQLException;
import java.util.List;

public class App 
{
  public static void main(String[] args) throws SQLException 
  {
	  Catalog icebergCatalog=IcebergCatalogConnectionManager.loadCatalog();
	  
	  List<TableIdentifier> tables=icebergCatalog.listTables(Namespace.of("default"));	  
	  IcebergTableMetadataReader reader=new IcebergTableMetadataReader();
	  reader.icebergMetadataReader(tables);
  }  
}
