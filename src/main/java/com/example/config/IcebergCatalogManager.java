package com.example.config;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.jdbc.JdbcCatalog;

public class IcebergCatalogManager 
{
	public static Catalog loadCatalog()
	{
		Configuration hadoopConf = new Configuration();
		hadoopConf.set("fs.s3a.access.key", ConfigLoader.get("minio.access.key"));
		hadoopConf.set("fs.s3a.secret.key", ConfigLoader.get("minio.secret.key"));
		hadoopConf.set("fs.s3a.endpoint", ConfigLoader.get("minio.endpoint"));
		hadoopConf.set("fs.s3a.path.style.access", ConfigLoader.get("minio.path.style.access"));
		hadoopConf.set("fs.s3a.connection.ssl.enabled", ConfigLoader.get("minio.connection.ssl.enabled"));
		hadoopConf.set("fs.s3.impl", ConfigLoader.get("hadoop.fs.s3.impl"));
	
		Map<String, String> props = new HashMap<>();
		props.put("uri", ConfigLoader.get("iceberg.jdbc.uri"));
		props.put("jdbc.user", ConfigLoader.get("iceberg.jdbc.user"));
		props.put("jdbc.password", ConfigLoader.get("iceberg.jdbc.password"));
		props.put("warehouse", ConfigLoader.get("iceberg.warehouse"));
	
		Catalog catalog = new JdbcCatalog();
		((JdbcCatalog) catalog).setConf(hadoopConf);
		catalog.initialize("demo", props);
		return catalog;
	}

}
