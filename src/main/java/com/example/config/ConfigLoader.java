package com.example.config;

import java.io.InputStream;
import java.util.Properties;

/**
 * ConfigLoader loads application configuration from the {@code application.properties} file.
 * 
 * It provides static methods to retrieve configuration values, including the DuckLake catalog alias.
 */
public class ConfigLoader {
    private static final Properties props = new Properties();
    private static  String catalogAlias ;
    
    // Static block to load properties when the class is loaded.
    static 
    {
        try (InputStream input = ConfigLoader.class.getClassLoader().getResourceAsStream("application.properties")) 
        {
            props.load(input);
            catalogAlias = props.getProperty("ducklake.catalog.alias");
            if (catalogAlias == null || catalogAlias.isEmpty()) 
            {
                throw new RuntimeException("ducklake.catalog.alias not found in properties!");
            }
        }
        catch (Exception e) 
        {
            throw new RuntimeException("Could not load application.properties", e);
        } 
    }
    
    //Gets the value of a property by key.
    public static String get(String key) 
    {
        return props.getProperty(key);
    }
    
    // Gets the DuckLake catalog alias.
    public static String getAlias() 
    {
        return catalogAlias;
    }
}