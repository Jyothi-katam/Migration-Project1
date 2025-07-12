package com.example.config;

import java.io.InputStream;
import java.util.Properties;

public class ConfigLoader {
    private static final Properties props = new Properties();

    static {
        try (InputStream input = ConfigLoader.class.getClassLoader().getResourceAsStream("application.properties")) {
            props.load(input);
        } catch (Exception e) {
            throw new RuntimeException("Could not load application.properties", e);
        }
    }

    public static String get(String key) {
        return props.getProperty(key);
    }
}