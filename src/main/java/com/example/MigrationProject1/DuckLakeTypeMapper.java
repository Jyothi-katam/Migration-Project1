package com.example.MigrationProject1;

import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

public class DuckLakeTypeMapper 
{

    public static String mapIcebergTypeToDuckLake(Type type) 
    {
        switch (type.typeId()) {
            case INTEGER:
                return "INT32"; // DuckLake spec
            case LONG:
                return "BIGINT";
            case FLOAT:
                return "FLOAT";
            case DOUBLE:
                return "DOUBLE";
            case BOOLEAN:
                return "BOOLEAN";
            case STRING:
                return "VARCHAR";
            case BINARY:
                return "BLOB";
            case DATE:
                return "DATE";
            case TIME:
                return "TIME";
            case TIMESTAMP:
                return "TIMESTAMP WITH TIME ZONE";
            case DECIMAL:
                Types.DecimalType decimal = (Types.DecimalType) type;
                return "DECIMAL(" + decimal.precision() + "," + decimal.scale() + ")";
            case MAP:
                return type.toString(); // e.g. map<string, int>
            case LIST:
                return type.toString(); // e.g. list<struct<...>>
            case STRUCT:
                return type.toString(); // e.g. struct<id: int, name: string>
            default:
                throw new IllegalArgumentException("Unsupported Iceberg type: " + type);
        }
    }
}
