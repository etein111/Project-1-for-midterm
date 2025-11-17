package com;
import java.lang.reflect.InvocationTargetException;

public class DataFactory {
    public DataManipulation createDataManipulation(String arg) {
        String name;
        if (arg.toLowerCase().equals("file")) {
            name = "com.FileManipulation";
        } else if (arg.toLowerCase().equals("database")) {
            name = "com.DatabaseManipulation";
        } else if (arg.toLowerCase().equals("mysql")) {
            name = "com.MySQLDatabaseManipulation";
        } else {
            throw new IllegalArgumentException("Illegal Argument:" + arg);
        }
        try {
            return (DataManipulation) Class.forName(name).getDeclaredConstructor().newInstance();
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        return null;
    }
}
