package com.eakonovalov.storm.reconciliation;

import org.apache.commons.dbcp2.BasicDataSource;

import javax.sql.DataSource;

public class ConnectionPool {

    public static final String DRIVER = "com.mysql.cj.jdbc.Driver";
    public static final String URL = "jdbc:mysql://localhost:3306/smartr?useSSL=false";
    public static final String USER = "root";
    public static final String PASSWORD = "root";

    private static ConnectionPool instance;
    private BasicDataSource pool;

    public static ConnectionPool getInstance() {
        if(instance == null) {
            synchronized (ConnectionPool.class) {
                if(instance == null) {
                    instance = new ConnectionPool(URL, USER, PASSWORD);
                }
            }
        }

        return instance;
    }

    private ConnectionPool(String url, String user, String password) {
        pool = new BasicDataSource();
        pool.setUrl(url);
        pool.setUsername(user);
        pool.setPassword(password);
        pool.setDriverClassName(DRIVER);
        pool.setInitialSize(1);
        pool.setMaxTotal(5);
    }

    public DataSource getDataSource() {
        return pool;
    }

}
