package com.vesoft.nebula.jdbc;

import java.sql.*;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;

public abstract class NebulaAbstractDriver implements Driver {

    protected static final String JDBC_PREFIX = "jdbc:nebula://";

    protected Properties poolProperties = new Properties();

    protected Properties connectionConfig = new Properties();

    protected NebulaAbstractDriver(){}

    //查询驱动程序是否认为它可以打开到给定 url 的连接
    @Override
    public boolean acceptsURL(String url) throws SQLException {
        if(url==null){
            throw new SQLException("Invalid url!");
        }
        String[] split = url.split("//");
        if(url.startsWith(JDBC_PREFIX)&&split.length==2){
            return true;
        }
        return false;
    }

    protected void parseUrlProperties(String url,Properties connectionConfig){
        Properties parseUrlGetConfig = new Properties();

        if(connectionConfig!=null){
            for (Map.Entry<Object, Object> entry : connectionConfig.entrySet()) {
                parseUrlGetConfig.put(entry.getKey().toString().toLowerCase(),entry.getValue());
            }
        }

        String graphSpace = url.split("//")[1];
        parseUrlGetConfig.put("graphSpace",graphSpace);
        parseUrlGetConfig.put("url",url);
        this.connectionConfig = parseUrlGetConfig;
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        return new DriverPropertyInfo[0];
    }

    @Override
    public int getMajorVersion() {
        return 1;
    }

    @Override
    public int getMinorVersion() {
        return 0;
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return null;
    }
}
