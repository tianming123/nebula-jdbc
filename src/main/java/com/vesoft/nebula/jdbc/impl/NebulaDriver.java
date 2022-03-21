package com.vesoft.nebula.jdbc.impl;

import com.vesoft.nebula.client.graph.NebulaPoolConfig;
import com.vesoft.nebula.client.graph.data.HostAddress;
import com.vesoft.nebula.client.graph.exception.*;
import com.vesoft.nebula.client.graph.net.NebulaPool;
import com.vesoft.nebula.client.graph.net.Session;
import com.vesoft.nebula.jdbc.NebulaAbstractDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class NebulaDriver extends NebulaAbstractDriver {

    private static String defaultIp = "127.0.0.1";
    private static int defaultPort = 9669;
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private NebulaPoolConfig nebulaPoolConfig;
    private NebulaPool nebulaPool;

    private void setDefaultPoolProperties(){
        Properties defaultPoolProperties = new Properties();

        ArrayList<HostAddress> addressList = new ArrayList<>();
        addressList.add(new HostAddress(defaultIp,defaultPort));

        defaultPoolProperties.put("addressList", addressList);
        defaultPoolProperties.put("minConnsSize", 0);
        defaultPoolProperties.put("maxConnsSize", 10);
        defaultPoolProperties.put("timeout", 0);
        defaultPoolProperties.put("idleTime", 0);
        defaultPoolProperties.put("intervalIdle", -1);
        defaultPoolProperties.put("waitTime", 0);
    }

    private void initNebulaPool() throws UnknownHostException, SQLException {
        List<HostAddress> defaultAddressList = new ArrayList<>();
        defaultAddressList.add(new HostAddress(defaultIp,defaultPort));
        List<HostAddress> addressList = (List<HostAddress>) poolProperties.getOrDefault("addressList",defaultAddressList);

        int minConnsSize = (int) poolProperties.getOrDefault("minConnsSize", 0);
        int maxConnsSize = (int) poolProperties.getOrDefault("maxConnsSize", 10);
        int timeout = (int) poolProperties.getOrDefault("timeout", 0);
        int idleTime = (int) poolProperties.getOrDefault("idleTime", 0);
        int intervalIdle = (int) poolProperties.getOrDefault("intervalIdle", -1);
        int waitTime = (int) poolProperties.getOrDefault("waitTime", 0);

        nebulaPoolConfig = new NebulaPoolConfig();
        nebulaPoolConfig.setMinConnSize(minConnsSize);
        nebulaPoolConfig.setMaxConnSize(maxConnsSize);
        nebulaPoolConfig.setTimeout(timeout);
        nebulaPoolConfig.setIdleTime(idleTime);
        nebulaPoolConfig.setIntervalIdle(intervalIdle);
        nebulaPoolConfig.setWaitTime(waitTime);

        try {
            long start = System.currentTimeMillis();
            this.nebulaPool.init(addressList,nebulaPoolConfig);
            long end = System.currentTimeMillis();
            logger.info("NebulaPool.init(addressList, nebulaPoolConfig) use " +  (end - start) + " ms");
        }catch (UnknownHostException | InvalidConfigException e){
            throw new SQLException(e);
        }
    }

    protected Session getSessionFromNebulaPool() throws SQLException {
        String user = (String) connectionConfig.getOrDefault("user", "root");
        String password = (String) connectionConfig.getOrDefault("password", "nebula");
        boolean reconnect = (boolean) connectionConfig.getOrDefault("reconnect", false);

        Session nebulaSession;
        try{
            nebulaSession = nebulaPool.getSession(user, password, reconnect);
        }catch (NotValidConnectionException | IOErrorException | AuthFailedException | ClientServerIncompatibleException e){
            throw new SQLException(e);
        }
        return nebulaSession;

    }

    public NebulaDriver() throws SQLException, UnknownHostException {
        this.setDefaultPoolProperties();
        this.initNebulaPool();
        DriverManager.registerDriver(this);
    }

    public NebulaDriver(Properties properties) throws SQLException, UnknownHostException {
        this.poolProperties = properties;
        this.initNebulaPool();
        DriverManager.registerDriver(this);
    }

    public NebulaDriver(String address) throws SQLException, UnknownHostException {
        String[] addressInfo = address.split(":");
        if(address.length()!=2){
            throw new SQLException(String.format("url [%s] is invalid, please make sure your url match thr format: \"ip:port\".", address));
        }
        String ip = addressInfo[0];
        int port = Integer.parseInt(addressInfo[1]);
        List<HostAddress> userAddressList = new ArrayList<>();
        userAddressList.add(new HostAddress(ip,port));

        this.poolProperties.put("addressList",userAddressList);
        initNebulaPool();
        DriverManager.registerDriver(this);
    }

    public void closePool(){
        nebulaPoolConfig = null;
        nebulaPool.close();
        nebulaPool = null;
        logger.info("NebulaDriver is closed");
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        if(this.acceptsURL(url)){
            parseUrlProperties(url,connectionConfig);
            this.connectionConfig.put("url",url);
            String graphSpace = this.connectionConfig.getProperty("graphSpace");
            NebulaConnection connection = new NebulaConnection(this, graphSpace);
            logger.info("Get JDBCConnection succeeded");
            return connection;
        }
        else{
            throw new SQLException("url: " + url + " is not accepted, " +
                    "url example: jdbc:nebula://graphSpace " +
                    "make sure your url match this format.");
        }
    }

    public Properties getPoolProperties(){
        return this.poolProperties;
    }
    public NebulaPoolConfig getNebulaPoolConfig(){
        return this.nebulaPoolConfig;
    }
    public Properties getConnectionConfig(){
        return this.connectionConfig;
    }
}
