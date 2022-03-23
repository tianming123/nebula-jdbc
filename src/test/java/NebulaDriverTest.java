
import com.vesoft.nebula.client.graph.NebulaPoolConfig;
import com.vesoft.nebula.client.graph.data.HostAddress;
import com.vesoft.nebula.jdbc.impl.NebulaConnection;
import com.vesoft.nebula.jdbc.impl.NebulaDriver;
import com.vesoft.nebula.jdbc.impl.NebulaResultSet;
import org.junit.jupiter.api.Test;

import java.net.UnknownHostException;
import java.sql.*;
import java.util.ArrayList;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Attention: Please run {@link RunMeBeforeTest#createTestGraphSpace()} if you do not run that method before,
 * it will create graph space for test and insert data into it.
 */

public class NebulaDriverTest {

    @Test
    public void getDefaultDriverTest() throws SQLException, UnknownHostException {

        Properties defaultPoolProperties = new Properties();
        String defaultIp = "127.0.0.1";
        int defaultPort = 9669;
        ArrayList<HostAddress> addressList = new ArrayList<>();
        addressList.add(new HostAddress(defaultIp, defaultPort));


        defaultPoolProperties.put("addressList", addressList);
        defaultPoolProperties.put("minConnsSize", 0);
        defaultPoolProperties.put("maxConnsSize", 10);
        defaultPoolProperties.put("timeout", 0);
        defaultPoolProperties.put("idleTime", 0);
        defaultPoolProperties.put("intervalIdle", -1);
        defaultPoolProperties.put("waitTime", 0);

        NebulaDriver defaultDriver = new NebulaDriver();
        Properties properties = defaultDriver.getPoolProperties();

        assertEquals(defaultPoolProperties, properties);

        Properties connectionConfig = new Properties();
        connectionConfig.put("user", RunMeBeforeTest.USERNAME);
        connectionConfig.put("password", RunMeBeforeTest.PASSWORD);
        connectionConfig.put("reconnect", true);

        NebulaConnection connection1 = (NebulaConnection)defaultDriver.connect(RunMeBeforeTest.URL, connectionConfig);
        NebulaConnection connection2 = (NebulaConnection)DriverManager.getConnection(RunMeBeforeTest.URL, RunMeBeforeTest.USERNAME, RunMeBeforeTest.PASSWORD);
        NebulaConnection connection3 = (NebulaConnection)DriverManager.getConnection(RunMeBeforeTest.URL, connectionConfig);
        DriverManager.deregisterDriver(defaultDriver);

        assertEquals(connection1.getClientInfo(), connection2.getClientInfo());
        assertNotEquals(connection1.getConnectionConfig(), connection2.getConnectionConfig());
        assertEquals(connection1.getConnectionConfig(), connection3.getConnectionConfig());

        defaultDriver.closePool();
    }


    @Test
    public void getCustomizedDriverTest() throws SQLException, UnknownHostException {

        Properties poolProperties = new Properties();
        ArrayList<HostAddress> addressList = new ArrayList<>();
        addressList.add(new HostAddress(RunMeBeforeTest.IP, RunMeBeforeTest.PORT));
        //addressList.add(new HostAddress("127.0.0.1", 9670));

        poolProperties.put("addressList", addressList);
        poolProperties.put("minConnsSize", 2);
        poolProperties.put("maxConnsSize", 12);
        poolProperties.put("timeout", 1015);
        poolProperties.put("idleTime", 727);
        poolProperties.put("intervalIdle", 1256);
        poolProperties.put("waitTime", 1256);

        NebulaDriver customizedDriver = new NebulaDriver(poolProperties);
        NebulaPoolConfig nebulaPoolConfig = customizedDriver.getNebulaPoolConfig();

        assertEquals(poolProperties.getOrDefault("minConnsSize", 0), nebulaPoolConfig.getMinConnSize());
        assertEquals(poolProperties.getOrDefault("maxConnsSize", 10), nebulaPoolConfig.getMaxConnSize());
        assertEquals(poolProperties.getOrDefault("timeout", 0), nebulaPoolConfig.getTimeout());
        assertEquals(poolProperties.getOrDefault("idleTime", 0), nebulaPoolConfig.getIdleTime());
        assertEquals(poolProperties.getOrDefault("intervalIdle", 0), nebulaPoolConfig.getIntervalIdle());
        assertEquals(poolProperties.getOrDefault("waitTime", 0), nebulaPoolConfig.getWaitTime());

        Properties connectionConfig = new Properties();
        connectionConfig.put("user", RunMeBeforeTest.USERNAME);
        connectionConfig.put("password", RunMeBeforeTest.PASSWORD);
        connectionConfig.put("reconnect", true);

        NebulaConnection connection1 = (NebulaConnection)customizedDriver.connect(RunMeBeforeTest.URL, connectionConfig);
        NebulaConnection connection2 = (NebulaConnection)DriverManager.getConnection(RunMeBeforeTest.URL, RunMeBeforeTest.USERNAME, RunMeBeforeTest.PASSWORD);
        NebulaConnection connection3 = (NebulaConnection)DriverManager.getConnection(RunMeBeforeTest.URL, connectionConfig);
        DriverManager.deregisterDriver(customizedDriver);

        assertEquals(connection1.getClientInfo(), connection2.getClientInfo());
        assertNotEquals(connection1.getConnectionConfig(), connection2.getConnectionConfig());
        assertEquals(connection1.getConnectionConfig(), connection3.getConnectionConfig());

        customizedDriver.closePool();
    }


    @Test
    public void getCustomizedUrlDriverTest() throws SQLException, UnknownHostException {
        NebulaDriver customizedUrlDriver = new NebulaDriver(RunMeBeforeTest.IP + ":" + RunMeBeforeTest.PORT);

        NebulaPoolConfig nebulaPoolConfig = customizedUrlDriver.getNebulaPoolConfig();
        assertEquals(0, nebulaPoolConfig.getMinConnSize());
        assertEquals(10, nebulaPoolConfig.getMaxConnSize());
        assertEquals(0, nebulaPoolConfig.getTimeout());
        assertEquals(0, nebulaPoolConfig.getIdleTime());
        assertEquals(-1, nebulaPoolConfig.getIntervalIdle());
        assertEquals(0, nebulaPoolConfig.getWaitTime());

        DriverManager.deregisterDriver(customizedUrlDriver);
        customizedUrlDriver.closePool();
    }



}
