package com.vesoft.nebula.jdbc.impl;

import com.vesoft.nebula.client.graph.data.ResultSet;
import com.vesoft.nebula.client.graph.exception.IOErrorException;
import com.vesoft.nebula.client.graph.net.Session;
import com.vesoft.nebula.jdbc.NebulaAbstractConnection;
import com.vesoft.nebula.jdbc.NebulaAbstractResultSet;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class NebulaConnection extends NebulaAbstractConnection {

    private NebulaDriver nebulaDriver;
    private Session nebulaSession;
    private String graphSpace = "unknown";
    private boolean isClosed = false;

    protected NebulaConnection(NebulaDriver nebulaDriver, String graphSpace) throws SQLException {
        super(NebulaAbstractResultSet.CLOSE_CURSORS_AT_COMMIT);
        this.nebulaDriver = nebulaDriver;
        this.nebulaSession = this.nebulaDriver.getSessionFromNebulaPool();
        this.connectionConfig = this.nebulaDriver.getConnectionConfig();

        try {
            ResultSet result = nebulaSession.execute("use " + graphSpace);
            if (result.isSucceeded()){
                this.graphSpace = graphSpace;
                log.info(String.format("Access graph space [%s] succeeded", graphSpace));
            }else{
                throw new SQLException(String.format("Access graph space [%s] failed. Error code: %d, Error message: %s",
                        graphSpace, result.getErrorCode(), result.getErrorMessage()));
            }
        } catch (IOErrorException e) {
            e.printStackTrace();
        }

    }
}
