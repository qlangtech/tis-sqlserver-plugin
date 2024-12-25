package com.qlangtech.tis.plugins.incr.flink.cdc.sqlserver;

import com.qlangtech.tis.TIS;
import com.qlangtech.tis.offline.DataxUtils;
import com.qlangtech.tis.plugin.ds.DataSourceFactory;
import com.qlangtech.tis.plugin.ds.JDBCConnection;
import com.qlangtech.tis.plugin.ds.PostedDSProp;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-11-30 09:44
 * @see io.debezium.connector.sqlserver.SqlServerConnection
 * @see io.debezium.jdbc.JdbcConnection.ConnectionFactoryDecorator#connect
 **/
public class SQLServerConnectionFactory implements JdbcConnection.ConnectionFactory {
    @Override
    public Connection connect(JdbcConfiguration jdbcConfiguration) throws SQLException {
        DataSourceFactory dsFactory = TIS.getDataBasePlugin(PostedDSProp.parse(jdbcConfiguration.getString(DataxUtils.DATASOURCE_FACTORY_IDENTITY)));
        String jdbcUrl = jdbcConfiguration.getString(DataxUtils.DATASOURCE_JDBC_URL);
        if (StringUtils.isEmpty(jdbcUrl)) {
            throw new IllegalArgumentException("param jdbcUrl can not be null, relevant key:" + DataxUtils.DATASOURCE_JDBC_URL);
        }
        JDBCConnection connection = dsFactory.getConnection(jdbcUrl, false, false);
        return connection.getConnection();
    }
}
