package com.qlangtech.tis.plugins.incr.flink.connector.sink;

import com.dtstack.chunjun.connector.jdbc.sink.SinkColMetas;
import com.dtstack.chunjun.connector.sqlserver.sink.SqlserverOutputFormat;
import com.qlangtech.tis.plugin.ds.DataSourceFactory;
import com.qlangtech.tis.plugins.incr.flink.chunjun.common.DialectUtils;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Objects;

/**
 * @create: 2024-10-24 00:37
 **/
public class TISSqlServerOutputFormat extends SqlserverOutputFormat {
    private final DataSourceFactory dsFactory;

    public TISSqlServerOutputFormat(DataSourceFactory dsFactory, SinkColMetas cols) {
        super(cols);
        this.dsFactory = Objects.requireNonNull(dsFactory, "dsFactory can not be null");
    }

    @Override
    protected Connection getConnection() throws SQLException {
        DataSourceFactory dsFactory = Objects.requireNonNull(this.dsFactory, "dsFactory can not be null");
        return dsFactory.getConnection((this.jdbcConf.getJdbcUrl()), false).getConnection();
    }

    @Override
    protected void initializeRowConverter() {
        this.setRowConverter(DialectUtils.createColumnConverter(jdbcDialect, jdbcConf, this.colsMeta));
    }
}
