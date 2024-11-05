package com.qlangtech.tis.plugins.incr.flink.connector.sink;

import com.dtstack.chunjun.conf.SyncConf;
import com.dtstack.chunjun.connector.jdbc.conf.JdbcConf;
import com.dtstack.chunjun.connector.jdbc.sink.JdbcSinkFactory;
import com.dtstack.chunjun.connector.sqlserver.dialect.SqlserverDialect;
import com.dtstack.chunjun.converter.RawTypeConverter;
import com.qlangtech.tis.plugins.incr.flink.cdc.AbstractRowDataMapper;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;

import java.util.List;
import java.util.Optional;

/**
 * @create: 2024-10-29 13:34
 **/
public class TISSqlServerDialect extends SqlserverDialect {
    private final JdbcConf jdbcConf;

    public TISSqlServerDialect(SyncConf conf) {
        super(conf);
        this.jdbcConf = JdbcSinkFactory.getJdbcConf(conf);
    }

    @Override
    public Optional<String> getReplaceStatement(String schema, String tableName, List<String> fieldNames) {
        if (CollectionUtils.isEmpty(jdbcConf.getUniqueKey())) {
            throw new IllegalArgumentException("jdbcConf.getUniqueKey() can not be empty");
        }
        return getUpsertStatement(schema, tableName, fieldNames, jdbcConf.getUniqueKey(), false);
    }

    @Override
    public String buildTableInfoWithSchema(String schema, String tableName) {
        // SqlServer是不能带DBName的
        return quoteIdentifier(tableName);
    }

    @Override
    public RawTypeConverter getRawTypeConverter() {
        return (colMeta) -> AbstractRowDataMapper.mapFlinkCol(colMeta, -1).type;
    }

}
