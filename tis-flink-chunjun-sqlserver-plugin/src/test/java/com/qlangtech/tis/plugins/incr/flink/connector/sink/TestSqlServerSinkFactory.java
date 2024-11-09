package com.qlangtech.tis.plugins.incr.flink.connector.sink;

import com.qlangtech.plugins.incr.flink.chunjun.doris.sink.TestFlinkSinkExecutor;
import com.qlangtech.tis.coredefine.module.action.TargetResName;
import com.qlangtech.tis.plugin.datax.DataXSqlserverWriter;
import com.qlangtech.tis.plugin.datax.common.BasicDataXRdbmsWriter;
import com.qlangtech.tis.plugin.ds.BasicDataSourceFactory;
import com.qlangtech.tis.plugin.ds.sqlserver.SqlServerDatasourceFactory;
import com.qlangtech.tis.plugins.incr.flink.cdc.sqlserver.TISMSSQLServerContainer;
import com.qlangtech.tis.plugins.incr.flink.connector.ChunjunSinkFactory;
import com.qlangtech.tis.plugins.incr.flink.connector.UpdateMode;
import com.qlangtech.tis.plugins.incr.flink.connector.impl.UpsertType;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Objects;

/**
 * ref: com.qlangtech.plugins.incr.flink.chunjun.oracle.sink.TestChunjunOracleSinkFactory
 *
 * @create: 2024-10-29 13:39
 **/
public class TestSqlServerSinkFactory extends TestFlinkSinkExecutor {

    public static SqlServerDatasourceFactory sqlServerDS;
    private static TISMSSQLServerContainer container;
    private static final TargetResName resName = new TargetResName("test");

    @BeforeClass
    public static void initialize() {

        container = TISMSSQLServerContainer.createContainer();
        sqlServerDS = (SqlServerDatasourceFactory) container.getDataSourceFactory(resName);
    }

    @AfterClass
    public static void stop() {
        container.close();
    }

    @Override
    protected UpdateMode createIncrMode() {
        UpsertType upsert = new UpsertType();
        return upsert;
    }

    @Override
    protected BasicDataSourceFactory getDsFactory() {
        return sqlServerDS;
    }

//    @Override
//    protected void startTestSinkSync(Map<TableAlias, TabSinkFunc<RowData>> sinkFunction) {
//
//
//    }

    /**
     * 相关表
     * <pre>
     * CREATE TABLE "totalpayinfo"
     * (
     *     "entity_id"    varchar(6),
     *     "num"          int,
     *     "id"           varchar(32),
     *     "create_time"  bigint,
     *     "update_time"  datetime,
     *     "update_date"  datetime,
     *     "start_time"   datetime,
     *     "price"        decimal(10, 2)
     * ,PRIMARY KEY ( "id","update_time" ))
     *
     * </pre>
     *
     * @throws Exception
     */
    @Test
    @Override
    public void testSinkSync() throws Exception {
        super.testSinkSync();
    }

    @Override
    protected ChunjunSinkFactory getSinkFactory() {
        return new SqlServerSinkFactory();
    }

    @Override
    protected BasicDataXRdbmsWriter createDataXWriter() {
        DataXSqlserverWriter writer = new DataXSqlserverWriter() {
            @Override
            public SqlServerDatasourceFactory getDataSourceFactory() {
                return Objects.requireNonNull(sqlServerDS, "sqlServerDS can not be null");
            }
        };
        return writer;
    }
}
