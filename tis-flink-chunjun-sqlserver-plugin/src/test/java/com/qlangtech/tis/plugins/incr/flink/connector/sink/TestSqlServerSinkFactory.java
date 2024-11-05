package com.qlangtech.tis.plugins.incr.flink.connector.sink;

import com.google.common.collect.Lists;
import com.qlangtech.plugins.incr.flink.chunjun.doris.sink.TestFlinkSinkExecutor;
import com.qlangtech.tis.coredefine.module.action.TargetResName;
import com.qlangtech.tis.datax.TableAlias;
import com.qlangtech.tis.plugin.datax.DataXSqlserverWriter;
import com.qlangtech.tis.plugin.datax.common.BasicDataXRdbmsWriter;
import com.qlangtech.tis.plugin.ds.BasicDataSourceFactory;
import com.qlangtech.tis.plugin.ds.sqlserver.SqlServerDatasourceFactory;
import com.qlangtech.tis.plugins.incr.flink.cdc.sqlserver.TISMSSQLServerContainer;
import com.qlangtech.tis.plugins.incr.flink.connector.ChunjunSinkFactory;
import com.qlangtech.tis.plugins.incr.flink.connector.UpdateMode;
import com.qlangtech.tis.plugins.incr.flink.connector.impl.UpsertType;
import com.qlangtech.tis.realtime.TabSinkFunc;
import com.qlangtech.tis.realtime.transfer.DTO;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.flink.table.data.RowData;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
        //  return new UpdateType();
        //  InsertType insertType = new InsertType();
        UpsertType upsert = new UpsertType();
        return upsert;
        // UpdateType updateMode = new UpdateType();
        //  updateMode.updateKey = Lists.newArrayList(colId, updateTime);
        //  return insertType;
    }

//    @Override
//    protected ArrayList<String> getUniqueKey() {
//        return Lists.newArrayList(colId, updateTime);
//    }

    @Override
    protected BasicDataSourceFactory getDsFactory() {
        return sqlServerDS;
    }

    @Override
    protected void startTestSinkSync(Map<TableAlias, TabSinkFunc<RowData>> sinkFunction) {

//        sqlServerDS.visitFirstConnection((conn -> {
//            try (Statement statement = conn.createStatement()) {
//                try (ResultSet resultSet = statement.executeQuery("select count(1) from \"totalpayinfo\"")) {
//                    if (resultSet.next()) {
//                        Assert.assertEquals(0, resultSet.getInt(1));
//                    } else {
//                        throw new IllegalStateException("must contain val");
//                    }
//                }
//            }
//
//        }));

    }

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
//        Assert.assertNotNull(sqlServerDS);
//        sqlServerDS.visitAllConnection((conn) -> {
//            try (Statement statement = conn.getConnection().createStatement()) {
//
//                try (ResultSet resultSet = statement.executeQuery("select count(1) from customers")) {
//                    Assert.assertTrue(resultSet.next());
//                    int rowCount = resultSet.getInt(1);
//                    Assert.assertTrue("rowCount must big than 0", rowCount > 0);
//                }
//
//            }
//        });
    }

//    @Override
//    protected DTO[] createTestDTO(boolean needDelete) {
//        return super.createTestDTO(false);
        // create id is 88888888887
//        List<DTO> dto = Lists.newArrayList(super.createTestDTO(false));
//
//        // 创建一条添加记录
//        DTO add = createDTO(DTO.EventType.ADD, (after) -> {
//            after.put(colId, "88888888889");
//        });
//
//        dto.add(add);
//
//        final String deleteFinallyId = "88888888890";
//        // 创建三条event，最终会被删除
//        DTO[] deleteFinally = super.createTestDTO(false);
//        Map<String, Object> after;
//        Map<String, Object> before;
//        for (DTO d : deleteFinally) {
//            after = d.getAfter();
//            before = d.getBefore();
//            if (MapUtils.isNotEmpty(after)) {
//                after.put(colId, deleteFinallyId);
//            }
//            if (MapUtils.isNotEmpty(before)) {
//                before.put(colId, deleteFinallyId);
//            }
//            // 三条： 1.添加 2.更新 3.删除
//            dto.add(d);
//        }
//
//        Assert.assertEquals(7, dto.size());
//        return dto.toArray(new DTO[dto.size()]);
  //  }

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
