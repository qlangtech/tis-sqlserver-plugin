package com.qlangtech.tis.plugins.incr.flink.cdc.sqlserver;

import com.qlangtech.plugins.incr.flink.cdc.CDCTestSuitParams;
import com.qlangtech.plugins.incr.flink.cdc.CDCTestSuitParams.Builder;
import org.junit.Test;
import org.testcontainers.containers.JdbcDatabaseContainer;

/**
 * @create: 2024-11-10 22:16
 **/
public class TestFlinkCDCSqlServerSourceFactoryE2E extends SqlServerSourceTestBase {

    private TISMSSQLServerContainer container = null;

    protected FlinkCDCSqlServerSourceFactory createCDCFactory() {
        return new FlinkCDCSqlServerSourceFactory();
    }

    @Override
    protected JdbcDatabaseContainer getSqlServerContainer() {
        if (container == null) {
            container = TISMSSQLServerContainer.createContainer();
        }
        return container;
    }

    @Override
    protected Builder suitParamBuilder(String tableName) {
        return CDCTestSuitParams.createBuilder();
    }

    /**
     * 使用base表（base_01,base_02）的分表策略测试
     *
     * @throws Exception
     */
    @Test()
    public void testBaseTableWithSplit() throws Exception {

    }

    /**
     * 在数据中binlog中增加Transformer算子
     *
     * @throws Exception
     */
    @Test()
    public void testBinlogConsumeWithRowTransformer() throws Exception {

    }

    /**
     * 使用各种类型的数据进行 兼容测试
     *
     * @throws Exception
     */
    @Test()
    public void testFullTypesConsume() throws Exception {

    }
}
