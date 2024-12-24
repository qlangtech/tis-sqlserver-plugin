package com.qlangtech.tis.plugins.incr.flink.cdc.sqlserver;

import com.qlangtech.plugins.incr.flink.cdc.CDCTestSuitParams;
import com.qlangtech.plugins.incr.flink.cdc.CDCTestSuitParams.Builder;
import com.qlangtech.plugins.incr.flink.cdc.CUDCDCTestSuit;
import com.qlangtech.plugins.incr.flink.cdc.IResultRows;
import com.qlangtech.plugins.incr.flink.cdc.source.TestTableRegisterFlinkSourceHandle;
import com.qlangtech.plugins.incr.flink.launch.TISFlinkCDCStreamFactory;
import com.qlangtech.tis.coredefine.module.action.TargetResName;
import com.qlangtech.tis.plugin.datax.common.BasicDataXRdbmsReader;
import com.qlangtech.tis.plugin.ds.BasicDataSourceFactory;
import com.qlangtech.tis.plugin.incr.TISSinkFactory;
import com.qlangtech.tis.plugins.incr.flink.cdc.sqlserver.startup.LatestOffset;
import org.junit.Test;
import org.testcontainers.containers.JdbcDatabaseContainer;

/**
 * @create: 2024-11-10 22:16
 **/
public class TestFlinkCDCSqlServerSourceFactoryE2E extends SqlServerSourceTestBase {

    private TISMSSQLServerContainer container = null;

    protected FlinkCDCSqlServerSourceFactory createCDCFactory() {
        FlinkCDCSqlServerSourceFactory sqlServerSourceFactory = new FlinkCDCSqlServerSourceFactory();
        sqlServerSourceFactory.timeZone = FlinkCDCSqlServerSourceFactory.dftZoneId();
        sqlServerSourceFactory.startupOptions = new LatestOffset();
        return sqlServerSourceFactory;
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
    public void testBaseTable() throws Exception {
        FlinkCDCSqlServerSourceFactory mysqlCDCFactory = createCDCFactory();


        // final String tabName = "base";
        TISFlinkCDCStreamFactory streamFactory = new TISFlinkCDCStreamFactory();
        streamFactory.parallelism = 1;
        CDCTestSuitParams suitParams = tabParamMap.get(tabBase);//new CDCTestSuitParams("base");
        // Optional.of("_01")
        CUDCDCTestSuit cdcTestSuit = new CUDCDCTestSuit(suitParams) {
            @Override
            protected BasicDataSourceFactory createDataSourceFactory(TargetResName dataxName, boolean useSplitTabStrategy) {
                return (BasicDataSourceFactory) ((TISMSSQLServerContainer) getSqlServerContainer()).getDataSourceFactory(dataxName);
            }

            @Override
            protected IResultRows createConsumerHandle(BasicDataXRdbmsReader dataxReader, String tabName, TISSinkFactory sinkFuncFactory) {
                TestTableRegisterFlinkSourceHandle sourceHandle = new TestTableRegisterFlinkSourceHandle(tabName, cols);
                sourceHandle.setSinkFuncFactory(sinkFuncFactory);
                sourceHandle.setSourceStreamTableMeta(dataxReader);
                sourceHandle.setStreamFactory(streamFactory);
                sourceHandle.setSourceFlinkColCreator(mysqlCDCFactory.createFlinkColCreator());
                return sourceHandle;
            }
        };

        cdcTestSuit.startTest(mysqlCDCFactory);
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
