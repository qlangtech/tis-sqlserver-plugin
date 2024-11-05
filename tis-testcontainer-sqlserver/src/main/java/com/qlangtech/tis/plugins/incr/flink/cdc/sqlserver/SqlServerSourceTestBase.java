package com.qlangtech.tis.plugins.incr.flink.cdc.sqlserver;

import com.google.common.collect.ImmutableMap;
import com.qlangtech.plugins.incr.flink.cdc.CDCTestSuitParams;
import com.qlangtech.plugins.incr.flink.junit.TISApplySkipFlinkClassloaderFactoryCreation;
import com.qlangtech.tis.TIS;
import com.qlangtech.tis.coredefine.module.action.TargetResName;
import com.qlangtech.tis.plugin.ds.BasicDataSourceFactory;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.test.util.AbstractTestBase;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.TestRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.lifecycle.Startables;

import java.util.Map;
import java.util.stream.Stream;

import static org.apache.flink.cdc.connectors.mysql.testutils.MySqlContainer.TOKEN_MySQLV8DataSourceFactory;

/**
 * // @see MySqlSourceTestBase
 * @create: 2024-10-23 15:33
 **/
public abstract class SqlServerSourceTestBase extends AbstractTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(SqlServerSourceTestBase.class);

    @ClassRule(order = 100)
    public static TestRule name = new TISApplySkipFlinkClassloaderFactoryCreation();

    protected abstract JdbcDatabaseContainer getSqlServerContainer();

    private BasicDataSourceFactory dsFactory;

    /**
     * @see  org.testcontainers.containers.MSSQLServerContainer
     * @param dataxName
     * @return
     */
    public BasicDataSourceFactory createDataSource(TargetResName dataxName) {
        if (this.dsFactory != null) {
            return this.dsFactory;
        }
        JdbcDatabaseContainer container = this.getSqlServerContainer();
        Startables.deepStart(Stream.of(container)).join();

        if (container instanceof TISMSSQLServerContainer) {
            return this.dsFactory = (BasicDataSourceFactory) ((TISMSSQLServerContainer) container).getDataSourceFactory(dataxName);
        } else {
            this.dsFactory = (BasicDataSourceFactory)
                    TISMSSQLServerContainer.getBasicDataSourceFactory(dataxName, TIS.get().getDescriptor(TOKEN_MySQLV8DataSourceFactory), container, false);
            this.dsFactory.initializeDB(StringUtils.substring(TISMSSQLServerContainer.INITIAL_DB_SQL, 1));
            return dsFactory;
        }
    }

    public static String tabStu = "stu";
    public static String tabBase = "base";
    public static String fullTypes = "full_types";
    public static final String tabInstanceDetail = "instancedetail";

    public Map<String, CDCTestSuitParams> tabParamMap;

    @Before
    public void initializeTabParamMap() {

        ImmutableMap.Builder<String, CDCTestSuitParams> builder = ImmutableMap.builder();
        builder.put(tabStu, suitParamBuilder(tabStu)
                .setTabName(tabStu).build());

        builder.put(tabBase, suitParamBuilder(tabBase)
                //.setIncrColumn("update_time")
                .setTabName(tabBase) //
                .build());

        builder.put(tabInstanceDetail, suitParamBuilder(tabInstanceDetail)
                //.setIncrColumn("modify_time")
                .setTabName(tabInstanceDetail).build());
        builder.put(fullTypes
                , suitParamBuilder(fullTypes)
                        .setTabName(fullTypes).build());

        tabParamMap = builder.build();

    }

    protected abstract CDCTestSuitParams.Builder suitParamBuilder(String tableName);

    @BeforeClass
    public static void startContainers() {

    }
}
