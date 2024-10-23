package com.qlangtech.tis.plugins.incr.flink.cdc.sqlserver;

import com.qlangtech.tis.coredefine.module.action.TargetResName;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.plugin.ds.BasicDataSourceFactory;
import com.qlangtech.tis.plugin.ds.DataSourceFactory;
import com.qlangtech.tis.plugin.ds.SplitTableStrategy;
import com.qlangtech.tis.realtime.utils.NetUtils;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.MSSQLServerContainer;
import org.testcontainers.lifecycle.Startables;

import java.util.stream.Stream;

/**
 * @create: 2024-10-23 15:57
 **/
public class TISMSSQLServerContainer extends MSSQLServerContainer {

    public static final String INITIAL_DB_SQL = "/docker/setup.sql";
    private static Logger LOG = LoggerFactory.getLogger(TISMSSQLServerContainer.class);

    public static DataSourceFactory getBasicDataSourceFactory(TargetResName dataxName, Descriptor dataSourceFactoryDesc, JdbcDatabaseContainer container, boolean splitTabStrategy) {
        LOG.info("Starting containers...");
        Startables.deepStart(Stream.of(container)).join();
        LOG.info("Containers are started.");

        Descriptor mySqlV5DataSourceFactory = dataSourceFactoryDesc;// TIS.get().getDescriptor(imageTag.equals(DEFAULT_TAG) ? "MySQLV5DataSourceFactory" : "MySQLV8DataSourceFactory");
        Assert.assertNotNull("desc of mySqlV5DataSourceFactory can not be null", mySqlV5DataSourceFactory);

        Descriptor.FormData formData = new Descriptor.FormData();
        formData.addProp("name", "mysql");
        formData.addProp("dbName", container.getDatabaseName());
        // formData.addProp("nodeDesc", mySqlContainer.getHost());

        formData.addProp("nodeDesc", NetUtils.getHost());

        if (splitTabStrategy) {
            Descriptor.FormData splitStrategyForm = new Descriptor.FormData();
            splitStrategyForm.addProp("tabPattern", SplitTableStrategy.PATTERN_PHYSICS_TABLE.pattern());
            formData.addSubForm("splitTableStrategy"
                    , "com.qlangtech.tis.plugin.ds.split.DefaultSplitTableStrategy", splitStrategyForm);
        } else {
            formData.addSubForm("splitTableStrategy"
                    , "com.qlangtech.tis.plugin.ds.split.NoneSplitTableStrategy", new Descriptor.FormData());
        }


        formData.addProp("password", container.getPassword());
        formData.addProp("userName", container.getUsername());
        formData.addProp("port", String.valueOf(container.getMappedPort(MS_SQL_SERVER_PORT)));
        formData.addProp("encode", "utf8");
        formData.addProp("useCompression", "true");

        Descriptor.ParseDescribable<DataSourceFactory> parseDescribable
                = mySqlV5DataSourceFactory.newInstance(dataxName.getName(), formData);
        Assert.assertNotNull(parseDescribable.getInstance());

        return parseDescribable.getInstance();
    }

    public BasicDataSourceFactory createMySqlDataSourceFactory(TargetResName dataxName) {
        throw new UnsupportedOperationException();
    }
}
