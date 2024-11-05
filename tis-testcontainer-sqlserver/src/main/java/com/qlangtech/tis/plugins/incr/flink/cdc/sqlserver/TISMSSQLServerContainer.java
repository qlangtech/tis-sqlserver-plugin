package com.qlangtech.tis.plugins.incr.flink.cdc.sqlserver;

import com.qlangtech.plugins.incr.flink.slf4j.TISLoggerConsumer;
import com.qlangtech.tis.TIS;
import com.qlangtech.tis.coredefine.module.action.TargetResName;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.plugin.ds.DataSourceFactory;
import com.qlangtech.tis.plugin.ds.SplitTableStrategy;
import com.qlangtech.tis.realtime.utils.NetUtils;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.MSSQLServerContainer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.shaded.org.awaitility.Awaitility;
import org.testcontainers.shaded.org.awaitility.core.ConditionTimeoutException;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @create: 2024-10-23 15:57
 **/
public class TISMSSQLServerContainer extends MSSQLServerContainer {

    public static final String INITIAL_DB_SQL = "/docker/setup.sql";
    private static Logger LOG = LoggerFactory.getLogger(TISMSSQLServerContainer.class);
    private static final String DISABLE_DB_CDC =
            "IF EXISTS(select 1 from sys.databases where name='#' AND is_cdc_enabled=1)\n"
                    + "EXEC sys.sp_cdc_disable_db";
    private static final String INITIALIZE_DB_NAME = "customer";
    private static final Pattern COMMENT_PATTERN = Pattern.compile("^(.*)--.*$");
    private static final String STATEMENTS_PLACEHOLDER = "#";
    private DataSourceFactory dataSourceFactory;

    public TISMSSQLServerContainer() {
        super("mcr.microsoft.com/mssql/server:2019-latest");
    }


    @Override
    public Connection createConnection(String queryString) throws SQLException, NoDriverFoundException {
        Connection conn = super.createConnection(queryString);
        initializeSqlServerTable(conn, INITIALIZE_DB_NAME);
        return conn;
    }

    public static final TISMSSQLServerContainer createContainer() {
        // https://github.com/apache/flink-cdc/blob/master/flink-cdc-connect/flink-cdc-source-connectors/flink-connector-sqlserver-cdc/src/test/java/org/apache/flink/cdc/connectors/sqlserver/SqlServerTestBase.java
        TISMSSQLServerContainer container =
                (TISMSSQLServerContainer) new TISMSSQLServerContainer()
                        .withPassword("Password!")
                        .withEnv("MSSQL_AGENT_ENABLED", "true")
                        .withEnv("MSSQL_PID", "Standard")
                        .withLogConsumer(new TISLoggerConsumer(LOG));
        return container;
    }

    /**
     * Executes a JDBC statement using the default jdbc config without autocommitting the
     * connection.
     */
    protected void initializeSqlServerTable(Connection conn, String sqlFile) {
        final String ddlFile = String.format("ddl/%s.sql", sqlFile);
        final URL ddlTestFile = TISMSSQLServerContainer.class.getClassLoader().getResource(ddlFile);
        Assert.assertNotNull("Cannot locate " + ddlFile, ddlTestFile);
        Connection connection = conn;
        try (Statement statement = connection.createStatement()) {
            dropTestDatabase(connection, sqlFile);
            final List<String> statements =
                    Arrays.stream(
                                    Files.readAllLines(Paths.get(ddlTestFile.toURI())).stream()
                                            .map(String::trim)
                                            .filter(x -> !x.startsWith("--") && !x.isEmpty())
                                            .map(
                                                    x -> {
                                                        final Matcher m =
                                                                COMMENT_PATTERN.matcher(x);
                                                        return m.matches() ? m.group(1) : x;
                                                    })
                                            .collect(Collectors.joining("\n"))
                                            .split(";"))
                            .collect(Collectors.toList());
            for (String stmt : statements) {
                statement.execute(stmt);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static void dropTestDatabase(Connection connection, String databaseName)
            throws SQLException {
        try {
            Awaitility.await("Disabling CDC")
                    .atMost(60, TimeUnit.SECONDS)
                    .until(
                            () -> {
                                try {
                                    connection
                                            .createStatement()
                                            .execute(String.format("USE [%s]", databaseName));
                                } catch (SQLException e) {
                                    // if the database doesn't yet exist, there is no need to
                                    // disable CDC
                                    return true;
                                }
                                try {
                                    disableDbCdc(connection, databaseName);
                                    return true;
                                } catch (SQLException e) {
                                    return false;
                                }
                            });
        } catch (ConditionTimeoutException e) {
            throw new IllegalArgumentException(
                    String.format("Failed to disable CDC on %s", databaseName), e);
        }

        connection.createStatement().execute("USE master");

        try {
            Awaitility.await(String.format("Dropping database %s", databaseName))
                    .atMost(60, TimeUnit.SECONDS)
                    .until(
                            () -> {
                                try {
                                    String sql =
                                            String.format(
                                                    "IF EXISTS(select 1 from sys.databases where name = '%s') DROP DATABASE [%s]",
                                                    databaseName, databaseName);
                                    connection.createStatement().execute(sql);
                                    return true;
                                } catch (SQLException e) {
                                    LOG.warn(
                                            String.format(
                                                    "DROP DATABASE %s failed (will be retried): {}",
                                                    databaseName),
                                            e.getMessage());
                                    try {
                                        connection
                                                .createStatement()
                                                .execute(
                                                        String.format(
                                                                "ALTER DATABASE [%s] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;",
                                                                databaseName));
                                    } catch (SQLException e2) {
                                        LOG.error("Failed to rollbackimmediately", e2);
                                    }
                                    return false;
                                }
                            });
        } catch (ConditionTimeoutException e) {
            throw new IllegalStateException("Failed to drop test database", e);
        }
    }

    public DataSourceFactory getDataSourceFactory(TargetResName resName) {
        if (dataSourceFactory == null) {
            dataSourceFactory = getBasicDataSourceFactory(resName
                    , TIS.get().getDescriptor("SqlServer2019DatasourceFactory")
                    , this, false);
        }
        return dataSourceFactory;
    }

    /**
     * Disables CDC for a given database, if not already disabled.
     *
     * @param name the name of the DB, may not be {@code null}
     * @throws SQLException if anything unexpected fails
     */
    protected static void disableDbCdc(Connection connection, String name) throws SQLException {
        Objects.requireNonNull(name);
        connection.createStatement().execute(DISABLE_DB_CDC.replace(STATEMENTS_PLACEHOLDER, name));
    }

    public static DataSourceFactory getBasicDataSourceFactory(TargetResName dataxName
            , Descriptor dataSourceFactoryDesc, JdbcDatabaseContainer container, boolean splitTabStrategy) {
        LOG.info("Starting containers...");
        Startables.deepStart(Stream.of(container)).join();
        LOG.info("Containers are started.");

        Descriptor sqlServerDSFactory = dataSourceFactoryDesc;
        Assert.assertNotNull("desc of sqlServerDSFactory can not be null", sqlServerDSFactory);

        Descriptor.FormData formData = new Descriptor.FormData();
        formData.addProp("name", "sqlserver");
        formData.addProp("dbName", INITIALIZE_DB_NAME// container.getDatabaseName()
        );
        // formData.addProp("nodeDesc", mySqlContainer.getHost());

        formData.addProp("nodeDesc", container.getHost());

//        if (splitTabStrategy) {
//            Descriptor.FormData splitStrategyForm = new Descriptor.FormData();
//            splitStrategyForm.addProp("tabPattern", SplitTableStrategy.PATTERN_PHYSICS_TABLE.pattern());
//            formData.addSubForm("splitTableStrategy"
//                    , "com.qlangtech.tis.plugin.ds.split.DefaultSplitTableStrategy", splitStrategyForm);
//        } else {
//            formData.addSubForm("splitTableStrategy"
//                    , "com.qlangtech.tis.plugin.ds.split.NoneSplitTableStrategy", new Descriptor.FormData());
//        }


        formData.addProp("password", container.getPassword());
        formData.addProp("userName", container.getUsername());
        formData.addProp("port", String.valueOf(container.getMappedPort(MS_SQL_SERVER_PORT)));
        // formData.addProp("encode", "utf8");
        formData.addProp("useSSL", "false");

        Descriptor.ParseDescribable<DataSourceFactory> parseDescribable
                = sqlServerDSFactory.newInstance(dataxName.getName(), formData);
        Assert.assertNotNull(parseDescribable.getInstance());

        return parseDescribable.getInstance();
    }
}
