package com.qlangtech.tis.plugins.incr.flink.cdc.sqlserver;

import com.google.common.collect.Maps;
import com.qlangtech.plugins.incr.flink.cdc.BiFunction;
import com.qlangtech.plugins.incr.flink.cdc.FlinkCol;
import com.qlangtech.plugins.incr.flink.cdc.ISourceValConvert;
import com.qlangtech.plugins.incr.flink.cdc.SourceChannel;
import com.qlangtech.plugins.incr.flink.cdc.SourceChannel.ReaderSourceCreator;
import com.qlangtech.plugins.incr.flink.cdc.TISDeserializationSchema;
import com.qlangtech.plugins.incr.flink.cdc.valconvert.DateTimeConverter;
import com.qlangtech.tis.async.message.client.consumer.IConsumerHandle;
import com.qlangtech.tis.async.message.client.consumer.IFlinkColCreator;
import com.qlangtech.tis.async.message.client.consumer.IMQListener;
import com.qlangtech.tis.async.message.client.consumer.MQConsumeException;
import com.qlangtech.tis.coredefine.module.action.TargetResName;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.IDataxReader;
import com.qlangtech.tis.offline.DataxUtils;
import com.qlangtech.tis.plugin.datax.common.BasicDataXRdbmsReader;
import com.qlangtech.tis.plugin.datax.transformer.RecordTransformerRules;
import com.qlangtech.tis.plugin.ds.BasicDataSourceFactory;
import com.qlangtech.tis.plugin.ds.DBConfig.HostDB;
import com.qlangtech.tis.plugin.ds.DBConfig.HostDBs;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import com.qlangtech.tis.plugin.ds.RunningContext;
import com.qlangtech.tis.plugin.ds.TableInDB;
import com.qlangtech.tis.plugins.incr.flink.FlinkColMapper;
import com.qlangtech.tis.plugins.incr.flink.cdc.AbstractRowDataMapper;
import com.qlangtech.tis.realtime.ReaderSource;
import com.qlangtech.tis.realtime.dto.DTOStream;
import com.qlangtech.tis.realtime.transfer.DTO;
import com.qlangtech.tis.util.IPluginContext;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.jdbc.JdbcConfiguration;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.cdc.connectors.sqlserver.source.SqlServerSourceBuilder;
import org.apache.flink.cdc.connectors.sqlserver.source.SqlServerSourceBuilder.SqlServerIncrementalSource;
import org.apache.kafka.connect.data.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.function.Function;

import static io.debezium.config.CommonConnectorConfig.DATABASE_CONFIG_PREFIX;

/**
 * 具体实现可参考： https://gitee.com/qlangtech/plugins/blob/master/tis-incr/tis-flink-cdc-mysql-plugin/src/main/java/com/qlangtech/tis/plugins/incr/flink/cdc/mysql/FlinkCDCMysqlSourceFunction.java
 *
 * @create: 2024-10-23 14:26
 **/
public class FlinkCDCSqlServerSourceFunction implements IMQListener<JobExecutionResult> {
    private final FlinkCDCSqlServerSourceFactory sourceFactory;

    public FlinkCDCSqlServerSourceFunction(FlinkCDCSqlServerSourceFactory sourceFactory) {
        this.sourceFactory = sourceFactory;
    }

    @Override
    public IConsumerHandle getConsumerHandle() {
        return sourceFactory.getConsumerHander();
    }

    @Override
    public JobExecutionResult start(TargetResName dataxName
            , IDataxReader dataSource, List<ISelectedTab> tabs, IDataxProcessor dataXProcessor) throws MQConsumeException {
        // 具体实现可参考

        try {
            Objects.requireNonNull(dataXProcessor, "param dataXProcessor can not be null");
            BasicDataXRdbmsReader rdbmsReader = (BasicDataXRdbmsReader) dataSource;
            BasicDataSourceFactory dsFactory = (BasicDataSourceFactory) rdbmsReader.getDataSourceFactory();
            Map<String, FlinkColMapper> tabColsMapper = Maps.newHashMap();
            TableInDB tablesInDB = dsFactory.getTablesInDB();
            IFlinkColCreator<FlinkCol> flinkColCreator = sourceFactory.createFlinkColCreator();
            IPluginContext pluginContext = IPluginContext.namedContext(dataxName.getName());
            for (ISelectedTab tab : tabs) {
                FlinkColMapper colsMapper
                        = AbstractRowDataMapper.getAllTabColsMetaMapper(tab.getCols(), flinkColCreator);
                tabColsMapper.put(tab.getName(), colsMapper);
            }

            Map<String, Map<String, Function<RunningContext, Object>>> contextParamValsGetterMapper
                    = RecordTransformerRules.contextParamValsGetterMapper(pluginContext, rdbmsReader, tabs);
            //
            TISDeserializationSchema deserializationSchema
                    = new TISDeserializationSchema(
                    new SQLServerSourceDTOColValProcess(tabColsMapper)
                    , tablesInDB.getPhysicsTabName2LogicNameConvertor()
                    , contextParamValsGetterMapper);


            SourceChannel sourceChannel = new SourceChannel(
                    SourceChannel.getSourceFunction(
                            dsFactory,
                            tabs
                            , new SQLServerReaderSourceCreator(dsFactory, this.sourceFactory, deserializationSchema)
                    ));
            sourceChannel.setFocusTabs(tabs, dataXProcessor.getTabAlias(null)
                    , (tabName) -> DTOStream.createDispatched(tabName, sourceFactory.independentBinLogMonitor));
            return (JobExecutionResult) getConsumerHandle().consume(dataxName, sourceChannel, dataXProcessor);
        } catch (Exception e) {
            throw new MQConsumeException(e.getMessage(), e);
        }
    }

    public static class SQLServerSourceDTOColValProcess implements ISourceValConvert, Serializable {
        final Map<String, FlinkColMapper> tabColsMapper;


        public SQLServerSourceDTOColValProcess(Map<String, FlinkColMapper> tabColsMapper) {
            this.tabColsMapper = tabColsMapper;
        }

        @Override
        public Object convert(DTO dto, Field field, Object val) {
            FlinkColMapper colMapper = tabColsMapper.get(dto.getTableName());
            if (colMapper == null) {
                throw new IllegalStateException("tableName:" + dto.getTableName()
                        + " relevant colMapper can not be null, exist cols:"
                        + String.join(",", tabColsMapper.keySet()));
            }
            BiFunction process = colMapper.getSourceDTOColValProcess(field.name());
            if (process == null) {
                // 说明用户在选在表的列时候，没有选择该列，所以就不用处理了
                return null;
            }
            return process.apply(val);
        }
    }

    public static class SQLServerReaderSourceCreator implements ReaderSourceCreator {
        private final BasicDataSourceFactory dsFactory;
        private final FlinkCDCSqlServerSourceFactory sourceFactory;
        private final TISDeserializationSchema deserializationSchema;
        private static final Logger logger = LoggerFactory.getLogger(SQLServerReaderSourceCreator.class);

//        public SQLServerReaderSourceCreator(BasicDataSourceFactory dsFactory, FlinkCDCSqlServerSourceFactory sourceFactory) {
//            this(dsFactory, sourceFactory, new TISDeserializationSchema());
//        }

        public SQLServerReaderSourceCreator(BasicDataSourceFactory dsFactory, FlinkCDCSqlServerSourceFactory sourceFactory, TISDeserializationSchema deserializationSchema) {
            this.dsFactory = dsFactory;
            this.sourceFactory = sourceFactory;
            this.deserializationSchema = deserializationSchema;
        }

        @Override
        public List<ReaderSource> create(String dbHost, HostDBs dbs, Set<String> tbs, Properties debeziumProperties) {

            // DateTimeConverter.setDatetimeConverters(SQLServerDateTimeConverter.class.getName(), debeziumProperties);

            debeziumProperties.setProperty(
                    CommonConnectorConfig.EVENT_PROCESSING_FAILURE_HANDLING_MODE.name()
                    , CommonConnectorConfig.EventProcessingFailureHandlingMode.WARN.getValue());

            debeziumProperties.setProperty(
                    DATABASE_CONFIG_PREFIX + JdbcConfiguration.CONNECTION_FACTORY_CLASS.name()
                    , SQLServerConnectionFactory.class.getCanonicalName());

            if (CollectionUtils.isEmpty(dbs.dbs)) {
                throw new IllegalStateException("dbs.dbs can not be empty");
            }
            for (HostDB hostDB : dbs.dbs) {
                debeziumProperties.setProperty(DATABASE_CONFIG_PREFIX + DataxUtils.DATASOURCE_FACTORY_IDENTITY, this.dsFactory.identityValue());
                debeziumProperties.setProperty(DATABASE_CONFIG_PREFIX + DataxUtils.DATASOURCE_JDBC_URL, hostDB.getJdbcUrl());
                break;
            }


//            debeziumProperties.setProperty(
//                    SqlServerConnectorConfig.INCONSISTENT_SCHEMA_HANDLING_MODE.name()
//                    , CommonConnectorConfig.EventProcessingFailureHandlingMode.WARN.getValue());

            String[] databases = dbs.getDataBases();
            if (StringUtils.isEmpty(sourceFactory.timeZone)) {
                throw new IllegalStateException("timezone can not be null");
            }

            logger.info("monitor db:{} databaseList:{},tableList:{}", dbHost, databases, tbs);
            SqlServerIncrementalSource<DTO> sourceFunc =
                    new SqlServerSourceBuilder()
                            .hostname(dbHost)
                            .serverTimeZone(sourceFactory.timeZone)
                            .port(dsFactory.port)
                            .debeziumProperties(debeziumProperties)
                            .databaseList(databases)
                            .includeSchemaChanges(false)
                            .tableList(tbs.toArray(new String[tbs.size()]))
                            .username(dsFactory.getUserName())
                            .password(dsFactory.getPassword())
                            .deserializer(deserializationSchema)
                            .startupOptions(sourceFactory.getStartupOptions())
                            .build();

//                    MySqlSource.<DTO>builder()
//                    .hostname(dbHost)
//                    .serverTimeZone(sourceFactory.timeZone)
//                    .port(dsFactory.port)
//                    .databaseList(databases) // monitor all tables under inventory database
//                    .tableList(tbs.toArray(new String[tbs.size()]))
//                    // .serverTimeZone(BasicDataSourceFactory.DEFAULT_SERVER_TIME_ZONE.getId())
//                    .username(dsFactory.getUserName())
//                    .password(dsFactory.getPassword())
//                    .startupOptions(sourceFactory.getStartupOptions())
//                    .debeziumProperties(debeziumProperties)
//                    .deserializer(deserializationSchema) // converts SourceRecord to JSON String
//                    .build();

            return createReaderSources(dbHost, dbs, sourceFunc);
        }

        protected List<ReaderSource> createReaderSources(String dbHost, HostDBs dbs, SqlServerIncrementalSource<DTO> sourceFunc) {
            return Collections.singletonList(ReaderSource.createDTOSource(
                            dbHost + ":" + dsFactory.port + ":" + dbs.joinDataBases("_")
                            , sourceFunc
                    )
            );
        }
    }

}
