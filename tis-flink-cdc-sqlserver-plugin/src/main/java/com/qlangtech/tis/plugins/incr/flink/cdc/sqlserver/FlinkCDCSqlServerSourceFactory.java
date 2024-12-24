package com.qlangtech.tis.plugins.incr.flink.cdc.sqlserver;

import com.qlangtech.plugins.incr.flink.cdc.FlinkCol;
import com.qlangtech.plugins.incr.flink.cdc.FlinkCol.LocalDateProcess;
import com.qlangtech.plugins.incr.flink.cdc.RowFieldGetterFactory;
import com.qlangtech.tis.async.message.client.consumer.IConsumerHandle;
import com.qlangtech.tis.async.message.client.consumer.IFlinkColCreator;
import com.qlangtech.tis.async.message.client.consumer.IMQListener;
import com.qlangtech.tis.async.message.client.consumer.impl.MQListenerFactory;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ds.DataType;
import com.qlangtech.tis.plugin.ds.IColMetaGetter;
import com.qlangtech.tis.plugins.incr.flink.cdc.AbstractRowDataMapper;
import com.qlangtech.tis.plugins.incr.flink.cdc.sqlserver.startup.CDCStartupOptions;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.AtomicDataType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.TimestampType;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Objects;

/**
 * SqlServer 基于Flink-CDC启动入口
 *
 * @create: 2024-10-23 14:23
 **/
public class FlinkCDCSqlServerSourceFactory extends MQListenerFactory {
    private transient IConsumerHandle consumerHandle;
    /**
     * binlog监听在独立的slot中执行
     */
    @FormField(ordinal = 2, advance = true, type = FormFieldType.ENUM, validate = {Validator.require})
    public boolean independentBinLogMonitor;

    @FormField(ordinal = 0, validate = {Validator.require})
    public CDCStartupOptions startupOptions;

    @FormField(ordinal = 1, type = FormFieldType.ENUM, validate = {Validator.require})
    public String timeZone;

    StartupOptions getStartupOptions() {
        return startupOptions.getOptionsType();
    }


    @Override
    public IFlinkColCreator<FlinkCol> createFlinkColCreator() {
        return (meta, colIndex) -> {
            return meta.getType().accept(new SqlServerCDCTypeVisitor(meta, colIndex));
        };
    }

    public IConsumerHandle getConsumerHander() {
        Objects.requireNonNull(this.consumerHandle, "prop consumerHandle can not be null");
        return this.consumerHandle;
    }

    @Override
    public void setConsumerHandle(IConsumerHandle consumerHandle) {
        this.consumerHandle = consumerHandle;
    }

    @Override
    public IMQListener create() {
        return new FlinkCDCSqlServerSourceFunction(this);
    }

    public static class SqlServerCDCTypeVisitor extends AbstractRowDataMapper.DefaultTypeVisitor {
        public SqlServerCDCTypeVisitor(IColMetaGetter meta, int colIndex) {
            super(meta, colIndex);
        }

        @Override
        public FlinkCol dateType(DataType type) {
            // return super.dateType(type);
            return new FlinkCol(meta, type, new AtomicDataType(new DateType(nullable))
                    , new SQLServerDateConvert()
                    , new SQLServerLocalDateProcess()
                    , new RowFieldGetterFactory.DateGetter(meta.getName(), colIndex));
        }

        @Override
        public FlinkCol timestampType(DataType type) {

            return new FlinkCol(meta, type, new AtomicDataType(new TimestampType(nullable, 3)) //DataTypes.TIMESTAMP(3)
                    , new SQLServerTimestampDataConvert()
                    , new SQLServerDateTimeProcess()
                    , new RowFieldGetterFactory.TimestampGetter(meta.getName(), colIndex));
        }
    }

    public static class SQLServerLocalDateProcess extends LocalDateProcess {
        @Override
        public Object apply(Object o) {
            return LocalDate.ofEpochDay((Integer) o);
        }
    }

    public static class SQLServerDateConvert extends SQLServerLocalDateProcess {
        @Override
        public Object apply(Object o) {
            LocalDate localDate = (LocalDate) super.apply(o);
            return Integer.valueOf((int) localDate.toEpochDay());
        }
    }

    public static class SQLServerDateTimeProcess extends FlinkCol.DateTimeProcess {
        @Override
        public Object apply(Object o) {
            return LocalDateTime.ofInstant(Instant.ofEpochMilli((Long) o), ZoneId.systemDefault());
        }
    }

    public static class SQLServerTimestampDataConvert extends SQLServerDateTimeProcess {
        @Override
        public Object apply(Object o) {
            LocalDateTime v = (LocalDateTime) super.apply(o);
            return TimestampData.fromLocalDateTime(v);
        }
    }

    @TISExtension()
    public static class DefaultDescriptor extends BaseDescriptor {
        @Override
        public String getDisplayName() {
            return "Flink-CDC-" + getEndType().name();
        }

        @Override
        public PluginVender getVender() {
            return PluginVender.FLINK_CDC;
        }

        @Override
        public EndType getEndType() {
            return EndType.SqlServer;
        }
    }
}
