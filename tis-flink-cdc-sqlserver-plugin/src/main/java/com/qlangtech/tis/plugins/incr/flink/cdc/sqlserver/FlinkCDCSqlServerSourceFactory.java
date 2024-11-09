package com.qlangtech.tis.plugins.incr.flink.cdc.sqlserver;

import com.qlangtech.plugins.incr.flink.cdc.FlinkCol;
import com.qlangtech.tis.async.message.client.consumer.IConsumerHandle;
import com.qlangtech.tis.async.message.client.consumer.IFlinkColCreator;
import com.qlangtech.tis.async.message.client.consumer.IMQListener;
import com.qlangtech.tis.async.message.client.consumer.impl.MQListenerFactory;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.ds.IColMetaGetter;
import com.qlangtech.tis.plugins.incr.flink.cdc.AbstractRowDataMapper;

import java.util.Objects;

/**
 * SqlServer 基于Flink-CDC启动入口
 *
 * @create: 2024-10-23 14:23
 **/
public class FlinkCDCSqlServerSourceFactory extends MQListenerFactory {
    private transient IConsumerHandle consumerHandle;

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
