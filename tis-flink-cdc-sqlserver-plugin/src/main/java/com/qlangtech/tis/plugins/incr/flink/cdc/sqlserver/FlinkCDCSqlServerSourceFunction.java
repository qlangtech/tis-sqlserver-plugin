package com.qlangtech.tis.plugins.incr.flink.cdc.sqlserver;

import com.qlangtech.tis.async.message.client.consumer.IAsyncMsgDeserialize;
import com.qlangtech.tis.async.message.client.consumer.IConsumerHandle;
import com.qlangtech.tis.async.message.client.consumer.IMQListener;
import com.qlangtech.tis.async.message.client.consumer.MQConsumeException;
import com.qlangtech.tis.coredefine.module.action.TargetResName;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.IDataxReader;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import org.apache.flink.api.common.JobExecutionResult;

import java.util.List;

/**
 * 具体实现可参考： https://github.com/qlangtech/plugins/blob/master/tis-incr/tis-flink-cdc-mysql-plugin/src/main/java/com/qlangtech/tis/plugins/incr/flink/cdc/mysql/FlinkCDCMysqlSourceFunction.java
 * @author: 百岁（baisui@qlangtech.com）
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
    public JobExecutionResult start(TargetResName dataxName, IDataxReader rdbmsReader, List<ISelectedTab> tabs, IDataxProcessor dataXProcessor) throws MQConsumeException {
        // 具体实现可参考
        return null;
    }

}
