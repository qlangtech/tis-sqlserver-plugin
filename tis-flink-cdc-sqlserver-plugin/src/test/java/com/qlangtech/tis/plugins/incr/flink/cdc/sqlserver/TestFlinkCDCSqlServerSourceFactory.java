package com.qlangtech.tis.plugins.incr.flink.cdc.sqlserver;

import com.qlangtech.tis.async.message.client.consumer.impl.MQListenerFactory;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.PluginFormProperties;
import com.qlangtech.tis.extension.util.PluginExtraProps;
import com.qlangtech.tis.plugin.common.PluginDesc;
import org.junit.Assert;
import org.junit.Test;

import java.util.Optional;

/**
 * @create: 2024-10-23 15:31
 **/
public class TestFlinkCDCSqlServerSourceFactory {

    @Test
    public void testDescGenerate() {
        PluginDesc.testDescGenerate(FlinkCDCSqlServerSourceFactory.class
                , "flink-cdc-sqlserver-source-factory-descriptor.json");
    }

    @Test
    public void testGetPluginFormPropertyTypes() {
        FlinkCDCSqlServerSourceFactory sqlServerSourceFactory = new FlinkCDCSqlServerSourceFactory();
        Descriptor<MQListenerFactory> descriptor = sqlServerSourceFactory.getDescriptor();
        Assert.assertNotNull(descriptor);

        PluginFormProperties propertyTypes = descriptor.getPluginFormPropertyTypes();
        Assert.assertEquals(-1, propertyTypes.getKVTuples().size());

        // TODO 增加对每个Property的名称及属性等相关内容断言
    }

    @Test
    public void testPluginExtraPropsLoad() throws Exception {
        Optional<PluginExtraProps> extraProps = PluginExtraProps.load(FlinkCDCSqlServerSourceFactory.class);
        Assert.assertTrue(extraProps.isPresent());
        // TODO 增加对extraProp属性的Key的断言
    }

}
