package com.qlangtech.tis.plugins.incr.flink.cdc.sqlserver.startup;

import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-11-29 10:28
 **/
public class Snapshot extends CDCStartupOptions {
    @Override
    public StartupOptions getOptionsType() {
        return StartupOptions.snapshot();
    }

    @TISExtension
    public static class DefaultDescriptor extends Descriptor<CDCStartupOptions> {
        @Override
        public String getDisplayName() {
            return Snapshot.class.getSimpleName();
        }
    }
}
