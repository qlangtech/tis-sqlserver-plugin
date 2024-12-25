package com.qlangtech.tis.plugins.incr.flink.cdc.sqlserver.startup;

import com.qlangtech.tis.extension.Describable;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-11-29 10:05
 **/
public abstract class CDCStartupOptions implements Describable<CDCStartupOptions> {
    public abstract org.apache.flink.cdc.connectors.base.options.StartupOptions getOptionsType();
}
