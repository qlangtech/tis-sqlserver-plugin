package com.microsoft.sqlserver.jdbc;

import com.qlangtech.tis.plugins.incr.flink.cdc.sqlserver.SQLServerConnectionFactory;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-11-30 15:38
 * @see SQLServerConnectionFactory
 * @see io.debezium.connector.sqlserver.SqlServerConnection
 **/
public class SQLServerDriver {
    static {
        System.out.println("this is puppet for deceive SqlServerConnection when execute createConnectionFactory in constructor ，the creation is delegate to "
                + SQLServerConnectionFactory.class.getCanonicalName());
    }
}
