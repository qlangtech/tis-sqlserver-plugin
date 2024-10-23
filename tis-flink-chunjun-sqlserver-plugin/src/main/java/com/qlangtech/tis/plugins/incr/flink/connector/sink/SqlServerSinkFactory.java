package com.qlangtech.tis.plugins.incr.flink.connector.sink;

import com.alibaba.citrus.turbine.Context;
import com.dtstack.chunjun.connector.jdbc.conf.JdbcConf;
import com.dtstack.chunjun.connector.jdbc.dialect.JdbcDialect;
import com.dtstack.chunjun.connector.jdbc.sink.JdbcOutputFormat;
import com.qlangtech.tis.compiler.incr.ICompileAndPackage;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.IEndTypeGetter;
import com.qlangtech.tis.plugin.ds.DataSourceFactory;
import com.qlangtech.tis.plugins.incr.flink.connector.ChunjunSinkFactory;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;

/**
 * @create: 2024-10-23 13:57
 **/
public class SqlServerSinkFactory extends ChunjunSinkFactory {
    @Override
    protected boolean supportUpsetDML() {
        return false;
    }

    @Override
    protected Class<? extends JdbcDialect> getJdbcDialectClass() {
        return null;
    }

    @Override
    protected JdbcOutputFormat createChunjunOutputFormat(DataSourceFactory dsFactory, JdbcConf jdbcConf) {
        return null;
    }

    @Override
    protected void initChunjunJdbcConf(JdbcConf jdbcConf) {

    }

    @Override
    public ICompileAndPackage getCompileAndPackageManager() {
        return null;
    }

    @TISExtension
    public static class DefaultDescriptor extends BasicChunjunSinkDescriptor {
        @Override
        protected boolean validateAll(IControlMsgHandler msgHandler, Context context, PostFormVals postFormVals) {
            return super.validateAll(msgHandler, context, postFormVals);
        }


        @Override
        protected IEndTypeGetter.EndType getTargetType() {
            return EndType.SqlServer;
        }
    }
}
