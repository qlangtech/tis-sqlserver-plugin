package com.qlangtech.tis.plugins.incr.flink.connector.sink;

import com.alibaba.citrus.turbine.Context;
import com.dtstack.chunjun.connector.jdbc.conf.JdbcConf;
import com.dtstack.chunjun.connector.jdbc.dialect.JdbcDialect;
import com.dtstack.chunjun.connector.jdbc.sink.JdbcOutputFormat;
import com.dtstack.chunjun.connector.jdbc.util.JdbcUtil;
import com.google.common.collect.Sets;
import com.qlangtech.tis.compiler.incr.ICompileAndPackage;
import com.qlangtech.tis.compiler.streamcode.CompileAndPackage;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.IEndTypeGetter;
import com.qlangtech.tis.plugin.ds.DataSourceFactory;
import com.qlangtech.tis.plugins.incr.flink.chunjun.common.ColMetaUtils;
import com.qlangtech.tis.plugins.incr.flink.connector.ChunjunSinkFactory;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import com.dtstack.chunjun.connector.sqlserver.dialect.SqlserverDialect;

/**
 * reference: com.qlangtech.tis.plugins.incr.flink.connector.sink.MySQLSinkFactory
 *
 * @create: 2024-10-23 13:57
 **/
public class SqlServerSinkFactory extends ChunjunSinkFactory {
    @Override
    protected boolean supportUpsetDML() {
        return true;
    }

    @Override
    protected Class<? extends JdbcDialect> getJdbcDialectClass() {
        return SqlserverDialect.class;
    }

    @Override
    protected JdbcOutputFormat createChunjunOutputFormat(DataSourceFactory dsFactory, JdbcConf jdbcConf) {
        return new TISSqlServerOutputFormat(dsFactory, ColMetaUtils.getColMetasMap(this, jdbcConf));
    }

    @Override
    protected void initChunjunJdbcConf(JdbcConf jdbcConf) {
        JdbcUtil.putExtParam(jdbcConf);
    }

    @Override
    public ICompileAndPackage getCompileAndPackageManager() {
        return new CompileAndPackage(Sets.newHashSet(SqlServerSinkFactory.class));
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
