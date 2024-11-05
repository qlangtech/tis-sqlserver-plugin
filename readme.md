## 功能说明
此工程为TIS提供SqlServer的增量读/写功能插件支持

* tis-flink-cdc-sqlserver-plugin: 支持基于Flink-CDC的SqlServer 增量读功能
* tis-flink-chunjun-sqlserver-plugin： 支持基于Chunjun的SqlServer 实时写入功能
* tis-testcontainer-sqlserver： 提供基于testcontainer的测试基础组件，为`tis-flink-cdc-sqlserver-plugin`和`tis-flink-chunjun-sqlserver-plugin`单元测试支持

## 使用

```shell
mvn tis:run -Dtis.network.interface.preferred=en0
```

## Flink启动

需要在本机启动，并且在`conf/flink-conf.yaml`配置文件中添加JVM参数`-Dskip_classloader_factory_creation=true -Ddata.dir=${USER_HOME}/tis_tmp/uber-data/data`


