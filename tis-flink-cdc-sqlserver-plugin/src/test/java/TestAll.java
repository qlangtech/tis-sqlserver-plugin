import com.qlangtech.tis.plugins.incr.flink.cdc.sqlserver.TestFlinkCDCSqlServerSourceFactory;
import com.qlangtech.tis.plugins.incr.flink.cdc.sqlserver.TestFlinkCDCSqlServerSourceFactoryE2E;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * @create: 2024-10-23 15:31
 **/

@RunWith(Suite.class)
@Suite.SuiteClasses({TestFlinkCDCSqlServerSourceFactory.class, TestFlinkCDCSqlServerSourceFactoryE2E.class})
public class TestAll {
}
