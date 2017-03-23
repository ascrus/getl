package getl.hive

import getl.csv.CSVDataset
import getl.data.Dataset
import getl.driver.Driver
import getl.jdbc.JDBCDriver

/**
 * Created by ascru on 15.03.2017.
 */
class HiveDriver extends JDBCDriver {
    HiveDriver() {
        super()

        allowLocalTemporaryTable = true
        localTemporaryTablePrefix = 'TEMPORARY'
    }

    @Override
    public List<Driver.Operation> operations() {
        return super.operations() +
                [Driver.Operation.CLEAR, Driver.Operation.DROP, Driver.Operation.EXECUTE, Driver.Operation.CREATE]
    }

    @Override
    public List<Driver.Support> supported() {
        return super.supported() +
                [Driver.Support.BATCH, Driver.Support.WRITE, Driver.Support.TRANSACTIONAL,
                 Driver.Support.BLOB, Driver.Support.CLOB]
    }

    @Override
    public String defaultConnectURL () {
        return 'jdbc:hive2://{host}/{database}'
    }

    /*
    @Override
    public void bulkLoadFile(CSVDataset source, Dataset dest, Map bulkParams, Closure prepareCode) {

    }*/


}
