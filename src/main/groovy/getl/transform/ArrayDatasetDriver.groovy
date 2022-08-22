//file:noinspection UnnecessaryQualifiedReference
package getl.transform

import getl.csv.CSVDataset
import getl.data.Dataset
import getl.data.Field
import getl.driver.Driver
import getl.exception.ExceptionGETL
import getl.utils.ConvertUtils
import groovy.transform.CompileStatic
import groovy.transform.InheritConstructors

/**
 * Array dataset driver class
 * @author Alexsey Konstantinov
 *
 */
@CompileStatic
@InheritConstructors
class ArrayDatasetDriver extends Driver {
    @SuppressWarnings("UnnecessaryQualifiedReference")
    @Override
    List<Driver.Support> supported() {
        return [Driver.Support.EACHROW]
    }

    @Override
    List<Driver.Operation> operations() {
        return []
    }

    @Override
    Boolean isConnected() {
        return true
    }

    @Override
    void connect() { }

    @Override
    void disconnect() { }

    @Override
    List<Object> retrieveObjects(Map params, Closure<Boolean> filter) {
        throw new ExceptionGETL("Retrieve objects not supported!")
    }

    @Override
    List<Field> fields(Dataset dataset) {
        throw new ExceptionGETL("Retrieve fields not supported!")
    }

    @Override
    void startTran(Boolean useSqlOperator = false) {
        throw new ExceptionGETL("Transaction not supported!")
    }

    @Override
    void commitTran(Boolean useSqlOperator = false) {
        throw new ExceptionGETL("Transaction not supported!")
    }

    @Override
    void rollbackTran(Boolean useSqlOperator = false) {
        throw new ExceptionGETL("Transaction not supported!")
    }

    @Override
    void createDataset(Dataset dataset, Map params) {
        throw new ExceptionGETL("Create dataset not supported!")
    }

    @Override
    void dropDataset(Dataset dataset, Map params) {
        throw new ExceptionGETL("Drop dataset not supported!")
    }

    @Override
    Long eachRow(Dataset dataset, Map params, Closure prepareCode, Closure code) {
        def ds = dataset as ArrayDataset
        def res = 0L

        if (prepareCode != null)
            prepareCode.call(ds.field)

        def data = ConvertUtils.Object2List(params.localDatasetData?:ds.localDatasetData)
        if (data == null)
            return res

        def fn = ds.field[0].name.toLowerCase()
        data.each { elem ->
            res++
            def row = [:] as Map<String, Object>
            row.put(fn, elem)
            code.call(row)
        }

        return res
    }

    @Override
    void openWrite(Dataset dataset, Map params, Closure prepareCode) {
        throw new ExceptionGETL("Write to dataset not supported!")
    }

    @Override
    void write(Dataset dataset, Map row) {
        throw new ExceptionGETL("Write to dataset not supported!")
    }

    @Override
    void doneWrite(Dataset dataset) {
        throw new ExceptionGETL("Write to dataset not supported!")
    }

    @Override
    void closeWrite(Dataset dataset) {
        throw new ExceptionGETL("Write to dataset not supported!")
    }

    @Override
    void bulkLoadFile(CSVDataset source, Dataset dest, Map params, Closure prepareCode) {
        throw new ExceptionGETL("Bulk load not supported!")
    }

    @Override
    void clearDataset(Dataset dataset, Map params) {
        (dataset as ArrayDataset).localDatasetData = null
    }

    @Override
    Long executeCommand(String command, Map params) {
        throw new ExceptionGETL("Execution command not supported!")
    }

    @Override
    Long getSequence(String sequenceName) {
        throw new ExceptionGETL("Sequence not supported!")
    }
}