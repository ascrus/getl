package getl.transform

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.data.Connection
import getl.data.Dataset
import getl.data.Field
import getl.data.sub.AttachData
import getl.exception.ExceptionGETL
import groovy.transform.CompileStatic
import groovy.transform.InheritConstructors

/**
 * Array dataset
 * @author Alexsey Konstantinov
 */
@CompileStatic
@InheritConstructors
class ArrayDataset extends Dataset implements AttachData {
    @Override
    protected void registerParameters() {
        super.registerParameters()

        methodParams.register('eachRow', ['localDatasetData'])
    }

    @Override
    protected void initParams() {
        super.initParams()
        connection = new ArrayDatasetConnection()
        field = [Field.New('value') { type = objectFieldType }]
    }

    @Override
    Boolean allowChangeFields() { false }

    /**
     * Local dataset data<br><br>
     * The data must be in the form of a iterable object.
     */
    @JsonIgnore
    @Override
    Object getLocalDatasetData() { sysParams.localDatasetData }
    /**
     * Local dataset data<br><br>
     * The data must be in the form of a iterable object.
     */
    @Override
    void setLocalDatasetData(Object value) { sysParams.localDatasetData = value }

    /** Count row */
    Long countRow() {
        return rows().size()
    }

    @Override
    void setConnection(Connection value) {
        if (value != null && !(value instanceof ArrayDatasetConnection))
            throw new ExceptionGETL('The file dataset only supports array dataset connections!')

        super.setConnection(value)
    }

    /** Use array dataset connection */
    void useConnection(ArrayDatasetConnection value) {
        setConnection(value)
    }

    /** Current array dataset connection */
    ArrayDatasetConnection currentArrayDatasetConnection() { connection as ArrayDatasetConnection }
}