package getl.tfs

import getl.data.Connection
import getl.exception.ExceptionGETL
import getl.h2.H2Table
import getl.utils.StringUtils
import groovy.transform.InheritConstructors

/**
 * Table with temp database
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class TDSTable extends H2Table {
    @Override
    protected void initParams() {
        super.initParams()

        tableName = "TDS_" + StringUtils.TransformObjectName(StringUtils.RandomStr()).toUpperCase()
    }

    @Override
    void setConnection(Connection value) {
        if (value != null && !(value instanceof TDS))
            throw new ExceptionGETL('The tds table only supports tds connections!')
        super.setConnection(value)
    }
}