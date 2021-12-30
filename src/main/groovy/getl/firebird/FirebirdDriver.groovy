package getl.firebird

import getl.driver.Driver
import getl.jdbc.JDBCDataset
import getl.jdbc.JDBCDriver
import groovy.transform.InheritConstructors

/**
 * Firebird driver class
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class FirebirdDriver extends JDBCDriver {
    @Override
    protected void initParams() {
        super.initParams()

        connectionParamBegin = '?'
        connectionParamJoin = ';'

        //commitDDL = true
        transactionalDDL = true

        caseObjectName = 'UPPER'
        caseRetrieveObject = 'UPPER'
        //caseQuotedName = false

        sqlExpressions.now = 'cast(\'NOW\' as timestamp)'
        sqlExpressions.sequenceNext = "SELECT NEXT VALUE FOR {value} AS id FROM {sysDualTable}"
        sqlExpressions.sysDualTable = 'RDB$DATABASE'
        sqlExpressions.ddlAutoIncrement = 'GENERATED BY DEFAULT AS IDENTITY'
        sqlExpressions.ddlCreateField = '{column} {type}{ %increment%}{ %default%}{ %check%}{ %compute%}{ %not_null%}'

        createViewTypes = ['CREATE']

        ruleNameNotQuote = '(?i)^[a-z]+[a-z0-9_]*$'
        ruleQuotedWords.add('VALUE')
    }

    @SuppressWarnings("UnnecessaryQualifiedReference")
    @Override
    List<Driver.Support> supported() {
        return super.supported() +
            [Support.GLOBAL_TEMPORARY, Support.SEQUENCE, Support.BLOB, Support.INDEX, Support.TIME, Support.DATE, Support.BOOLEAN, Support.AUTO_INCREMENT] -
            [/*Support.DEFAULT_VALUE, Support.COMPUTE_FIELD, */Support.DATABASE, Support.SCHEMA, Support.SELECT_WITHOUT_FROM]
    }

    @SuppressWarnings("UnnecessaryQualifiedReference")
    @Override
    List<Driver.Operation> operations() {
        return super.operations() - [Operation.CREATE_SCHEMA, Operation.TRUNCATE]
    }

    @Override
    String defaultConnectURL() {
        return 'jdbc:firebirdsql://{host}/{database}'
    }

    @SuppressWarnings("UnnecessaryQualifiedReference")
    @Override
    Map<String, Map<String, Object>> getSqlType() {
        def res = super.getSqlType()
        res.BLOB.useLength = JDBCDriver.sqlTypeUse.NEVER

        return res
    }

    @Override
    void sqlTableDirective(JDBCDataset dataset, Map params, Map dir) {
        super.sqlTableDirective(dataset, params, dir)

        if (params.offs != null) {
            dir.afterOrderBy = ((dir.afterOrderBy != null) ? (dir.afterOrderBy + '\n') : '') + "OFFSET ${params.offs} ROWS"
            params.offs = null
        }

        if (params.limit != null) {
            dir.afterOrderBy = ((dir.afterOrderBy != null) ? (dir.afterOrderBy + '\n') : '') + "FETCH FIRST ${params.limit} ROWS ONLY"
            params.limit = null
        }
    }

    @Override
    Boolean blobReadAsObject() { return false }
}