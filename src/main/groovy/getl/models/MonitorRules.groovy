package getl.models

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.exception.ExceptionDSL
import getl.exception.ExceptionModel
import getl.jdbc.QueryDataset
import getl.jdbc.TableDataset
import getl.models.sub.BaseSpec
import getl.models.opts.MonitorRuleSpec
import getl.models.sub.BaseModel
import getl.proc.Executor
import getl.proc.Flow
import getl.tfs.TDS
import getl.tfs.TDSTable
import getl.utils.BoolUtils
import getl.utils.DateUtils
import getl.utils.EMailer
import getl.utils.Logs
import getl.utils.StringUtils
import groovy.time.TimeCategory
import groovy.transform.InheritConstructors
import groovy.transform.Synchronized
import groovy.transform.stc.ClosureParams
import groovy.transform.stc.SimpleType

/**
 * Rules for monitoring the state of database tables
 * @author Alexsey Konstantinov
 */
@InheritConstructors
class MonitorRules extends BaseModel<MonitorRuleSpec> {
    @Override
    protected void initSpec() {
        super.initSpec()

        histtab = TDS.dataset()
        StatusTableSetFields(histtab)
        histtab.with {
            // for determine new rows
            field('operation')  { length = 6; isNull = false }
            // to determine which rows should be included in the notification
            field('is_notification') { type = booleanFieldType; isNull = false }

            create()
        }
    }

    @Override
    void dslCleanProps() {
        super.dslCleanProps()
        histtab.drop(ifExists: true)
    }

    /** List of used rules */
    List<MonitorRuleSpec> getUsedRules() { usedObjects as List<MonitorRuleSpec> }
    /** List of used rules */
    void setUsedRules(List<MonitorRuleSpec> value) {
        usedObjects.clear()
        if (value != null)
            usedObjects.addAll(value)
    }
    /** List of enabled rules */
    List<MonitorRuleSpec> getEnabledRules() {
        return usedRules.findAll { rule -> BoolUtils.IsValue(rule.enabled, true) }
    }

    /** Monitoring status storage table name */
    String getStatusTableName() { params.statusTableName as String }
    /** Monitoring status storage table name */
    void setStatusTableName(String value) { useStatusTable(value) }

    /** Object monitoring status storage table */
    @JsonIgnore
    TableDataset getStatusTable() {
        return ((statusTableName != null)?dslCreator.jdbcTable(statusTableName):null)
    }

    /** Use monitoring status storage table */
    void useStatusTable(String value) {
        if (value == null) {
            saveParamValue('statusTableName', null)
            return
        }

        def table = dslCreator.jdbcTable(value)
        useStatusTable(table)
    }
    /** Use monitoring status storage table */
    void useStatusTable(TableDataset value) {
        if (value != null) {
            if (value.connection == null)
                throw new ExceptionModel("The connection for the table $value is not specified!")
            if (value.schemaName == null)
                throw new ExceptionModel("Table $value does not have a schema!")
            if (value.tableName == null)
                throw new ExceptionModel("Table $value does not have a table name!")
            if (value.dslNameObject == null)
                throw new ExceptionModel("Table $value is not registered in the repository!")
        }

        saveParamValue('statusTableName', value?.dslNameObject)
    }

    /** Number of concurrency rule processing threads */
    Integer getCountThreads() { (params.countThreads as Integer)?:1 }
    /** Number of concurrency rule processing threads */
    void setCountThreads(Integer value) { saveParamValue('countThreads', value) }

    /** Check the query is correct */
    @SuppressWarnings("GrMethodMayBeStatic")
    void validQuery(QueryDataset query) {
        if (query == null)
            throw new ExceptionDSL('No query specified!')
        if (query.dslNameObject == null)
            throw new ExceptionModel("Query is not registered in the repository!")
        def dsn = query.dslNameObject
        if (query.connection == null)
            throw new ExceptionModel("The connection for the query \"$dsn\" is not specified!")
        if (query.query == null && query.scriptFilePath == null)
            throw new ExceptionModel("Query \"$dsn\" does not have a sql text!")

    }

    /**
     * Find rule by query name
     * @param queryName
     * @return rule specification
     */
    MonitorRuleSpec findRule(String queryName) {
        usedRules.find { r -> r.queryName == queryName } as MonitorRuleSpec
    }

    /**
     * Define monitor source table
     * @param queryName repository query name
     * @param cl defining code
     * @return rule spec
     */
    MonitorRuleSpec rule(String queryName,
                         @DelegatesTo(MonitorRuleSpec)
                         @ClosureParams(value = SimpleType, options = ['getl.models.opts.MonitorRuleSpec'])
                                 Closure cl = null) {
        if (queryName == null) {
            def owner = DetectClosureDelegate(cl)
            if (owner instanceof QueryDataset)
                queryName = (owner as QueryDataset).dslNameObject
        }

        if (queryName == null)
            throw new ExceptionModel("The repository query name is not specified!")

        checkModel()

        def query = dslCreator.query(queryName)
        validQuery(query)
        def dslQueryName = query.dslNameObject

        def parent = findRule(dslQueryName)
        if (parent == null)
            parent = newSpec(dslQueryName) as MonitorRuleSpec

        parent.runClosure(cl)

        return parent
    }

    /**
     * Define monitor source table
     * @param cl defining code
     * @return table spec
     */
    MonitorRuleSpec rule(@DelegatesTo(MonitorRuleSpec)
                                  @ClosureParams(value = SimpleType, options = ['getl.models.opts.MonitorRuleSpec'])
                                    Closure cl) {
        rule(null, cl)
    }

    /** Create status table */
    private void createStatusTable() {
        def tab = statusTable
        if (tab.exists)
            throw new ExceptionModel('Status table already exists!')

        StatusTableSetFields(tab)
        tab.create()
    }

    /** Set fields for the rule status table */
    static void StatusTableSetFields(TableDataset table) {
        table.with {
            field.clear()
            field('rule_name') { length = 255; isKey = true }
            field('code') { length = 512; isKey = true }
            field('check_time') { type = datetimeFieldType; isNull = false }
            field('state_time') { type = datetimeFieldType }
            field('is_correct') { type = booleanFieldType; isNull = false }
            field('first_error_time') { type = datetimeFieldType }
            field('last_error_time') { type = datetimeFieldType }
            field('send_time') { type = datetimeFieldType }
            field('open_incident') { type = booleanFieldType; isNull = false }
        }
    }

    /** Get current date query */
    @SuppressWarnings('SpellCheckingInspection')
    private QueryDataset queryCurrentDate = new QueryDataset(query: 'SELECT {now} AS curdate {from}')

    /** Current date time on server */
    private Date _currentDateTime
    /** Current date time on server */
    @Synchronized
    @JsonIgnore
    Date getCurrentDateTime() {
        if (_currentDateTime == null) {
            queryCurrentDate.useConnection(statusTable.currentJDBCConnection)
            try {
                queryCurrentDate.with {
                    queryParams.now = currentJDBCConnection.currentJDBCDriver.nowFunc
                    queryParams.from = (currentJDBCConnection.currentJDBCDriver.sysDualTable != null) ?
                            "FROM ${currentJDBCConnection.currentJDBCDriver.sysDualTable}" : ''
                }
                _currentDateTime = DateUtils.TruncTime(Calendar.SECOND, queryCurrentDate.rows()[0].curdate as Date)
            }
            finally {
                queryCurrentDate.connection = null
            }
        }

        return _currentDateTime
    }
    @Synchronized
    void setCurrentDateTime(Date value) { this._currentDateTime = value }

    @Override
    void checkModel(Boolean checkObjects = true) {
        if (statusTableName == null)
            throw new ExceptionModel('A table of monitoring status storage is required!')

        super.checkModel(checkObjects)
    }

    @Override
    void checkObject(BaseSpec obj) {
        super.checkObject(obj)

        def rule = obj as MonitorRuleSpec

        if (rule.lagTime == null)
            throw new ExceptionModel("You must specify the frequency interval for checking the rule \"${rule.queryName}\" in parameter \"lagTime\"!")
        if (rule.checkFrequency == null)
            throw new ExceptionModel("You must specify the frequency interval for checking the rule \"${rule.queryName}\" in parameter \"checkFrequency\"!")

        validQuery(rule.query)
    }

    /** Last checked rule status */
    private TDSTable histtab
    /** Last checked rule status */
    @Synchronized
    @JsonIgnore
    TDSTable getLastCheckStatusTable() { histtab }

    /**
     * Checking the status of rules
     * @return returns true if it is not required to notify by a change in the state of the monitor
     */
    Boolean check() {
        checkModel(true)

        if (!statusTable.exists)
            createStatusTable()
        else
            StatusTableSetFields(statusTable)

        def statTab = statusTable
        _currentDateTime = null

        // Copy status table to temp
        new Flow().copy(source: statTab, dest: lastCheckStatusTable, clear: true) { s, d ->
            d.operation = 'NONE'
            d.is_notification = false
        }

        // Threading rules
        new Executor(abortOnError: true).run(enabledRules, countThreads) { elem ->
            def rule = elem as MonitorRuleSpec

            // Clone temp status table
            def hTab = lastCheckStatusTable.cloneDatasetConnection()
            // Detect rows by rule
            def hRows = hTab.rows(where: "rule_name = '${rule.queryName}'")
            // Detect last check time
            def hMinStat = (!hRows.isEmpty())?((hRows.min { r -> r.check_time }).check_time as Date):null

            // Detect current server time
            def curDate = currentDateTime

            // Return if verification time is not exceeded
            if (hMinStat != null && TimeCategory.minus(curDate, hMinStat) < rule.checkFrequency) {
                Logs.Info("Rule \"${rule.queryName}\" is skipped by check time")
                return
            }

            if (hMinStat != null)
                Logs.Finest("Check rule \"${rule.queryName}\" (last check was ${DateUtils.FormatDate('yyyy-MM-dd HH:mm', hMinStat)}) ...")
            else
                Logs.Finest("Check rule \"${rule.queryName}\" ...")

            // Retrieving rows from a rule query
            def ruleQuery = rule.query.cloneDatasetConnection() as QueryDataset
            ruleQuery.queryParams.putAll(rule.objectVars + modelVars)
            def statRows = ruleQuery.rows()
            if (statRows.isEmpty()) {
                Logs.Warning("Rule \"${rule.queryName}\" has no rows and is skipped")
                return
            }

            if (ruleQuery.fieldByName('value') == null)
                throw new ExceptionModel("State field \"value\" was not found in rule \"$rule.queryName\"!")

            def useGroups = (ruleQuery.field.size() > 1)
            if (useGroups) {
                if (ruleQuery.fieldByName('code') == null)
                    throw new ExceptionModel("Group field \"code\" was not found in rule \"$rule.queryName\"!")

                def validTable = TDS.dataset()
                validTable.with {
                    field('code') { length = 512; isNull = false }
                    create()
                }
                new Flow().writeTo(dest: validTable) { add ->
                    statRows.each { row -> add([code: (row.code as String)]) }
                }
                new QueryDataset(connection: validTable.connection).with {
                    setQuery 'SELECT code FROM {table} GROUP BY code HAVING Count(*) > 1 ORDER BY code'
                    queryParams.table = validTable.fullTableName
                    def invalidGroups = rows()
                    if (!invalidGroups.isEmpty()) {
                        def names = invalidGroups.collect { row -> row.code }
                        throw new ExceptionModel("For rule \"${rule.queryName}\" duplicates were revealed by codes: $names!")
                    }
                }

                validTable.drop()
            }

            new Flow().writeTo(dest: hTab, dest_operation: 'MERGE') { writeState ->
                // Row group processing
                statRows.each { statRow ->
                    // Skip empty value code
                    if (statRow.value == null)
                        return

                    // Current group value
                    def groupCode = (statRow.code?:'<-|None|->').toString()

                    // Search for a group rows in status
                    def groupRows = hRows.findAll { r -> r.code == groupCode }
                    if (groupRows.size() > 1)
                        throw new ExceptionModel("Identified multiple rows for rule \"${rule.queryName}\" with group code \"$groupCode\" from status table!")
                    def isNewStatRow = groupRows.isEmpty()

                    // Current group row for status table
                    def groupRow = (!isNewStatRow)?(groupRows[0] + [operation: 'UPDATE']):
                            [rule_name: rule.queryName, code: groupCode, open_incident: false, operation: 'INSERT']

                    // Current status time
                    def stateTime = DateUtils.TruncTime(Calendar.SECOND, statRow.value as Date)
                    // Current lag
                    def curLag = TimeCategory.minus(curDate, stateTime)
                    // Correct status
                    def isCorrect = (curLag <= rule.lagTime)
                    // Set notification status
                    def isNotification = (!groupRow.open_incident && (!isCorrect || (!isNewStatRow && isCorrect && !(groupRows[0].is_correct as Boolean))))
                    def last_send_time = (groupRow.send_time as Date)?:(groupRow.last_error_time as Date)
                    if (!isCorrect && isNotification && !isNewStatRow && rule.notificationTime != null && last_send_time != null) {
                        isNotification = !(TimeCategory.minus(curDate, last_send_time) < rule.notificationTime)
                    }

                    // Set field status
                    groupRow.check_time = curDate
                    groupRow.state_time = stateTime
                    groupRow.is_correct = isCorrect
                    groupRow.is_notification = isNotification
                    if (isCorrect) {
//                        groupRow.first_error_time = null
                        groupRow.last_error_time = null
                    }
                    else if (groupRow.last_error_time == null || isNotification) {
                        if (groupRow.first_error_time == null || groupRows[0].is_correct as Boolean)
                            groupRow.first_error_time = curDate
                        groupRow.last_error_time = curDate
                    }

                    writeState.call(groupRow)

                    if (!isCorrect)
                        Logs.Warning("For rule \"${rule.queryName}\" of group \"$groupCode\" " +
                                "a lag of $curLag was revealed  from the last " +
                                "time ${DateUtils.FormatDate('yyyy-MM-dd HH:mm', stateTime)}!")
                    else
                        Logs.Info("For rule \"${rule.queryName}\" of group \"$groupCode\" , a lag in work was " +
                                "found within the normal range and is $curLag from the last " +
                                "time ${DateUtils.FormatDate('yyyy-MM-dd HH:mm', stateTime)}")
                }
            }
        }

        new Flow().with {
            statusTable.currentJDBCConnection.transaction {
                writeTo(dest: statusTable, dest_operation: 'INSERT') { addStatus ->
                    histtab.eachRow(where: 'operation = \'INSERT\'') { row ->
                        addStatus row
                    }
                }

                writeTo(dest: statusTable, dest_operation: 'UPDATE') { updateStatus ->
                    histtab.eachRow(where: 'operation = \'UPDATE\'') { row ->
                        updateStatus row
                    }
                }
            }
        }

        return (histtab.countRow('is_notification') == 0)
    }

    /** Generate html notification */
    String htmlNotification(List<Map> rows) {
        StringBuilder sb = new StringBuilder()

        sb << """
<html>
<title>Checking the status of rules for monitor "$repositoryModelName"</title>
<body>
<h1>${lastCheckStatusTable.countRow('NOT is_correct')} errors detected, ${lastCheckStatusTable.countRow('is_notification AND is_correct')} errors has been cleared</h1>
<table border="1" cellpadding="10">
<tr bgcolor="Gainsboro">
    <th>Rule</th>
    <th>Group</th>
    <th>Last check time</th>
    <th>Object status time</th>
    <th>Allowable lag</th>
    <th>Error detection time</th>
    <th>Current lag</th>
</tr>"""

        Integer lastGroupError = null
        String curRule

        lastCheckStatusTable.eachRow(where: 'NOT is_correct OR (is_correct AND is_notification)',
                queryParams: [dt: currentDateTime],
                order: ['is_correct DESC', '(first_error_time = ParseDateTime(\'{dt}\', \'yyyy-MM-dd HH:mm:ss\')) DESC', 'open_incident', 'rule_name', 'code']) { row ->
            def rule = findRule(row.rule_name as String)
            if (rule == null)
                throw new ExceptionModel("Unknown rule \"${row.rule_name}\"")

            Integer groupError
            if (row.is_notification as Boolean && row.is_correct as Boolean)
                groupError = 0
            else if (row.first_error_time == currentDateTime)
                groupError = 1
            else if (!row.open_incident)
                groupError = 2
            else
                groupError = 3

            if (lastGroupError == null || lastGroupError != groupError) {
                lastGroupError = groupError
                curRule = ''

                switch (groupError) {
                    case 0:
                        sb << "\n<tr bgcolor=\"Gainsboro\"><td>CLOSED ERRORS:</td><td></td><td></td><td></td><td></td><td></td><td></td></tr>"
                        break
                    case 1:
                        sb << "\n<tr bgcolor=\"Gainsboro\"><td>NEW ERRORS:</td><td></td><td></td><td></td><td></td><td></td><td></td></tr>"
                        break
                    case 2:
                        sb << "\n<tr bgcolor=\"Gainsboro\"><td>ERRORS STILL ACTIVE:</td><td></td><td></td><td></td><td></td><td></td><td></td></tr>"
                        break
                    case 3:
                        sb << "\n<tr bgcolor=\"Gainsboro\"><td>OPEN INCIDENT ERRORS:</td><td></td><td></td><td></td><td></td><td></td><td></td></tr>"
                        break
                }
            }

            String ruleName = ''
            if (curRule == null || row.rule_name != curRule) {
                curRule = row.rule_name
                if (rule.description != null)
                    ruleName = StringUtils.ReplaceMany(rule.description, ['<': '&lt;', '>': '&gt;', '&': '&amp;'])
                else
                    ruleName = StringUtils.ReplaceMany(row.rule_name as String, ['<': '&lt;', '>': '&gt;', '&': '&amp;'])
            }

            sb << """
<tr>
    <td>$ruleName</td>
    <td>${(row.code != '<-|None|->') ? row.code : ''}
    <td>${DateUtils.FormatDate('yyyy-MM-dd HH:mm', row.check_time as Date)}</td>
    <td>${DateUtils.FormatDate('yyyy-MM-dd HH:mm', row.state_time as Date)}</td>
    <td>${rule.lagTime}</td>
    <td>${DateUtils.FormatDate('yyyy-MM-dd HH:mm', row.first_error_time as Date)}</td>
    <td>${TimeCategory.minus(currentDateTime, row.state_time as Date)}</td>
</tr>"""
        }
        sb <<
                    """
    </table>    
    </body>
    </html>
    """

        return sb.toString()
    }

    /**
     * Send notifications to email
     * @param smtpServer mail server for sending email
     * @param title email header template (variables are allowed: name, active and close)
     */
    void sendToSmtp(EMailer smtpServer, String title = null) {
        def rows = lastCheckStatusTable.rows(where: 'NOT is_correct OR (is_correct AND is_notification)',
                queryParams: [dt: currentDateTime],
                order: ['is_correct DESC', '(first_error_time = ParseDateTime(\'{dt}\', \'yyyy-MM-dd HH:mm:ss\')) DESC',
                        'open_incident', 'rule_name', 'code'])
        if (rows.isEmpty()) {
            Logs.Warning('No active events for email notification found!')
            return
        }

        if (title == null)
            title = "Monitor \"{name}\" detected {active} active errors and {close} closed errors"
        def activeErrors = lastCheckStatusTable.countRow('NOT is_correct')
        def closeErrors =  lastCheckStatusTable.countRow('is_notification AND is_correct')
        def titleStr = StringUtils.EvalMacroString(title, [name: repositoryModelName, active: activeErrors, close: closeErrors])

        smtpServer.with {
            Logs.Finest("Sending mail to ${DateUtils.FormatDate('yyyy-MM-dd HH:mm:ss', currentDateTime)} for recipients: $toAddress")
            def text = htmlNotification(rows)
            sendMail(null, titleStr, text, true)
        }

        new Flow().writeTo(dest: statusTable, destParams: [operation: 'UPDATE', updateField: ['first_error_time','send_time']]) { updater ->
            rows.each { row ->
                if (row.is_correct)
                    row.first_error_time = null

                updater.call(row + [send_time: currentDateTime])
            }
        }
    }

    @Override
    String toString() { "Monitoring ${usedObjects.size()} rules in \"$statusTableName\" table" }
}