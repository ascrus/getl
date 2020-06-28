/*
 GETL - based package in Groovy, which automates the work of loading and transforming data. His name is an acronym for "Groovy ETL".

 GETL is a set of libraries of pre-built classes and objects that can be used to solve problems unpacking,
 transform and load data into programs written in Groovy, or Java, as well as from any software that supports
 the work with Java classes.

 Copyright (C) EasyData Company LTD

 This program is free software: you can redistribute it and/or modify
 it under the terms of the GNU Lesser General Public License as published by
 the Free Software Foundation, either version 3 of the License, or
 (at your option) any later version.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.

 You should have received a copy of the GNU General Public License and
 GNU Lesser General Public License along with this program.
 If not, see <http://www.gnu.org/licenses/>.
*/
package getl.models

import getl.exception.ExceptionModel
import getl.jdbc.QueryDataset
import getl.jdbc.TableDataset
import getl.models.opts.BaseSpec
import getl.models.opts.MonitorRuleSpec
import getl.models.sub.BaseModel
import getl.proc.Executor
import getl.proc.Flow
import getl.tfs.TDS
import getl.tfs.TDSTable
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

    /** Monitoring status storage table name */
    String getStatusTableName() { params.statusTableName as String }
    /** Monitoring status storage table name */
    void useStatusTable(String value) {
        if (value == null) {
            params.statusTableName = null
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

        params.statusTableName = value?.dslNameObject
    }
    /** Object monitoring status storage table */
    TableDataset getStatusTable() {
        dslCreator.jdbcTable(statusTableName)
    }

    /** Number of concurrency rule processing threads */
    Integer getCountThreads() { (params.countThreads as Integer)?:1 }
    /** Number of concurrency rule processing threads */
    void setCountThreads(Integer value) { params.countThreads = value }

    /** Check the query is correct */
    @SuppressWarnings("GrMethodMayBeStatic")
    void validQuery(QueryDataset query) {
        if (query.connection == null)
            throw new ExceptionModel("The connection for the query is not specified!")
        if (query.query == null)
            throw new ExceptionModel("Query does not have a sql text!")
        if (query.dslNameObject == null)
            throw new ExceptionModel("Query is not registered in the repository!")
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
        def stattab = statusTable
        if (stattab.exists)
            throw new ExceptionModel('Status table already exists!')

        StatusTableSetFields(stattab)
        stattab.create()
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
            field('open_incident') { type = booleanFieldType; isNull = false }
        }
    }

    /** Get current date query */
    private QueryDataset queryCurrentDate = new QueryDataset(query: 'SELECT {now} AS curdate {from}')

    Date currentDateTime
    /** Get current date */
    @Synchronized
    Date getCurrentDateTime() {
        if (this.currentDateTime != null)
            return this.currentDateTime

        queryCurrentDate.with {
            queryParams.now = currentJDBCConnection.currentJDBCDriver.nowFunc
            queryParams.from = (currentJDBCConnection.currentJDBCDriver.sysDualTable != null)?
                    "FROM ${currentJDBCConnection.currentJDBCDriver.sysDualTable}" : ''
        }
        return queryCurrentDate.rows()[0].curdate as Date
    }
    @Synchronized
    void setCurrentDateTime(Date value) { this.currentDateTime = value }

    @Override
    void checkModel(boolean checkObjects = true) {
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
    TDSTable histtab
    /** Last checked rule status */
    @Synchronized
    TDSTable getLastCheckStatusTable() { histtab }

    /**
     * Checking the status of rules
     * @return true if everything is correct and false if error reporting is required
     */
    boolean check() {
        checkModel(true)

        if (!statusTable.exists)
            createStatusTable()
        else
            StatusTableSetFields(statusTable)

        def stattab = statusTable

        queryCurrentDate.useConnection(statusTable.currentJDBCConnection)
        try {
            // Copy status table to temp
            new Flow().copy(source: stattab, dest: lastCheckStatusTable, clear: true) { s, d ->
                d.operation = 'NONE'
                d.is_notification = false
            }

            // Threading rules
            new Executor(abortOnError: true).run(usedRules, countThreads) { elem ->
                def rule = elem as MonitorRuleSpec

                // Clone temp status table
                def hTab = lastCheckStatusTable.cloneDatasetConnection()
                // Detect rows by rule
                def hRows = hTab.rows(where: "rule_name = '${rule.queryName}'")
                // Detect last check time
                def hMinStat = (!hRows.isEmpty())?((hRows.min { r -> r.check_time }).check_time as Date):null

                // Detect current server time
                def curDate = getCurrentDateTime()

                // Return if verification time is not exceeded
                if (hMinStat != null && TimeCategory.minus(curDate, hMinStat) < rule.checkFrequency) {
                    Logs.Fine("Rule \"${rule.queryName}\" is skipped by check time")
                    return
                }

                if (hMinStat != null)
                    Logs.Fine("Check rule \"${rule.queryName}\" (last check was ${DateUtils.FormatDate('yyyy-MM-dd HH:mm', hMinStat)}) ...")
                else
                    Logs.Fine("Check rule \"${rule.queryName}\"")

                // Retrieving rows from a rule query
                def ruleQuery = rule.query.cloneDatasetConnection() as QueryDataset
                ruleQuery.queryParams.putAll(rule.objectVars + modelVars)
                def statRows = ruleQuery.rows()
                if (statRows.isEmpty()) {
                    Logs.Fine("Rule \"${rule.queryName}\" has no rows and is skipped")
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
                        def stateTime = statRow.value as Date
                        // Current lag
                        def curLag = TimeCategory.minus(curDate, stateTime)
                        // Correct status
                        def isCorrect = (curLag <= rule.lagTime)
                        // Set notification status
                        def isNotification = !isCorrect && !groupRow.open_incident
                        if (isNotification && !isNewStatRow && rule.notificationTime != null && groupRow.last_error_time != null) {
                            isNotification = !(TimeCategory.minus(curDate, groupRow.last_error_time as Date) < rule.notificationTime)
                        }

                        // Set field status
                        groupRow.check_time = curDate
                        groupRow.state_time = stateTime
                        groupRow.is_correct = isCorrect
                        groupRow.is_notification = isNotification
                        if (isCorrect) {
                            groupRow.first_error_time = null
                            groupRow.last_error_time = null
                        }
                        else if (groupRow.last_error_time == null || isNotification) {
                            if (groupRow.first_error_time == null)
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
        }
        finally {
            queryCurrentDate.connection = null
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
    String htmlNotification() {
        StringBuilder sb = new StringBuilder()

        sb << """
<html>
<title>Identified Monitor Errors "$dslNameObject"</title>
<body>
<h1>${lastCheckStatusTable.countRow('NOT is_correct')} errors were detected while checking rules</h1>
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
        lastCheckStatusTable.eachRow(where: 'NOT is_correct', order: ['(first_error_time = check_time) DESC', 'open_incident', 'rule_name', 'code']) { row ->
            def rule = findRule(row.rule_name as String)
            if (rule == null)
                throw new ExceptionModel("Unknown rule \"${row.rule_name}\"")

            Integer groupError
            if (row.first_error_time == row.check_time)
                groupError = 1
            else if (!row.open_incident)
                groupError = 2
            else
                groupError = 3

            if (lastGroupError == null || lastGroupError != groupError) {
                lastGroupError = groupError
                curRule = ''

                switch (groupError) {
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
                    ruleName = StringUtils.ReplaceMany(rule.description, ['<': '&lt;', '>':'&gt;', '&': '&amp;'])
                else
                    ruleName = StringUtils.ReplaceMany(row.rule_name as String, ['<': '&lt;', '>':'&gt;', '&': '&amp;'])
            }

            sb << """
<tr>
    <td>$ruleName</td>
    <td>${(row.code != '<-|None|->')?row.code:''}
    <td>${DateUtils.FormatDate('yyyy-MM-dd HH:mm', row.check_time as Date)}</td>
    <td>${DateUtils.FormatDate('yyyy-MM-dd HH:mm', row.state_time as Date)}</td>
    <td>${rule.lagTime}</td>
    <td>${DateUtils.FormatDate('yyyy-MM-dd HH:mm', row.first_error_time as Date)}</td>
    <td>${TimeCategory.minus(row.check_time as Date, row.state_time as Date)}</td>
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
     * @param smtpServer
     */
    void sendToSmtp(EMailer smtpServer) {
        smtpServer.with {
            def text = htmlNotification()
            sendMail(toAddress, "Monitor \"${this.dslNameObject}\" detected ${lastCheckStatusTable.countRow('NOT is_correct')} errors", text, true)
        }
    }
}