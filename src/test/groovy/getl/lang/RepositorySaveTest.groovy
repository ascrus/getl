//file:noinspection unused
//file:noinspection SpellCheckingInspection
package getl.lang

import getl.jdbc.HistoryPointManager
import getl.lang.sub.RepositorySave
import getl.lang.sub.SaveToRepository
import getl.models.WorkflowStepTestScript
import getl.tfs.TDS
import getl.tfs.TDSTable
import getl.tfs.TFS
import groovy.transform.BaseScript
import groovy.transform.Field

//noinspection GroovyUnusedAssignment
@BaseScript RepositorySave main

@Field TDS con = new TDS().with {
    connectDatabase = 'repositorysave_test'
    login = 'dba'
    password = '12345'
    assert password == '12345'
    connected = true
    storedLogins.admin = 'admin'
    storedLogins.user = 'user'
    return it
}

@Field TDSTable table = embeddedTable {
    useConnection con
    schemaName = 'public'
    tableName = 'table1'
    field('id') { type = integerFieldType; isKey = true }
    field('name') { length = 50; isNull = false }
    attributes.a1 = 1
}

@SaveToRepository(type = 'Connections', env = 'dev', mask = 'test:*')
void connections() {
    cloneConnection('test:con', con)
    assert embeddedConnection('test:con').password == repositoryStorageManager.encryptText('12345')
    embeddedConnection('test:con').attributes.a1 = 1
}

@SaveToRepository(type = 'Datasets', retrieve = true, mask = 'test:*')
void datasets() {
    table.create()
    // added from connection
    addTables(embeddedConnection('test:con'), 'public', 'test')
    embeddedTable('test:table1').attributes.a1 = 1

    embeddedTable('test:table_points', true) {
        useConnection embeddedConnection('test:con')
        HistoryPointManager.prepareTable(it)
    }
}

@SaveToRepository(type = 'Files', mask = 'test:*')
void filemanagers1() {
    files('test:file1', true) {
        rootPath = '/test1'
    }
}

@SaveToRepository(type = 'Files', env = 'Dev, Prod', mask = 'test:*', overwrite = true)
void filemanagers_ftp() {
    ftp('test:ftp1', true) {
        rootPath = '/ftp'
        login = 'user1'
        password = '12345'
        scriptHistoryFile = TFS.systemPath
        assert password == repositoryStorageManager.encryptText('12345')
        storedLogins.user2 = '12345'
        assert storedLogins.user2 == repositoryStorageManager.encryptText('12345')
    }
}

@SaveToRepository(type = 'Files', env = 'Dev', mask = 'test:*', overwrite = true)
void filemanagers2_dev() {
    files('test:file2', true) {
        rootPath = '/test2.dev'
    }
}

@SaveToRepository(type = 'Files', env = 'Prod', mask = 'test:*', overwrite = true)
void filemanagers2_prod() {
    files('test:file2', true) {
        rootPath = '/test2.prod'
    }
}

@SaveToRepository(type = 'Historypoints', mask = 'test:*')
void historypoints() {
    historypoint('test:hp', true) {
        useHistoryTableName 'test:table_points'
        sourceName = 'source1'
        sourceType = identitySourceType
        saveMethod = mergeSave
    }
}

@SaveToRepository(type = 'Sequences', mask = 'test:*')
void sequences() {
    sequence('test:seq', true) {
        useConnection embeddedConnection('test:con')
        name = 'public.s_sequence'
    }
}

@SaveToRepository(type = 'SetOfTables')
void setOfTables() {
    models.setOfTables('test:sets', true) {
        useSourceConnection 'test:con'

        modelVars.put('test 1', 1)

        table('test:table1') {
            objectVars.test2 = 2
            modelAttrs.a1 = 3
            modelAttrs.a2 = [a:1, b:2, c:3]
        }
    }
}

@SaveToRepository(type = 'Workflows')
void workflows() {
    models.workflow('test:workflow', true) {
        start('Start 1') {
            countThreads = 2

            exec('top1') {
                className = WorkflowStepTestScript.name
                vars = [stepName: stepName, stepNum: 1]
            }
            exec('top2') {
                className = WorkflowStepTestScript.name
                vars = [stepName: stepName, stepNum: 2]
            }

            onError {
                exec('error1') {
                    className = WorkflowStepTestScript.name
                    vars = [stepName: stepName, stepNum: -1]
                }
            }

            later('child1') {
                condition = '(configContent.countProcessed == 2)'

                exec('child1') {
                    className = WorkflowStepTestScript.name
                    vars = [stepName: stepName, stepNum: 101]
                }
                exec('child2') {
                    className = WorkflowStepTestScript.name
                    vars = [stepName: stepName, stepNum: 102]
                }

                onError {
                    exec('error2') {
                        className = WorkflowStepTestScript.name
                        vars = [stepName: stepName, stepNum: -101]
                    }
                }

                later('subchild1') {
                    condition = '(configContent.countProcessed == 4)'
                    exec('subchild2') {
                        className = WorkflowStepTestScript.name
                        vars = [stepName: stepName, stepNum: 301]
                    }
                    exec('subchild1') {
                        className = WorkflowStepTestScript.name
                        vars = [stepName: stepName, stepNum: 201]
                    }
                }
            }
        }
    }
}
