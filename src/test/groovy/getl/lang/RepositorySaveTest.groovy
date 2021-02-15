package getl.lang

import getl.lang.sub.RepositorySave
import getl.lang.sub.SaveToRepository
import getl.tfs.TDS
import getl.tfs.TDSTable
import getl.tfs.TFS

class RepositorySaveTest extends RepositorySave {
    def con = new TDS().with {
        connectDatabase = 'repositorysave_test'
        login = 'dba'
        password = '12345'
        connected = true
        storedLogins.admin = 'admin'
        storedLogins.user = 'user'
        return it
    }

    def table = embeddedTable {
        useConnection con
        schemaName = 'public'
        tableName = 'table1'
        field('id') { type = integerFieldType; isKey = true }
        field('name') { length = 50; isNull = false }
        create()
    }

    @SaveToRepository(type = 'Connections', env = 'dev', mask = 'test:*')
    void connections() {
        cloneConnection('test:con', con)
    }

    @SaveToRepository(type = 'Datasets', retrieve = true, mask = 'test:*')
    void datasets() {
        // added from connection
        addTables(embeddedConnection('test:con'), 'public', 'test')
    }

    @SaveToRepository(type = 'Files', mask = 'test:*')
    void filemanagers1() {
        files('test:file1', true) {
            rootPath = '/test1'
        }
    }

    @SaveToRepository(type = 'Files', env = 'Dev, Prod', mask = 'test:*')
    void filemanagers2() {
        files('test:file2', true) {
            rootPath = '/test2'
        }
    }

    @SaveToRepository(type = 'Historypoints', mask = 'test:*')
    void historypoints() {
        historypoint('test:hp', true) {
            useConnection embeddedConnection('test:con')
            schemaName = 'public'
            tableName = 's_hp'
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
                attrs.a1 = 3
                attrs.a2 = [a:1, b:2, c:3]
            }
        }
    }
}