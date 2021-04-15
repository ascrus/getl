package getl.lang

import getl.lang.sub.RepositorySave
import getl.lang.sub.SaveToRepository
import getl.tfs.TDS

class RepositorySaveTest extends RepositorySave {
    def con = new TDS().with {
        connectDatabase = 'repositorysave_test'
        login = 'dba'
        password = '12345'
        assert password == '12345'
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
        assert embeddedConnection('test:con').password == repositoryStorageManager.encryptText('12345')
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

        ftp('test:ftp1', true) {
            rootPath = '/'
            login = 'user1'
            password = '12345'
            assert password == repositoryStorageManager.encryptText('12345')
            storedLogins.user2 = '12345'
            assert storedLogins.user2 == repositoryStorageManager.encryptText('12345')
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
                modelAttrs.a1 = 3
                modelAttrs.a2 = [a:1, b:2, c:3]
            }
        }
    }
}