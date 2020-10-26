package getl.lang.sub

import getl.jdbc.JDBCConnection
import getl.lang.Getl
import getl.models.sub.RepositoryMapTables
import getl.models.sub.RepositoryMonitorRules
import getl.models.sub.RepositoryReferenceFiles
import getl.models.sub.RepositoryReferenceVerticaTables
import getl.models.sub.RepositorySetOfTables
import static org.junit.Assert.assertEquals
import static org.junit.Assert.assertNotNull
import static org.junit.Assert.assertTrue

/**
 * Manager of saving objects to the repository
 * @author Alexsey Konstantinov
 */
class RepositorySave extends Getl {
    static void Start(Class<RepositorySave> startClass, String[] args) {
        Application(startClass, args.toList() + ['environment=dev'])
    }

    /** Init object before save objects */
    protected void initRepository() { }


    /** Finalize object after save objects */
    protected void doneRepository() { }

    @Override
    Object run() {
        super.run()
        initRepository()
        try {
            getClass().methods.each { method ->
                def an = method.getAnnotation(SaveToRepository)
                if (an != null) {
                    repositoryStorageManager { autoLoadFromStorage = true }
                    repositoryStorageManager.clearRepositories()
                    def env = an.env() ?: 'all'
                    def retrieve = getl.utils.BoolUtils.IsValue(an.retrieve())
                    def type = an.type()
                    assert type in ['Connections', 'Datasets', 'Files', 'Historypoints', 'Sequences',
                                    'ReferenceFiles', 'ReferenceVerticaTables', 'MonitorRules', 'SetOfTables', 'MapTables'],
                            "Unknown type \"$type\""
                    logInfo "Process method \"${method.name}\" with type \"$type\" from \"$env\" environment"
                    thisObject."${method.name}"()
                    def saveMethod = 'save' + type
                    if (type in ['Connections', 'Files'])
                        thisObject."$saveMethod"(env)
                    else if (type == 'Datasets')
                        thisObject."$saveMethod"(retrieve)
                    else
                        thisObject."$saveMethod"()
                }
            }
        }
        finally {
            doneRepository()
        }
    }

    /** Save connections */
    static void saveConnections(String env = 'dev', String mask = null) {
        Dsl {
            def count = repositoryStorageManager.saveRepository(RepositoryConnections, mask, env)
            assertEquals(listConnections(mask).size(), count)
            logInfo "For environment \"$env\" $count connections saved"
        }
    }

    /** Save datasets */
    static void saveDatasets(String mask = null, Boolean retrieveFields = false) {
        Dsl {
            if (retrieveFields) {
                processJdbcTables(mask) { tableName ->
                    def tbl = jdbcTable(tableName)
                    if (tbl.field.isEmpty()) {
                        tbl.retrieveFields()
                        assert !tbl.field.isEmpty(), "Failed to read the fields of table \"$tbl\"!"
                    }
                }
            }
            def count = repositoryStorageManager.saveRepository(RepositoryDatasets, mask)
            assertEquals(listDatasets(mask).size(), count)
            logInfo "$count datasets saved"
        }
    }

    /** Save datasets */
    static void saveDatasets(Boolean retrieveFields) {
        saveDatasets(null, retrieveFields)
    }

    /** Retrieve and add tables for the specified connection schemata to repository */
    static void addTables(JDBCConnection con, String schema, String group, List<String> tables = null) {
        assertNotNull(con)
        assertNotNull(schema)
        assertNotNull(group)

        Dsl {
            con.with {
                def list = retrieveDatasets {
                    schemaName = schema
                    tableMask = tables
                }
                assertTrue(list.size() > 0)
                addTablesToRepository(list, group)
                assertEquals(list.size(), repositoryStorageManager.saveRepository(RepositoryDatasets))

                logInfo "Added ${list.size()} tables for schemata \"$schema\" to \"$group\" group in repository"
            }
        }
    }

    /** Save file managers */
    static void saveFiles(String env = 'dev', String mask = null) {
        Dsl {
            def count = repositoryStorageManager.saveRepository(RepositoryFilemanagers, mask, env)
            assertEquals(listFilemanagers(mask).size(), count)
            logInfo "For environment \"$env\" $count file managers saved"
        }
    }

    /** Save history point managers */
    static void saveHistorypoints(String mask = null) {
        Dsl {
            def count = repositoryStorageManager.saveRepository(RepositoryHistorypoints, mask)
            assertEquals(listHistorypoints(mask).size(), count)
            logInfo "$count history point managers saved"
        }
    }

    /** Save sequences */
    static void saveSequences(String mask = null) {
        Dsl {
            def count = repositoryStorageManager.saveRepository(RepositorySequences, mask)
            assertEquals(listSequences(mask).size(), count)
            logInfo "$count sequences saved"
        }
    }

    /** Save models of reference files */
    static void saveReferenceFiles(String mask = null) {
        Dsl {
            def count = repositoryStorageManager.saveRepository(RepositoryReferenceFiles, mask)
            assertEquals(models.listReferenceFiles(mask).size(), count)
            logInfo "$count model of reference files saved"
        }
    }

    /** Save model of reference Vertica tables */
    static void saveReferenceVerticaTables(String mask = null) {
        Dsl {
            def count = repositoryStorageManager.saveRepository(RepositoryReferenceVerticaTables, mask)
            assertEquals(models.listReferenceVerticaTables(mask).size(), count)
            logInfo "$count models of reference Vertica tables saved"
        }
    }

    /** Save model of monitoring rules */
    static void saveMonitorRules(String mask = null) {
        Dsl {
            def count = repositoryStorageManager.saveRepository(RepositoryMonitorRules, mask)
            assertEquals(models.listMonitorRules(mask).size(), count)
            logInfo "$count model of monitoring rules saved"
        }
    }

    /** Save models of set tables */
    static void saveSetOfTables(String mask = null) {
        Dsl {
            def count = repositoryStorageManager.saveRepository(RepositorySetOfTables, mask)
            assertEquals(models.listSetOfTables(mask).size(), count)
            logInfo "$count models of tablesets saved"
        }
    }

    /** Save models of map tables */
    static void saveMapTables(String mask = null) {
        Dsl {
            def count = repositoryStorageManager.saveRepository(RepositoryMapTables, mask)
            assertEquals(models.listMapTables(mask).size(), count)
            logInfo "$count models of map tables saved"
        }
    }
}