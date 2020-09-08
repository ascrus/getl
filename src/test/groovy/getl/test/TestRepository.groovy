package getl.test

import getl.lang.Getl
import getl.utils.FileUtils
import org.junit.Before

import static getl.lang.Getl.Dsl

class TestRepository extends GetlDslTest {
    final def repConfigFileName = 'tests/repository/vars.conf'

    @Before
    void setUp() {
        if (!FileUtils.ExistsFile(repConfigFileName)) return
        Dsl {
            configuration {
                load repConfigFileName
            }

            repositoryStorageManager {
                storagePath = 'resource:/repository'
                autoLoadFromStorage = true
            }
        }
    }

    @Override
    Boolean allowTests() { FileUtils.ExistsFile(repConfigFileName) }

    @Override
    Class<Getl> useInitClass() { TestInit }
    @Override
    Class<Getl> useGetlClass() { TestRunner }
}