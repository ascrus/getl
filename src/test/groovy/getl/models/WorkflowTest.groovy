package getl.models

import getl.lang.Getl
import getl.test.GetlDslTest
import getl.utils.ListUtils
import org.junit.Test

class WorkflowTest extends GetlDslTest {
    @Test
    void testReadFields() {
        def res = Workflows.ReadClassFields(WorkflowStepTestScript)
        assertEquals(4, res.size())
        assertEquals([name: 'stepName', type: 'java.lang.String'], res[0])
        assertEquals([name: 'stepNum', type: 'java.lang.Integer'], res[1])
        assertEquals([name: 'map', type: 'java.util.Map'], res[2])
        assertEquals([name: 'list', type: 'java.util.List'], res[3])
    }

    @Test
    void testExecute() {
        Getl.Dsl {getl ->
            def mod1 = models.workflow('test:workflow', true) {
                start('Start 1') {
                    countThreads = 2

                    exec('root1') {
                        className = WorkflowStepTestScript.name
                        vars = [stepName: stepName, stepNum: 1, map: [a: '1', b: '2'], list: ['a', 'b', 'c']]
                    }
                    exec('root2') {
                        className = WorkflowStepTestScript.name
                        vars = [stepName: stepName, stepNum: 2]
                    }

                    onError {
                        exec {
                            className = WorkflowStepTestScript.name
                            vars = [stepName: stepName, stepNum: -1]
                        }
                    }

                    later {
                        condition = '''configContent.countProcessed == 2 && result('root1').processed == 1 && result('root2').processed == 2'''

                        exec('later1') {
                            className = WorkflowStepTestScript.name
                            vars = [stepName: stepName, stepNum: 101, map: "[a: '1', b: '2']", list: "['a', 'b', 'c']"]
                        }
                        exec('later2') {
                            className = WorkflowStepTestScript.name
                            vars = [stepName: stepName, stepNum: 102]
                        }

                        onError {
                            exec {
                                className = WorkflowStepTestScript.name
                                vars = [stepName: stepName, stepNum: -101]
                            }
                        }

                        later {
                            condition = '''configContent.countProcessed == 4 && result('later1').processed == 101 && result('later2').processed == 102'''
                            exec('child1') {
                                className = WorkflowStepTestScript.name
                                vars = [stepName: stepName, stepNum: 201, map: "a: '1', b: '2'", list: "'a', 'b', 'c'"]
                            }
                        }
                    }
                }
                assertEquals(5, execute())
                //results.each { name, result -> println "$name: $result" }
                assertEquals(5, results.size())
                assertEquals(1, result('root1').processed)
                assertEquals([a: '1', b: '2'], result('root1').map)
                assertEquals(['a', 'b', 'c'], result('root1').list)
                assertEquals(2, result('root2').processed)
                assertNull(result('root2').map)
                assertNull(result('root2').list)
                assertEquals(101, result('later1').processed)
                assertEquals([a: '1', b: '2'], result('later1').map)
                assertEquals(['a', 'b', 'c'], result('later1').list)
                assertEquals(102, result('later2').processed)
                assertNull(result('later2').map)
                assertNull(result('later2').list)
                assertEquals(201, result('child1').processed)
                assertEquals([a: '1', b: '2'], result('child1').map)
                assertEquals(['a', 'b', 'c'], result('child1').list)

                script('root1').vars.remove('list')
                step('Start 1') {
                    objectVars.list = ['aa', 'bb', 'cc']
                }
                assertEquals(2, execute([map: [a: '100', b: '200']]))
                assertEquals([a: '100', b: '200'], result('root1').map)
                assertEquals(['aa', 'bb', 'cc'], result('root1').list)

                configContent.countProcessed = 0

                script('child1') {
                    vars.stepName = null
                }
                shouldFail { execute() }
            }
            def mod2 = new Workflows(getl, true, mod1.params)
            assertEquals(mod1.usedSteps.size(), mod2.usedSteps.size())
        }
    }

    @Test
    void testBadClass() {
        Getl.GetlInstance().models.workflow('test:workflow_bad', true) {
            start {
                exec {
                    className = WorkflowStepTestScript.name + '_not_found'
                }
            }

            shouldFail {
                execute()
            }
        }
    }
}