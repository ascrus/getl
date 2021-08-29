package getl.models

import getl.lang.Getl
import getl.lang.WorkflowStepTestScript
import getl.test.GetlDslTest
import org.junit.Test

class WorkflowTest extends GetlDslTest {
    @Test
    void testRun() {
        Getl.Dsl {
            models.workflow('test:workflow', true) {
                start('Start 1') {
                    countThreads = 2

                    exec('root1') {
                        className = WorkflowStepTestScript.name
                        vars = [stepName: stepName, stepNum: 1]
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
                            vars = [stepName: stepName, stepNum: 101]
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
                                vars = [stepName: stepName, stepNum: 201]
                            }
                        }
                    }
                }
                results.each { name, result -> println "$name: $result" }

                assertEquals(5, execute())
                assertEquals(2, execute())

                configContent.countProcessed = 0
                cleanResults()

                script('child1') {
                    vars.stepName = null
                }
                shouldFail { execute() }
            }
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