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

                    script WorkflowStepTestScript, [stepName: stepName, stepNum: 1]
                    script WorkflowStepTestScript, [stepName: stepName, stepNum: 2]

                    onError {
                        script WorkflowStepTestScript, [stepName: stepName, stepNum: -1]
                    }

                    later {
                        condition = '(configContent.countProcessed == 2)'

                        script WorkflowStepTestScript, [stepName: stepName, stepNum: 101]
                        script WorkflowStepTestScript, [stepName: stepName, stepNum: 102]

                        onError {
                            script WorkflowStepTestScript, [stepName: stepName, stepNum: -101]
                        }

                        later {
                            condition = '(configContent.countProcessed == 4)'
                            script WorkflowStepTestScript, [stepName: stepName, stepNum: 201]
                        }
                    }
                }

                assertEquals(5, execute())

                assertEquals(2, execute())

                configContent.countProcessed = 0
                step('Start 1') {
                    step('EXECUTE 1') {
                        scripts[0].params.stepName = null
                    }
                }

                shouldFail { execute() }
            }
        }
    }
}
