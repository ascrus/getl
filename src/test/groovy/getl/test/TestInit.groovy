package getl.test

import getl.utils.Config
import groovy.transform.InheritConstructors

@InheritConstructors
class TestInit extends TestRunner {
    @Override
    Object run() {
        Dsl {
            configContent.global = [inittest: true]
        }
    }
}