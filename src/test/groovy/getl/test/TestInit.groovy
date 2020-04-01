package getl.test

import getl.utils.Config

class TestInit extends TestRunner {
    @Override
    Object run() {
        Dsl {
            Config.content.global = [inittest: true]
        }
    }
}