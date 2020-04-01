package getl.test

import getl.lang.Getl

class TestRunner extends Getl {
    @Override
    protected Class useInitClass() {
        TestInit
    }
}