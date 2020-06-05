/*
 GETL - based package in Groovy, which automates the work of loading and transforming data. His name is an acronym for "Groovy ETL".

 GETL is a set of libraries of pre-built classes and objects that can be used to solve problems unpacking,
 transform and load data into programs written in Groovy, or Java, as well as from any software that supports
 the work with Java classes.

 Copyright (C) EasyData Company LTD

 This program is free software: you can redistribute it and/or modify
 it under the terms of the GNU Lesser General Public License as published by
 the Free Software Foundation, either version 3 of the License, or
 (at your option) any later version.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.

 You should have received a copy of the GNU General Public License and
 GNU Lesser General Public License along with this program.
 If not, see <http://www.gnu.org/licenses/>.
*/

package getl.test

import getl.config.ConfigManager
import getl.config.ConfigSlurper
import getl.lang.Getl

import groovy.transform.InheritConstructors
import org.junit.AfterClass
import org.junit.Before
import org.junit.BeforeClass

/**
 * Dsl language functional testing base class
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class GetlDslTest extends GetlTest {
    @Override
    protected Class<ConfigManager> useConfigManager() { ConfigSlurper }

    /** Use this initialization class at application startup if it is not explicitly specified */
    Class<Getl> useInitClass() { null }
    /** Run initialization only once */
    Boolean onceRunInitClass() { false }
    /** Used class for Getl */
    Class<Getl> useGetlClass() { Getl }

    @BeforeClass
    static void InitDslTestClass() {
        Getl.CleanGetl()
    }

    @AfterClass
    static void DoneDslTestClass() {
        Getl.CleanGetl()
    }

    /** Status init script */
    private boolean initWasRun = false

    /** Clean Getl on every test */
    protected boolean cleanGetlBeforeTest() { true }

    @Before
    void beforeDslTest() {
        if (cleanGetlBeforeTest()) {
            Getl.CleanGetl(false)
            initWasRun = false
        }

        if (!Getl.GetlInstanceCreated()) {
            def eng = useGetlClass().newInstance()
            Getl.GetlSetInstance(eng)
        }

        Getl.Dsl(this) {
            if (configuration().environment != 'dev')
                configuration().environment = 'dev'
        }

        def initClass = useInitClass()
        if (initClass != null && (!this.onceRunInitClass() || !initWasRun)) {
            Getl.Dsl(this) {
                callScript initClass
            }
            initWasRun = true
        }
    }
}