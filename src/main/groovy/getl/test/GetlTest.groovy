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

import getl.config.ConfigFiles
import getl.config.ConfigManager
import getl.utils.Config
import getl.utils.FileUtils
import getl.utils.Logs
import groovy.test.GroovyAssert
import groovy.time.Duration
import org.junit.AfterClass
import org.junit.Before
import org.junit.BeforeClass
/**
 * Getl functional testing base class
 * @author Alexsey Konstantinov
 *
 */
//@InheritConstructors
//@RunWith(JUnit4.class)
abstract class GetlTest extends GroovyAssert {
    /** Configuration manager class to use */
    protected Class<ConfigManager> useConfigManager() { ConfigFiles }

    /** Install the required configuration manager */
    protected InstallConfigManager() {
        if (!useConfigManager().isInstance(Config.configClassManager))
            Config.configClassManager = useConfigManager().newInstance() as ConfigManager
    }

    @BeforeClass
    static void InitTestClass() {
        Config.ReInit()
        Logs.Init()
        FileUtils.ListResourcePath.clear()
    }

    @AfterClass
    static void DoneTestClass() {
        Logs.Done()
        FileUtils.ListResourcePath.clear()
    }

    @Before
    void beforeTest() {
        InstallConfigManager()
        org.junit.Assume.assumeTrue(allowTests())
    }

    /** Allow to run tests */
    boolean allowTests() { true }

    /**
     * Asserts that two objects are equal. If they are not, an AssertionError without a message is thrown. If expected and actual are null, they are considered equal.
     * @param message message text
     * @param expected expected value
     * @param actual the value to check against expected
     */
    static void assertEquals(String message, Duration expected, Duration actual) {
        assertEquals(message, expected.toString(), actual.toString())
    }

    /**
     * Asserts that two objects are equal. If they are not, an AssertionError without a message is thrown. If expected and actual are null, they are considered equal.
     * @param expected expected value
     * @param actual the value to check against expected
     */
    static void assertEquals(Duration expected, Duration actual) {
        assertEquals(null as String, expected, actual)
    }
}