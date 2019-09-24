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

package getl.config

import getl.proc.Job
import getl.utils.Config
import getl.utils.Logs
import getl.utils.MapUtils

/**
 * Migrate configuration file utilite
 * @author Alexsey Konstantinov
 *
 */
class ConfigUtils extends Job {
    static void main(def args) {
        new ConfigUtils().run(args)
    }

    @Override
    void init() {
        Config.evalVars = false
    }

    @Override
    void process() {
        if (jobArgs.isEmpty()) {
            println 'syntax: getl.config.ConfigUtils config.<parameter1>=<value> config.<parameter2>=<value> dest.<parameter2>=<value>'
            println "config.* - source config manager parameters"
            println "dest.* - destination config manager parameters"
        }

        if (jobArgs.config == null) {
            println 'Required parameters "config.*" for source manager'
            return
        }

        if (jobArgs.dest == null) {
            println 'Required parameters "dest.*" for destination manager'
            return
        }

        if ((jobArgs.dest as Map).manager == null) {
            println 'Required parameters "dest.manager" for destination manager'
            return
        }

        if (Config.IsEmpty()) {
            println 'Found empty content of source config'
            return
        }

        Logs.Info("config: load ${Config.content.size()} parameters for source")

        def destMan = Class.forName((jobArgs.dest as Map).manager as String).newInstance() as ConfigManager
        Config.params.clear()
        Config.configClassManager = destMan
        def destParams = [config: jobArgs.dest]
        destMan.init(destParams)
        Config.SaveConfig()

        Config.ClearConfig()
        Config.LoadConfig()

        Logs.Info("config: save ${Config.content.size()} parameters to destination")
//        println MapUtils.ToJson(Config.content)
    }
}
