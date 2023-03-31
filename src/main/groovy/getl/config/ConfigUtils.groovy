package getl.config

import getl.proc.Job
import getl.utils.Config
import groovy.transform.InheritConstructors

/**
 * Migrate configuration file utilities
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
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

        this.getLogger().info("config: load ${Config.content.size()} parameters for source")

        def destMan = Class.forName((jobArgs.dest as Map).manager as String).getConstructor().newInstance() as ConfigManager
        Config.params.clear()
        Config.configClassManager = destMan
        def destParams = [config: jobArgs.dest]
        destMan.init(destParams)
        Config.SaveConfig()

        Config.ClearConfig()
        Config.LoadConfig()

        this.getLogger().info("config: save ${Config.content.size()} parameters to destination")
//        println MapUtils.ToJson(Config.content)
    }
}
