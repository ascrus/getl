package getl.utils

import getl.exception.*
import groovy.transform.Synchronized

/**
 * Balancer connections
 * @author Alexsey Konstantinov
 *
 */
class Balancer  {
	Balancer () {
		super()
		params."servers" = []
	}
	
	/**
	 * Name in config from section "balancers"
	 */
	private String config

	String getConfig () { config }

	void setConfig (String value) {
		config = value
		if (config != null) {
			if (Config.ContainsSection("balancers.${this.config}")) {
				doInitConfig.call()
			}
			else {
				Config.RegisterOnInit(doInitConfig)
			}
		}
	}
	
	/** Balancer parameters */
	final Map params = [:]

	Map getParams () { params }

	void setParams(Map value) {
		params.clear()
		params.putAll(value)
	}

	/**
	 * Init configuration
	 */
	protected void onLoadConfig (Map configSection) {
		MapUtils.MergeMap(params as Map<String, Object>, configSection as Map<String, Object>)
		if (params."servers" == null) params."servers" = []
	}
	
	/**
	 * Call init configuration
	 */
	private final Closure doInitConfig = {
		if (config == null) return
		Map cp = Config.FindSection("balancers.${config}")
		if (cp.isEmpty()) throw new ExceptionGETL("Config section \"balancers.${config}\" not found")
		onLoadConfig(cp)
		Logs.Config("Load config \"balancers\".\"${config}\"")
	}
	
	/**
	 * List of servers
	 * <h1>Attributes of server:</h1>
	 * <ul>
	 * <li>host - server host and port
	 * <li>database - database name
	 * <li>count - count of connected server
	 * </ul>
	 * @return
	 */
	List<Map> getServers () { params."servers" as List<Map> }
	
	@Synchronized
	void setServers (List<Map> value) {
		servers.clear()
		servers.addAll(value)
	}
	
	/**
	 * 
	 * @return
	 */
	Integer getCheckTimeErrorServers () { (params.checkTimeErrorServers as Integer)?:600 }
	
	@Synchronized
	void setCheckTimeErrorServers (Integer value) { params."checkTimeErrorServers" = value }
	
	/**
	 * Get server parameter for connect
	 * @return
	 */
	@Synchronized
	Map wantConnect () {
		if (servers.isEmpty()) throw new ExceptionGETL("Required servers list")
		
		servers.each { server ->
			if (server."count" == null) server."count" = 0
			
			if (server."errorTime" != null) {
				if (DateUtils.AddDate("ss", checkTimeErrorServers, (Date)server."errorTime") < DateUtils.Now()) {
					server."errorTime" = null
				}
			}
		}
		
		Map res = servers[0]
		if (res."errorTime" != null) return null
		
		res."count"++
		sortPriority()

		res
	}
	
	/**
	 * Unregister connected session from server
	 * @param server
	 * @return
	 */
	@Synchronized
	Boolean didDisconnect (Map server) {
		if (servers.isEmpty()) throw new ExceptionGETL("Required servers list")
		def i = servers.indexOf(server)
		if (i != -1) {
			if (server."count" > 0) {
				server."count"--
			}
			else {
				server."count" = 0
			}
		}
		
		sortPriority()
		
		(i != -1)
	}
	
	/**
	 * Detected error from server
	 * @param server
	 * @return
	 */
	@Synchronized
	Boolean errorDisconnect (Map server) {
		if (servers.isEmpty()) throw new ExceptionGETL("Required servers list")
		def i = servers.indexOf(server)
		if (i != -1) {
			server."errorTime" = DateUtils.Now()
			server."count" = 0
			
		}
		sortPriority()

		(i != -1)
	}
	
	protected void sortPriority () {
		ListUtils.SortList(servers) { a, b -> ((a."errorTime" == null)?a."count":100000000) <=> ((b."errorTime" == null)?b."count":100000000) }
	}
}
