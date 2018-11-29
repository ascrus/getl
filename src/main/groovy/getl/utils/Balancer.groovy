/*
 GETL - based package in Groovy, which automates the work of loading and transforming data. His name is an acronym for "Groovy ETL".

 GETL is a set of libraries of pre-built classes and objects that can be used to solve problems unpacking,
 transform and load data into programs written in Groovy, or Java, as well as from any software that supports
 the work with Java classes.
 
 Copyright (C) 2013-2015  Alexsey Konstantonov (ASCRUS)

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
	public String getConfig () { config }
	public void setConfig (String value) {
		config = value
		if (config != null) {
			if (Config.ContainsSection("balancers.${this.config}")) {
				doInitConfig()
			}
			else {
				Config.RegisterOnInit(doInitConfig)
			}
		}
	}
	
	/**
	 * Public parameters
	 */
	private final Map params = [:]
	public Map getParams () { params }
	public void setParams(Map value) {
		params.clear()
		params.putAll(value)
	}

	/**
	 * Init configuration
	 */
	protected void onLoadConfig (Map configSection) {
		MapUtils.MergeMap(params, configSection)
		if (params."servers" == null) params."servers" = []
	}
	
	/**
	 * Call init configuraion
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
	public List<Map> getServers () { params."servers" }
	
	@Synchronized
	public void setServers (List<Map> value) {
		servers.clear()
		servers.addAll(value)
	}
	
	/**
	 * 
	 * @return
	 */
	public int getCheckTimeErrorServers () { params."checkTimeErrorServers"?:600 }
	
	@Synchronized
	public void setCheckTimeErrorServers (int value) { params."checkTimeErrorServers" = value }
	
	/**
	 * Get server parameter for connect
	 * @return
	 */
	@Synchronized
	public Map wantConnect () {
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
	public boolean didDisconnect (Map server) {
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
	public boolean errorDisconnect (Map server) {
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
