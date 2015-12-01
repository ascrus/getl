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

package getl.cache

import groovy.transform.InheritConstructors

import getl.data.*
import getl.jdbc.*
import getl.utils.*
import getl.proc.*

/**
 * Cache dataset class
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class CacheDataset extends TableDataset {
	public CacheDataset () {
		super()
		manualSchema = true
	}
	
	@Override
	public void setConnection(Connection value) {
		assert value == null || value instanceof CacheManager 
		CacheManager oldValue = connection
		super.setConnection(value)
		if (value != null && oldValue != value) {
			CacheManager cm = value
			cm.addCacheDataset(this)
		}
		else if (value == null && oldValue != null) {
			CacheManager cm = oldValue
			cm.removeCacheDataset(this)
		}
	}
	
	/**
	 * Source cache dataset
	 */
	public Dataset dataset
	
	/**
	 * Time of retrieve cache
	 */
	public int liveTime = 3600
	
	/**
	 * Event on reread data from source dataset
	 */
	public Closure onUpdateData
	
	protected long connectionid
	
	@Override
	public void eachRow (Map procParams, Closure code) {
		CacheManager cm = connection
		def rowObjects = cm.findObject(connectionid, dataset)
		assert rowObjects != null, "Can not find dataset \"${dataset.objectName}\" from register objects"
		
		// Limit live time
		Date readed = DateUtils.AddDate("ss", liveTime, rowObjects.readed)
		Date updated = rowObjects.updated
		// Current time
		Date now = DateUtils.Now()
		
		// Reread data from source with timeout live time and fix new time
		if (readed < now || updated > readed) {
			cm.startTran()
			try {
				new Flow().writeTo(dest: cm.objects, dest_operation: "UPDATE", dest_updateField: ["readed"]) { writer ->
					def row = [:]
					row.name = tableName
					row.readed = now
					writer(row)
				}
				new Flow().copy(source: dataset, dest: this, clear: true)
			}
			catch (Exception e) {
				cm.rollbackTran()
				throw e
			}
			cm.commitTran()
			if (onUpdateData != null) onUpdateData(this)
		}
		
		super.eachRow(procParams, code)
	}
	
	/**
	 * Last readed time source dataset
	 * @return
	 */
	public Date getCacheReaded () {
		def row
		CacheManager cm = connection
		cm.startTran()
		try {
			row = cm.findObject(this)
		}
		catch (Exception e) {
			cm.rollbackTran()
		}
		cm.commitTran()
		row?.readed
	}
	
	/**
	 * Last updated time source dataset
	 * @return
	 */
	public Date getCacheUpdated () {
		def row
		CacheManager cm = connection
		cm.startTran()
		try {
			row = cm.findObject(this)
		}
		catch (Exception e) {
			cm.rollbackTran()
		}
		cm.commitTran()
		row?.updated
	}
}
