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

package getl.utils

import groovy.transform.Synchronized

/**
 * Synchronized object
 * @author Alexsey Konstantinov
 */
class SynchronizeObject {
	private long count = 0

	/** Current counter value */
	@Synchronized
	long getCount () { count }

	/** Set new counter value */
	@Synchronized
	setCount (long value) { count = value }

	/** Clear counter value */
	@Synchronized
	clear  () { count = 0 }

	/** Increase counter value */
	@Synchronized
	long nextCount () {
		count++
		
		count
	}

	/** Decrease counter value */
	@Synchronized
	long prevCount () {
		count--
		
		count
	}

	/** Add number to counter value */
	@Synchronized
	void addCount (long value) {
		count += value
	}

	/** Text value */
	private String text

	/** Text value */
	@Synchronized
	String getText () { text }

	/** Text value */
	@Synchronized
	setText (String value) { text = value }

	/** Array list */
	private final List list = [] as ArrayList

	/** Array list node by index */
	@Synchronized
	def getList(int index) {
		list[index]
	}

	/** Add node to array list by index */
	@Synchronized
	void addToList (int index, def value) {
		list.add(index, value)
	}

	/** Append node to array list */
	@Synchronized
	Boolean addToList (def value) {
		list.add(value)
	}

	/** Append list to array list */
	@Synchronized
	static Boolean addAllToList (List list) {
		return list.addAll(list)
	}

	/** Append list to array list by index */
	@Synchronized
	static Boolean addAllToList (int index, List list) {
		return list.addAll(index, list)
	}

	/** Clear array list */
	@Synchronized
	void clearList () {
		list.clear()
	}

	/** Check empty array list */
	@Synchronized
	Boolean isEmptyList () {
		list.isEmpty()
	}

	/** Array list */
	@Synchronized
	List getList () { list }

	/** Index the item list by value */
	@Synchronized
	def indexOfListItem(def item) { list.indexOf(item) }

	/** Find item list */
	@Synchronized
	def findListItem(Closure cl) { list.find(cl) }
}