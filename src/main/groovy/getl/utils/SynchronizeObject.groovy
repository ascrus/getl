package getl.utils

/**
 GETL - based package in Groovy, which automates the work of loading and transforming data. His name is an acronym for «Groovy ETL».

 GETL is a set of libraries of pre-built classes and objects that can be used to solve problems unpacking,
 transform and load data into programs written in Groovy, or Java, as well as from any software that supports
 the work with Java classes.
 
 Copyright (C) 2013  Alexsey Konstantonov (ASCRUS)

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

import groovy.transform.Synchronized

class SynchronizeObject {
	private long count = 0
	
	@Synchronized
	public long getCount () { count }
	
	@Synchronized
	public setCount (long value) { count = value }
	
	@Synchronized
	public clear  () { count = 0 }
	
	@Synchronized
	public long nextCount () { 
		count++
		
		count
	}
	
	@Synchronized
	public long prevCount () {
		count--
		
		count
	}
	
	@Synchronized
	public void addCount (long value) {
		count += value
	}
	
	private String text
	
	@Synchronized
	public String getText () { text }
	
	@Synchronized
	public setText (String value) { text = value }
	
	private final List list = []
	
	@Synchronized
	public def getList(int index) {
		list[index]
	}
	
	@Synchronized
	public void addToList (int index, def value) {
		list.add(index, value)
	}
	
	@Synchronized
	public Boolean addToList (def value) {
		list.add(value)
	}
	
	@Synchronized
	public Boolean addAllToList (List list) {
		list.addAll(list)
	}
	
	@Synchronized
	public Boolean addAllToList (int index, List list) {
		list.addAll(index, list)
	}
	
	@Synchronized
	public void clearList () {
		list.clear()
	}
	
	@Synchronized
	public Boolean isEmptyList () {
		list.isEmpty()
	}
	
	@Synchronized
	public List getList () { list }
}
