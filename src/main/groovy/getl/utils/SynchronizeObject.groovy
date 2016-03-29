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

class SynchronizeObject {
	private long count = 0
	
	@groovy.transform.Synchronized
	public long getCount () { count }
	
	@groovy.transform.Synchronized
	public setCount (long value) { count = value }
	
	@groovy.transform.Synchronized
	public clear  () { count = 0 }
	
	@groovy.transform.Synchronized
	public long nextCount () { 
		count++
		
		count
	}
	
	@groovy.transform.Synchronized
	public long prevCount () {
		count--
		
		count
	}
	
	@groovy.transform.Synchronized
	public void addCount (long value) {
		count += value
	}
	
	private String text
	
	@groovy.transform.Synchronized
	public String getText () { text }
	
	@groovy.transform.Synchronized
	public setText (String value) { text = value }
	
	private final List list = []
	
	@groovy.transform.Synchronized
	public def getList(int index) {
		list[index]
	}
	
	@groovy.transform.Synchronized
	public void addToList (int index, def value) {
		list.add(index, value)
	}
	
	@groovy.transform.Synchronized
	public Boolean addToList (def value) {
		list.add(value)
	}
	
	@groovy.transform.Synchronized
	public Boolean addAllToList (List list) {
		list.addAll(list)
	}
	
	@groovy.transform.Synchronized
	public Boolean addAllToList (int index, List list) {
		list.addAll(index, list)
	}
	
	@groovy.transform.Synchronized
	public void clearList () {
		list.clear()
	}
	
	@groovy.transform.Synchronized
	public Boolean isEmptyList () {
		list.isEmpty()
	}
	
	@groovy.transform.Synchronized
	public List getList () { list }
}
