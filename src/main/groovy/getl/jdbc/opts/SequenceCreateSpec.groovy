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

package getl.jdbc.opts

import getl.lang.opts.BaseSpec
import groovy.transform.InheritConstructors

/**
 * Create options for creating sequence
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class SequenceCreateSpec extends BaseSpec {
    /** A positive or negative integer that specifies how much to increment or decrement the sequence on each call to NEXTVAL, by default set to 1 */
    BigInteger getIncrementBy() { params.incrementBy as BigInteger }
    /** A positive or negative integer that specifies how much to increment or decrement the sequence on each call to NEXTVAL, by default set to 1 */
    void setIncrementBy(BigInteger value) { params.incrementBy = value }

    /** Determines the minimum value a sequence can generate */
    BigInteger getMinValue() { params.minValue as BigInteger }
    /** Determines the minimum value a sequence can generate */
    void setMinValue(BigInteger value) { params.minValue = value }

    /* Determines the maximum value for the sequence */
    BigInteger getMaxValue() { params.maxValue as BigInteger }
    /* Determines the maximum value for the sequence */
    void setMaxValue(BigInteger value) { params.maxValue = value }

    /** Sets the sequence start value to integer */
    BigInteger getStartWith() { params.startWith as BigInteger }
    /** Sets the sequence start value to integer */
    void setStartWith(BigInteger value) { params.startWith = value }

    /** Specifies whether to cache unique sequence numbers for faster access */
    BigInteger getCacheNumbers() { params.cacheNumbers as BigInteger }
    /** Specifies whether to cache unique sequence numbers for faster access */
    void setCacheNumbers(BigInteger value) { params.cacheNumbers = value }

    /** Specifies whether the sequence can wrap when its minimum or maximum values are reached */
    Boolean getIsCycle() { params.isCycle as Boolean }
    /** Specifies whether the sequence can wrap when its minimum or maximum values are reached */
    void setIsCycle(Boolean value) { params.isCycle = value }
}
