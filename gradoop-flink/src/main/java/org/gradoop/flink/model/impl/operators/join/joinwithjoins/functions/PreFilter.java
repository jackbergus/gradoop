/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.flink.model.impl.operators.join.joinwithjoins.functions;

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.api.entities.EPGMElement;
import org.gradoop.flink.model.impl.operators.join.joinwithjoins.tuples.DisambiguationTupleWithVertexId;

import java.util.function.Function;

/**
 * Defining a non-serializable function allowing to pre-evaluate the vertices if required.
 * @param <K> Element that is filtered prior to the actual join phase
 *
 * Created by Giacomo Bergami on 30/01/17.
 */
public interface PreFilter<K extends EPGMElement> extends Function<DataSet<K>, DataSet<DisambiguationTupleWithVertexId>> {

}
