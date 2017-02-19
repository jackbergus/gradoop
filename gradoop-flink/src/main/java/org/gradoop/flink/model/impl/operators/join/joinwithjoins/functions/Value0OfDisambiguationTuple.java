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

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.functions.KeySelector;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.operators.join.joinwithjoins.tuples.DisambiguationTupleWithVertexId;

/**
 * (f0,f1,f2) => Vertex
 * From Value0Of3
 */
@FunctionAnnotation.ForwardedFields("f0->*")
public class Value0OfDisambiguationTuple
  implements
  MapFunction<DisambiguationTupleWithVertexId, Vertex>, KeySelector<DisambiguationTupleWithVertexId, Vertex> {

  @Override
  public Vertex map(DisambiguationTupleWithVertexId triple) throws Exception {
    return triple.f0;
  }

  @Override
  public Vertex getKey(DisambiguationTupleWithVertexId triple) throws Exception {
    return triple.f0;
  }
}
