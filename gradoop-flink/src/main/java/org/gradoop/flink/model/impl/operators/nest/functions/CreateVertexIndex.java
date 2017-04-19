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

package org.gradoop.flink.model.impl.operators.nest.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * Uses the tuple as a basis for creating the graphMapToVertex
 */
public class CreateVertexIndex
  implements MapFunction<Tuple3<GradoopId, GradoopId, GradoopId>, Tuple2<GradoopId, GradoopId>>  {

  /**
   * Reusable element
   */
  private Tuple2<GradoopId, GradoopId> reusable;

  /**
   * Default constructor
   * @param nestedGraphId The id to be associated to the graph
   */
  public CreateVertexIndex(GradoopId nestedGraphId) {
    reusable = new Tuple2<>();
    reusable.f0 = nestedGraphId;
  }

  @Override
  public Tuple2<GradoopId, GradoopId> map(Tuple3<GradoopId, GradoopId, GradoopId> value) throws
    Exception {
    if (value.f1.equals(GradoopId.NULL_VALUE)) {
      reusable.f1 = value.f2;
    } else {
      reusable.f1 = value.f1;
    }
    return reusable;
  }
}