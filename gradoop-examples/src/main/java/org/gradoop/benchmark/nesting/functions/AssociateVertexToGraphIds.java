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

/**
 * Contains the programs to execute the benchmarks.
 */
package org.gradoop.benchmark.nesting.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdList;
import org.gradoop.common.model.impl.pojo.Vertex;

/**
 * Created by vasistas on 20/04/17.
 */
@FunctionAnnotation.ForwardedFieldsFirst("id -> id; label -> label; properties -> properties")
@FunctionAnnotation.ForwardedFieldsSecond("f1 -> graphIds")
public class AssociateVertexToGraphIds implements
  JoinFunction<Vertex, Tuple2<GradoopId, GradoopIdList>, Vertex> {
  @Override
  public Vertex join(Vertex first, Tuple2<GradoopId, GradoopIdList> second) throws Exception {
    first.setGraphIds(second.f1);
    return first;
  }
}
