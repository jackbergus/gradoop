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

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.Properties;

/**
 * Converts the vertex into a GraphHead
 */
@FunctionAnnotation.ForwardedFieldsFirst("* -> id")
@FunctionAnnotation.ForwardedFieldsSecond("label -> label; properties -> properties")
public class VertexToGraphHead
  implements CoGroupFunction<GradoopId, Vertex, GraphHead> {

  /**
   * Reusable element
   */
  private final GraphHead reusable;

  /**
   * Default constructor
   */
  public VertexToGraphHead() {
    reusable = new GraphHead();
  }

  @Override
  public void coGroup(Iterable<GradoopId> iterable, Iterable<Vertex> iterable1,
    Collector<GraphHead> collector) throws Exception {
    for (GradoopId first : iterable) {
      reusable.setId(first);
      for (Vertex v : iterable1) {
        reusable.setLabel(v.getLabel());
        Properties props = v.getProperties();
        if (props != null) {
          reusable.setProperties(props);
        }
      }
      collector.collect(reusable);
      break;
    }
  }
}
