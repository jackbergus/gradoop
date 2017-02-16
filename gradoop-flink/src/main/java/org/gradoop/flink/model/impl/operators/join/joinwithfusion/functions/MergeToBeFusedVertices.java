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

package org.gradoop.flink.model.impl.operators.join.joinwithfusion.functions;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;

/**
 * This function collects the vertices that has to be fused into one
 * since they belong to the pattern having the same graphid
 * Created by vasistas on 15/02/17.
 */
public class MergeToBeFusedVertices implements
  CoGroupFunction<Tuple2<GradoopId, Vertex>, GraphHead, Tuple2<GradoopId, Vertex>> {

  /**
   * Reusable field
   */
  private final Vertex v;

  /**
   * Default constructor
   * @param gid   Graph id belonging to the final result
   */
  public MergeToBeFusedVertices(GradoopId gid) {
    this.v = new Vertex();
    this.v.addGraphId(gid);
  }

  @Override
  public void coGroup(Iterable<Tuple2<GradoopId, Vertex>> first, Iterable<GraphHead> second,
    Collector<Tuple2<GradoopId, Vertex>> out) throws Exception {
    for (GraphHead gh : second) {
      v.setProperties(gh.getProperties());
      v.setLabel(gh.getLabel());
      out.collect(new Tuple2<>(gh.getId(), v));
      break;
    }
  }
}
