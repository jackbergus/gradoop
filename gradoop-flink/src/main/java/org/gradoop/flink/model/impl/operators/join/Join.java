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

package org.gradoop.flink.model.impl.operators.join;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.CombineFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdList;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.flink.model.api.operators.BinaryGraphToGraphOperator;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.operators.fusion.FusionUtils;
import org.gradoop.flink.util.GradoopFlinkConfig;

/**
 * Created by Giacomo Bergami on 27/01/17.
 */
public class Join implements BinaryGraphToGraphOperator{

  /**
   * Default configuration to be used if there are some problems with the input graphs
   */
  private static final GradoopFlinkConfig DEFAULT_CONF =
    GradoopFlinkConfig.createConfig(ExecutionEnvironment.getExecutionEnvironment());


  public static void outputGraph(LogicalGraph left, LogicalGraph right) {

    LogicalGraph toret = LogicalGraph
      .createEmptyGraph(left == null ? DEFAULT_CONF : right.getConfig());
    DataSet<GradoopId> idSet = FusionUtils.getGraphId(toret);

  }


  private final KeySelector<Vertex, Long> leftHash;
  private final KeySelector<Vertex, Long> rightHash;
  private final FilterFunction<Tuple2<Vertex,Vertex>> theta;
  private final MapFunction<Tuple2<String,String>,String> mapLabels;

  public Join(KeySelector<Vertex, Long> leftHash, KeySelector<Vertex, Long> rightHash,
    FilterFunction<Tuple2<Vertex, Vertex>> theta,
    MapFunction<Tuple2<String, String>, String> mapLabels) {
    this.leftHash = leftHash;
    this.rightHash = rightHash;
    this.theta = theta;
    this.mapLabels = mapLabels;
  }

  @Override
  public String getName() {
    return getClass().getCanonicalName();
  }

  @Override
  public LogicalGraph execute(LogicalGraph firstGraph, LogicalGraph secondGraph) {




    firstGraph.getVertices()
      .coGroup(secondGraph.getVertices())
      .where(leftHash)
      .equalTo(rightHash)
      .with(new CoGroupFunction<Vertex, Vertex, Vertex>() {
        @Override
        public void coGroup(Iterable<Vertex> first, Iterable<Vertex> second,
          Collector<Vertex> out) throws Exception {
          for (Vertex l : first) {
            for (Vertex r : second) {
              if (theta.filter(new Tuple2<>(l,r))) {
                Vertex v = new Vertex();
                v.setId(GradoopId.get());

                GradoopIdList ll = new GradoopIdList();
                ll.addAll(l.getGraphIds());
                ll.addAll(r.getGraphIds());
                v.setGraphIds(ll);

                Properties p = new Properties();
                for (String k : l.getProperties().getKeys()) {
                  p.set(k,l.getPropertyValue(k));
                }
                for (String k : r.getProperties().getKeys()) {
                  p.set(k,r.getPropertyValue(k));
                }
                v.setProperties(p);

                v.setLabel(mapLabels.map(new Tuple2<>(l.getLabel(),r.getLabel())));
                out.collect(v);
              }
            }
          }
        }
      });





    return null;
  }
}
