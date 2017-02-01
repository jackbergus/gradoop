package org.gradoop.flink.model.impl.operators.join.blocks;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.flink.model.api.functions.Function;
import org.gradoop.flink.model.impl.operators.join.functions.Oplus;

import java.io.Serializable;

/**
 *
 * Defines the way to define a new graph head from two other ones
 *
 * Created by Giacomo Bergami on 01/02/17.
 */
public class CoJoinGraphHeads  implements CoGroupFunction<GraphHead, GraphHead, GraphHead>,
  Serializable {
  private final Function<Tuple2<GraphHead, GraphHead>, Boolean> thetaGraph;
  private final Oplus<GraphHead> combineHeads;
  private final GradoopId gid;

  public CoJoinGraphHeads(Function<Tuple2<GraphHead, GraphHead>, Boolean> thetaGraph,
    Oplus<GraphHead> combineHeads, GradoopId gid) {
    this.thetaGraph = thetaGraph;
    this.combineHeads = combineHeads;
    this.gid = gid;
  }

  @Override
  public void coGroup(Iterable<GraphHead> first, Iterable<GraphHead> second,
    Collector<GraphHead> out) throws Exception {
    for (GraphHead left : first) {
      for (GraphHead right : second) {
        if (thetaGraph.apply(new Tuple2<>(left,right))) {
          GraphHead gh1 = combineHeads.apply(new Tuple2<GraphHead,GraphHead>(left,right));
          gh1.setId(gid);
          out.collect(gh1);
        }
      }
    }
  }
}
