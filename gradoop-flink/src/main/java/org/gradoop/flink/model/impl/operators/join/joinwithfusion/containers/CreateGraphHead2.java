package org.gradoop.flink.model.impl.operators.join.joinwithfusion.containers;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.flink.model.impl.operators.join.common.tuples.DisambiguationTupleWithVertexId;

/**
 * Created by vasistas on 16/02/17.
 */
public class CreateGraphHead2 implements MapFunction<DisambiguationTupleWithVertexId,
  GraphHead> {

  private final GraphHead gh;

  public CreateGraphHead2() {
    gh = new GraphHead();
  }

  @Override
  public GraphHead map(DisambiguationTupleWithVertexId value) throws Exception {
    gh.setId(value.f2);
    gh.setProperties(value.f0.getProperties());
    gh.setLabel(value.f0.getLabel());
    return gh;
  }
}
