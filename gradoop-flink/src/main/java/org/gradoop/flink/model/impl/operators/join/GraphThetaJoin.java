package org.gradoop.flink.model.impl.operators.join;

import com.sun.istack.Nullable;
import org.apache.flink.api.java.operators.join.JoinType;
import org.gradoop.flink.model.api.functions.Function;
import org.gradoop.flink.model.impl.operators.join.edgesemantics.PredefinedEdgeSemantics;
import org.gradoop.flink.model.impl.operators.join.edgesemantics.GeneralEdgeSemantics;
import org.gradoop.flink.model.impl.operators.join.tuples.CombiningEdgeTuples;

/**
 * Created by Giacomo Bergami on 31/01/17.
 */
public class GraphThetaJoin extends GeneralJoinPlan {

  public GraphThetaJoin(JoinType vertexJoinType, GeneralEdgeSemantics edgeSemanticsImplementation,
    @Nullable Function leftHash, @Nullable Function rightHash, @Nullable Function thetaVertex,
    @Nullable Function thetaGraph, @Nullable Function vertexLabelConcatenation,
    @Nullable Function graphLabelConcatenation) {
    super(vertexJoinType, edgeSemanticsImplementation, null, null, leftHash,
      rightHash, thetaVertex, thetaGraph, vertexLabelConcatenation, graphLabelConcatenation);
  }

  public GraphThetaJoin(JoinType vertexJoinType,
    final Function<CombiningEdgeTuples,Function<CombiningEdgeTuples, Boolean>> thetaEdge,
    final PredefinedEdgeSemantics es,
    Function<String,Function<String, String>> edgeLabelConcatenation,
    @Nullable Function leftHash, @Nullable Function rightHash, @Nullable Function thetaVertex,
    @Nullable Function thetaGraph, @Nullable Function vertexLabelConcatenation,
    @Nullable Function graphLabelConcatenation) {
    super(vertexJoinType, GeneralEdgeSemantics
        .fromEdgePredefinedSemantics(thetaEdge,es,edgeLabelConcatenation), null,
      null, leftHash,
      rightHash, thetaVertex, thetaGraph, vertexLabelConcatenation, graphLabelConcatenation);
  }

}
