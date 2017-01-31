package org.gradoop.flink.model.impl.operators.join.edgesemantics;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.java.operators.join.JoinType;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.flink.model.api.functions.Function;
import org.gradoop.flink.model.impl.operators.join.JoinUtils;
import org.gradoop.flink.model.impl.operators.join.operators.Oplus;
import org.gradoop.flink.model.impl.operators.join.tuples.CombiningEdgeTuples;

/**
 * Created by Giacomo Bergami on 30/01/17.
 */
public class GeneralEdgeSemantics {
  public final JoinType edgeJoinType;
  public final FlatJoinFunction<CombiningEdgeTuples, CombiningEdgeTuples, Edge> joiner;

  public GeneralEdgeSemantics(JoinType edgeJoinType,
    FlatJoinFunction<CombiningEdgeTuples, CombiningEdgeTuples, Edge> joiner) {
    this.edgeJoinType = edgeJoinType;
    this.joiner = joiner;
  }

  public static GeneralEdgeSemantics fromEdgePredefinedSemantics(
    final Function<CombiningEdgeTuples,Function<CombiningEdgeTuples, Boolean>> thetaEdge,
    final PredefinedEdgeSemantics es,
    Function<String,Function<String, String>> edgeLabelConcatenation) {
    Oplus<Edge> combinateEdges = Oplus.generate(() -> {
      Edge v = new Edge();
      v.setId(GradoopId.get());
      return v;
    }, JoinUtils.generateConcatenator(edgeLabelConcatenation));
    return fromEdgePredefinedSemantics(thetaEdge,es,combinateEdges);
  }

  private static GeneralEdgeSemantics fromEdgePredefinedSemantics(
    final Function<CombiningEdgeTuples,Function<CombiningEdgeTuples, Boolean>> thetaEdge,
    final PredefinedEdgeSemantics es,
    final Oplus<Edge> combineEdges)
  {
    JoinType edgeJoinType = null;
    FlatJoinFunction<CombiningEdgeTuples, CombiningEdgeTuples, Edge> joiner = null;
    final Function<CombiningEdgeTuples,Function<CombiningEdgeTuples, Boolean>> finalThetaEdge =
      JoinUtils.extendBasic2(thetaEdge);
    switch (es) {
    case CONJUNCTIVE: {
      edgeJoinType = JoinType.INNER;
      joiner = new FlatJoinFunction<CombiningEdgeTuples, CombiningEdgeTuples, Edge>() {
        @Override
        public void join(CombiningEdgeTuples first, CombiningEdgeTuples second,
          Collector<Edge> out) throws Exception {
            if (first.f2.getId().equals(second.f2.getId())) {
              if (finalThetaEdge.apply(first).apply(second)) {
                Edge prepared = combineEdges.apply(first.f1).apply(second.f1);
                prepared.setSourceId(first.f0.getId());
                prepared.setTargetId(first.f2.getId());
                out.collect(prepared);
              }
            }
        }
      };
    }
      break;
    case DISJUNCTIVE: {
      edgeJoinType = JoinType.FULL_OUTER;
      joiner = new FlatJoinFunction<CombiningEdgeTuples, CombiningEdgeTuples, Edge>() {
        @Override
        public void join(CombiningEdgeTuples first, CombiningEdgeTuples second,
          Collector<Edge> out) throws Exception {
          if (first!=null && second!=null) {
            if (first.f2.getId().equals(second.f2.getId())) {
              if (finalThetaEdge.apply(first).apply(second)) {
                Edge prepared = combineEdges.apply(first.f1).apply(second.f1);
                prepared.setSourceId(first.f0.getId());
                prepared.setTargetId(first.f2.getId());
                out.collect(prepared);
              }
            }
          } else if (first!=null) {
            Edge e = new Edge();
            e.setSourceId(first.f0.getId());
            e.setTargetId(first.f2.getId());
            e.setProperties(first.f1.getProperties());
            e.setLabel(first.f1.getLabel());
            e.setId(GradoopId.get());
            out.collect(e);
          } else {
            Edge e = new Edge();
            e.setSourceId(second.f0.getId());
            e.setTargetId(second.f2.getId());
            e.setProperties(second.f1.getProperties());
            e.setLabel(second.f1.getLabel());
            e.setId(GradoopId.get());
            out.collect(e);
          }
        }
      };
    }
      break;
    }
    return new GeneralEdgeSemantics(edgeJoinType,joiner);
  }

}
