package org.gradoop.flink.model.impl.operators.join;

import com.sun.istack.Nullable;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.join.JoinType;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TypeInfoParser;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.api.functions.Function;
import org.gradoop.flink.model.impl.operators.join.edgesemantics.GeneralSemantics;
import org.gradoop.flink.model.impl.operators.join.operators.PreFilter;

/**
 * Created by vasistas on 31/01/17.
 */
public class GraphEdgeJoin extends GeneralJoinPlan<GradoopId> {

  private static final Function<Boolean, Function<DataSet<Edge>, PreFilter<Vertex, GradoopId>>>
    relationalJoinPrefilter =
    (Boolean isLeft) -> (DataSet<Edge> e) -> (PreFilter<Vertex, GradoopId>) vertexDataSet ->
      vertexDataSet
      .join(e).where((Vertex v) -> v.getId())
      .equalTo((Edge e1) -> isLeft ? e1.getSourceId() : e1.getTargetId())
      .with(new JoinFunction<Vertex, Edge, Tuple2<Vertex, GradoopId>>() {
        @Override
        public Tuple2<Vertex, GradoopId> join(Vertex first, Edge second) throws Exception {
          return new Tuple2<>(first, second.getId());
        }
      }).returns(TypeInfoParser.parse(
        Tuple2.class.getName() + "<" + Vertex.class.getCanonicalName() + "," +
          GradoopId.class.getCanonicalName() + ">"));
  private final DataSet<Edge> relations;


  public GraphEdgeJoin(JoinType vertexJoinType, GeneralSemantics edgeSemanticsImplementation,
    DataSet<Edge> relations, @Nullable Function<Vertex, Function<Vertex, Boolean>> thetaVertex,
    @Nullable Function<GraphHead, Function<GraphHead, Boolean>> thetaGraph,
    @Nullable Function<String, Function<String, String>> vertexLabelConcatenation,
    @Nullable Function<String, Function<String, String>> graphLabelConcatenation) {
    super(vertexJoinType, edgeSemanticsImplementation, relationalJoinPrefilter.apply(true).apply(relations),
      relationalJoinPrefilter.apply(false).apply(relations), null, null, thetaVertex, thetaGraph,
      vertexLabelConcatenation, graphLabelConcatenation);
    this.relations = relations;
  }

}
