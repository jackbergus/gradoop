package org.gradoop.flink.model.impl.operators.fusion.reduce;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.api.operators.GraphGraphGraphCollectionToGraph;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.functions.epgm.SourceId;
import org.gradoop.flink.model.impl.functions.epgm.TargetId;
import org.gradoop.flink.model.impl.functions.graphcontainment.GraphContainmentFilterBroadcast;
import org.gradoop.flink.model.impl.functions.graphcontainment.InGraphBroadcast;
import org.gradoop.flink.model.impl.functions.graphcontainment.NotInGraphBroadcast;
import org.gradoop.flink.model.impl.functions.tuple.Value0Of2;
import org.gradoop.flink.model.impl.functions.tuple.Value1Of2;
import org.gradoop.flink.model.impl.operators.combination.Combination;
import org.gradoop.flink.model.impl.operators.fusion.reduce.functions.CoGroupAssociateOldVerticesWithNewIds;
import org.gradoop.flink.model.impl.operators.fusion.reduce.functions.CoGroupGraphHeadToVertex;
import org.gradoop.flink.model.impl.operators.fusion.reduce.functions.FlatJoinSourceEdgeReference;
import org.gradoop.flink.model.impl.operators.fusion.reduce.functions.MapFunctionAddGraphElementToGraph2;
import org.gradoop.flink.model.impl.operators.fusion.reduce.functions.MapGraphHeadForNewGraph;
import org.gradoop.flink.model.impl.operators.fusion.reduce.functions.MapVertexToPairWithGraphId;
import org.gradoop.flink.model.impl.operators.fusion.reduce.functions.MapVerticesAsTuplesWithNullId;

/**
 * Given two graph operands and a graph collection of (solved)
 * patterns, summarize the union of the two operands by
 * replacing each of the pattern with a hypervertex.
 *
 * Please Note: this implementation induces hypervertices in order
 * to mimic the Reduce operation over the VertexFusion binary operator.
 * Please note that, by applying reduce over VertexFusion, you
 * won't logically obtain the same result. This should be considered
 * as a mere generalization.
 *
 * Created by Giacomo Bergami on 21/02/17.
 */
public class ReduceVertexFusion implements GraphGraphGraphCollectionToGraph {
  @Override
  public LogicalGraph execute(LogicalGraph left, LogicalGraph right,
    GraphCollection hypervertices) {
    
    LogicalGraph gU = new Combination().execute(left,right);
    // Missing in the theoric definition: creating a new header
    GradoopId newGraphid = GradoopId.get();
    DataSet<GraphHead> gh = gU.getGraphHead()
      .map(new MapGraphHeadForNewGraph(newGraphid));

    // PHASE 1: Induced Subgraphs
    // Associate each vertex to its graph id
    DataSet<Tuple2<Vertex,GradoopId>> vWithGid = hypervertices.getVertices()
      .filter(new InGraphBroadcast<>())
      .withBroadcastSet(gU.getGraphHead(), GraphContainmentFilterBroadcast.GRAPH_ID)
      .flatMap(new MapVertexToPairWithGraphId());

    // Associate each gid in hypervertices.H to the merged vertices
    DataSet<Tuple2<Vertex,GradoopId>> nuWithGid  = vWithGid
      .coGroup(hypervertices.getGraphHeads())
      .where(new Value1Of2<>()).equalTo(new Id<>())
      .with(new CoGroupGraphHeadToVertex());

    // PHASE 2: Recreating the vertices
    DataSet<Vertex> vi = gU.getVertices()
      .filter(new NotInGraphBroadcast<>())
      .withBroadcastSet(hypervertices.getGraphHeads(), GraphContainmentFilterBroadcast.GRAPH_ID);

    DataSet<Vertex> vToRet = nuWithGid
      .map(new Value0Of2<>())
      .union(vi)
      .map(new MapFunctionAddGraphElementToGraph2<>(newGraphid));

    // PHASE 3: Recreating the edges
    DataSet<Tuple2<Vertex,GradoopId>> idJoin = vWithGid
      .coGroup(nuWithGid)
      .where(new Value1Of2<>()).equalTo(new Value1Of2<>())
      .with(new CoGroupAssociateOldVerticesWithNewIds())
      .union(vi.map(new MapVerticesAsTuplesWithNullId()));

    DataSet<Edge> edges = gU.getEdges()
      .filter(new NotInGraphBroadcast<>())
      .withBroadcastSet(hypervertices.getGraphHeads(), GraphContainmentFilterBroadcast.GRAPH_ID)
      .fullOuterJoin(idJoin)
      .where(new SourceId<>()).equalTo(new LeftElementId<>())
      .with(new FlatJoinSourceEdgeReference(true))
      .fullOuterJoin(idJoin)
      .where(new TargetId<>()).equalTo(new LeftElementId<>())
      .with(new FlatJoinSourceEdgeReference(false))
      .map(new MapFunctionAddGraphElementToGraph2<>(newGraphid));

    return LogicalGraph.fromDataSets(gh,vToRet,edges,gU.getConfig());

  }

  @Override
  public String getName() {
    return ReduceVertexFusion.class.getName();
  }
}
