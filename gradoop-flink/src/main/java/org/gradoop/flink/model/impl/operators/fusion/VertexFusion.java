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

package org.gradoop.flink.model.impl.operators.fusion;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.api.operators.BinaryGraphToGraphOperator;
import org.gradoop.flink.model.api.operators.GraphGraphCollectionToGraphOperator;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.functions.epgm.SourceId;
import org.gradoop.flink.model.impl.functions.epgm.TargetId;
import org.gradoop.flink.model.impl.functions.graphcontainment.NotInGraphsBroadcast;
import org.gradoop.flink.model.impl.functions.tuple.Value0Of2;
import org.gradoop.flink.model.impl.functions.tuple.Value1Of2;
import org.gradoop.flink.model.impl.functions.utils.LeftSide;
import org.gradoop.flink.model.impl.operators.fusion.functions.CoGroupAssociateOldVerticesWithNewIds;
import org.gradoop.flink.model.impl.operators.fusion.functions.CoGroupGraphHeadToVertex;
import org.gradoop.flink.model.impl.operators.fusion.functions.FlatJoinSourceEdgeReference;
import org.gradoop.flink.model.impl.operators.fusion.functions.LeftElementId;
import org.gradoop.flink.model.impl.operators.fusion.functions.MapFunctionAddGraphElementToGraph2;
import org.gradoop.flink.model.impl.operators.fusion.functions.MapGraphHeadForNewGraph;
import org.gradoop.flink.model.impl.operators.fusion.functions.MapVertexToPairWithGraphId;
import org.gradoop.flink.model.impl.operators.fusion.functions.MapVerticesAsTuplesWithNullId;

import static org.gradoop.flink.model.impl.functions.graphcontainment.GraphsContainmentFilterBroadcast.GRAPH_IDS;


/**
 * Fuses a pattern logical graph set of vertices within the search graph
 *
 */
public class VertexFusion implements BinaryGraphToGraphOperator {

  /**
   * Fusing the already-combined sources
   *
   * @param searchGraph            Logical Graph defining the data lake
   * @param graphPatterns Collection of elements representing which vertices will be merged into
   *                      a vertex
   * @return              A single merged graph
   */
  @Override
  public LogicalGraph execute(LogicalGraph searchGraph, LogicalGraph graphPatterns) {

    // Missing in the theoric definition: creating a new header
    GradoopId newGraphid = GradoopId.get();

    DataSet<GraphHead> gh = searchGraph.getGraphHead()
      .map(new MapGraphHeadForNewGraph(newGraphid));

    DataSet<GradoopId> subgraphIds = graphPatterns.getGraphHead()
      .map(new Id<>());

    // PHASE 1: Induced Subgraphs
    // Associate each vertex to its graph id
    DataSet<Tuple2<Vertex, GradoopId>> vWithGid = graphPatterns.getVertices()
      .coGroup(searchGraph.getVertices())
      .where(new Id<>()).equalTo(new Id<>())
      .with(new LeftSide<>())
      .flatMap(new MapVertexToPairWithGraphId());

    // Associate each gid in hypervertices.H to the merged vertices
    DataSet<Tuple2<Vertex, GradoopId>> nuWithGid  = graphPatterns.getGraphHead()
      .map(new CoGroupGraphHeadToVertex());

    // PHASE 2: Recreating the vertices
    DataSet<Vertex> vi = searchGraph.getVertices()
      .filter(new NotInGraphsBroadcast<>())
      .withBroadcastSet(subgraphIds, GRAPH_IDS);

    // PHASE 3: Recreating the edges
    DataSet<Tuple2<Vertex, GradoopId>> idJoin = vWithGid
      .coGroup(nuWithGid)
      .where(new Value1Of2<>()).equalTo(new Value1Of2<>())
      .with(new CoGroupAssociateOldVerticesWithNewIds())
      .union(vi.map(new MapVerticesAsTuplesWithNullId()));

    DataSet<Vertex> vToRet = nuWithGid
      .coGroup(vWithGid)
      .where(new Value1Of2<>()).equalTo(new Value1Of2<>())
      .with(new LeftSide<>())
      .map(new Value0Of2<>())
      .union(vi)
      .map(new MapFunctionAddGraphElementToGraph2<>(newGraphid));

    DataSet<Edge> edges = searchGraph.getEdges()
      .filter(new NotInGraphsBroadcast<>())
      .withBroadcastSet(subgraphIds, GRAPH_IDS)
      .leftOuterJoin(idJoin)
      .where(new SourceId<>()).equalTo(new LeftElementId<>())
      .with(new FlatJoinSourceEdgeReference(true))
      .leftOuterJoin(idJoin)
      .where(new TargetId<>()).equalTo(new LeftElementId<>())
      .with(new FlatJoinSourceEdgeReference(false))
      .map(new MapFunctionAddGraphElementToGraph2<>(newGraphid));

    return LogicalGraph.fromDataSets(gh, vToRet, edges, searchGraph.getConfig());

  }

  @Override
  public String getName() {
    return VertexFusion.class.getName();
  }
}
