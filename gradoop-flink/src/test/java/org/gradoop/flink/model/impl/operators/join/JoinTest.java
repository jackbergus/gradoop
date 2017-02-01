package org.gradoop.flink.model.impl.operators.join;

import org.apache.flink.api.java.operators.join.JoinType;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.api.functions.Function;
import org.gradoop.flink.model.impl.operators.fusion.Fusion;
import org.gradoop.flink.model.impl.operators.fusion.FusionUtils;
import org.gradoop.flink.model.impl.operators.join.edgesemantics.GeneralEdgeSemantics;
import org.gradoop.flink.model.impl.operators.join.edgesemantics.PredefinedEdgeSemantics;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Test;

/**
 * Created by vasistas on 01/02/17.
 */
public class JoinTest extends GradoopFlinkTestBase {

  public static JoinType GRAPH_INNER_JOIN = JoinType.INNER;

  public static final  PredefinedEdgeSemantics CONJUNCTIVE_BASICS = PredefinedEdgeSemantics
    .CONJUNCTIVE;
  public static final Function<Tuple2<String,String>,String> BASIC_LABEL_CONCATENATION = xy
    -> xy.f0+"+"+ xy.f1;
  public static final GeneralEdgeSemantics CONJUNCTIVE_SEMANTICS =  GeneralEdgeSemantics
    .fromEdgePredefinedSemantics(x -> y -> true,CONJUNCTIVE_BASICS,BASIC_LABEL_CONCATENATION);

  @Test
  public void empty_empty_empty() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString("empty[]");
    LogicalGraph searchGraph = loader.getLogicalGraphByVariable("empty");
    LogicalGraph patternGraph = loader.getLogicalGraphByVariable("empty");
    GraphThetaJoin f = new GraphThetaJoin(GRAPH_INNER_JOIN, CONJUNCTIVE_SEMANTICS);
    LogicalGraph output = f.execute(searchGraph, patternGraph);
    LogicalGraph expected = ((loader.getLogicalGraphByVariable("empty")));
    collectAndAssertTrue(output.equalsByData(expected));
  }

  @Test
  public void emptyG_emptyG_emptyGG() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString("emptyG:G[] emptyGG:GG[]");
    LogicalGraph searchGraph = loader.getLogicalGraphByVariable("emptyG");
    LogicalGraph patternGraph = loader.getLogicalGraphByVariable("emptyG");
    GraphThetaJoin f = new GraphThetaJoin(GRAPH_INNER_JOIN, CONJUNCTIVE_SEMANTICS);
    LogicalGraph output = f.execute(searchGraph, patternGraph);
    LogicalGraph expected = ((loader.getLogicalGraphByVariable("emptyGG")));
    collectAndAssertTrue(output.equalsByData(expected));
  }

  @Test
  public void empty_emptyVertex_to_empty() throws Exception {
    FlinkAsciiGraphLoader loader =
      getLoaderFromString("empty[]" + "emptyVertex {emptyVertex : \"graph\"}[()]");
    LogicalGraph searchGraph = loader.getLogicalGraphByVariable("empty");
    LogicalGraph patternGraph = loader.getLogicalGraphByVariable("emptyVertex");
    Fusion f = new Fusion();
    LogicalGraph output = f.execute(searchGraph, patternGraph);
    LogicalGraph expected = ((loader.getLogicalGraphByVariable("empty")));
    collectAndAssertTrue(output.equalsByData(expected));
    collectAndAssertTrue(FusionUtils
      .myInducedEdgeSubgraphForFusion(searchGraph,expected)
      .equalsByElementIds(FusionUtils.myInducedEdgeSubgraphForFusion(expected,searchGraph)));
  }

}
