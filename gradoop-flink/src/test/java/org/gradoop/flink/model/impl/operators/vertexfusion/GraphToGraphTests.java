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
 * along with Gradoop.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.flink.model.impl.operators.vertexfusion;

import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.api.operators.GraphGraphCollectionToGraphOperator;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Test;

public abstract class GraphToGraphTests extends GradoopFlinkTestBase {
  
  protected abstract GraphGraphCollectionToGraphOperator op();

  @Test
  public void emptyAndEmptyToEmpty() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString("empty:G[]");
    LogicalGraph searchGraph = loader.getLogicalGraphByVariable("empty");
    GraphCollection patternGraph = loader.getGraphCollectionByVariables("empty");
    GraphGraphCollectionToGraphOperator f = op();
    LogicalGraph output = f.execute(searchGraph, patternGraph);
    LogicalGraph expected = loader.getLogicalGraphByVariable("empty");
    collectAndAssertTrue(output.equalsByData(expected));
  }

  @Test
  public void emptyAndEmptyvertexToEmpty() throws Exception {
    FlinkAsciiGraphLoader loader =
      getLoaderFromString("empty:G[]" + "emptyVertex:G {emptyVertex : \"graph\"}[()]");
    LogicalGraph searchGraph = loader.getLogicalGraphByVariable("empty");
    GraphCollection patternGraph = loader.getGraphCollectionByVariables("emptyVertex");
    GraphGraphCollectionToGraphOperator f = op();
    LogicalGraph output = f.execute(searchGraph, patternGraph);
    LogicalGraph expected = loader.getLogicalGraphByVariable("empty");
    collectAndAssertTrue(output.equalsByData(expected));
  }

  @Test
  public void emptyvertexAndEmptyToEmptyvertex() throws Exception {
    FlinkAsciiGraphLoader loader =
      getLoaderFromString("emptyVertex: {emptyVertex : \"graph\"}[()]" + "empty:[]");
    LogicalGraph searchGraph = loader.getLogicalGraphByVariable("emptyVertex");
    GraphCollection patternGraph = loader.getGraphCollectionByVariables("empty");
    GraphGraphCollectionToGraphOperator f = op();
    LogicalGraph output = f.execute(searchGraph, patternGraph);
    LogicalGraph expected = loader.getLogicalGraphByVariable("emptyVertex");
    collectAndAssertTrue(output.equalsByData(expected));
  }

  @Test
  public void emptyvertexAndGraphwithaToEmptyvertex() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(
      "emptyVertex:G {emptyVertex : \"graph\"}[()]" +
        "graphWithA:G {graphWithA : \"graph\"}[(a:A {atype : \"avalue\"})]");
    LogicalGraph searchGraph = loader.getLogicalGraphByVariable("emptyVertex");
    GraphCollection patternGraph = loader.getGraphCollectionByVariables("graphWithA");
    GraphGraphCollectionToGraphOperator f = op();
    LogicalGraph output = f.execute(searchGraph, patternGraph);
    LogicalGraph expected = loader.getLogicalGraphByVariable("emptyVertex");
    collectAndAssertTrue(output.equalsByData(expected));
  }

  @Test
  public void emptyvertexAndEmptyvertexToSingleInside() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(
      "emptyVertex:G {emptyVertex : \"graph\"}[()]" +
        "singleInside:G {emptyVertex : \"graph\"} [(u:G {emptyVertex : \"graph\"})]");
    LogicalGraph searchGraph = loader.getLogicalGraphByVariable("emptyVertex");
    GraphCollection patternGraph = loader.getGraphCollectionByVariables("emptyVertex");
    GraphGraphCollectionToGraphOperator f = op();
    LogicalGraph output = f.execute(searchGraph, patternGraph);
    LogicalGraph expected = loader.getLogicalGraphByVariable("singleInside");
    collectAndAssertTrue(output.equalsByData(expected));
  }

  @Test
  public void graphwithaAndGraphwithaToAgraphlabels() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(
      "graphWithA:G {graphWithA : \"graph\"}[(a:A {atype : \"avalue\"})]" +
        "aGraphLabels:G {graphWithA : \"graph\"} [(:G {graphWithA : \"graph\"})]");
    LogicalGraph searchGraph = loader.getLogicalGraphByVariable("graphWithA");
    GraphCollection patternGraph = loader.getGraphCollectionByVariables("graphWithA");
    GraphGraphCollectionToGraphOperator f = op();
    LogicalGraph output = f.execute(searchGraph, patternGraph);
    LogicalGraph expected = loader.getLogicalGraphByVariable("aGraphLabels");
    collectAndAssertTrue(output.equalsByData(expected));
  }

  @Test
  public void graphwithaAndEmptyToGraphwitha() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(
      "graphWithA:G {graphWithA : \"graph\"} [(a:A {atype : \"avalue\"})]" + "empty:G[]");
    LogicalGraph searchGraph = loader.getLogicalGraphByVariable("graphWithA");
    GraphCollection patternGraph = loader.getGraphCollectionByVariables("empty");
    GraphGraphCollectionToGraphOperator f = op();
    LogicalGraph output = f.execute(searchGraph, patternGraph);
    LogicalGraph expected = loader.getLogicalGraphByVariable("graphWithA");
    collectAndAssertTrue(output.equalsByData(expected));
  }

  @Test
  public void graphwithaAndEmptyvertexToGraphwitha() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(
      "graphWithA:G {graphWithA : \"graph\"} [(a:A {atype : \"avalue\"})]" +
        "emptyVertex:G {emptyVertex : \"graph\"}[()]");
    LogicalGraph searchGraph = loader.getLogicalGraphByVariable("graphWithA");
    GraphCollection patternGraph = loader.getGraphCollectionByVariables("emptyVertex");
    GraphGraphCollectionToGraphOperator f = op();
    LogicalGraph output = f.execute(searchGraph, patternGraph);
    LogicalGraph expected = loader.getLogicalGraphByVariable("graphWithA");
    collectAndAssertTrue(output.equalsByData(expected));
  }

  @Test
  public void abedgewithalphaAndGraphwithaToAggregatedasource() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(
      "ab_edgeWithAlpha:G {ab_edgeWithAlpha : \"graph\"}[" +
        "(a:A {atype : \"avalue\"})-[alpha:AlphaEdge {alphatype : \"alphavalue\"}]->(b:B {btype :" +
        " \"bvalue\"}) " + "]" +

        "graphWithA:G {graphWithA : \"graph\"}[(a:A {atype : \"avalue\"})]" +

        "aggregatedASource:G {ab_edgeWithAlpha : \"graph\"} [" +
        "(:G {graphWithA : \"graph\"})-[:AlphaEdge {alphatype : \"alphavalue\"}]->(:B {btype : " +
        "\"bvalue\"}) " + "]");
    LogicalGraph searchGraph = loader.getLogicalGraphByVariable("ab_edgeWithAlpha");
    GraphCollection patternGraph = loader.getGraphCollectionByVariables("graphWithA");
    GraphGraphCollectionToGraphOperator f = op();
    LogicalGraph output = f.execute(searchGraph, patternGraph);
    LogicalGraph expected = loader.getLogicalGraphByVariable("aggregatedASource");
    collectAndAssertTrue(output.equalsByData(expected));
  }

  @Test
  public void abedgewithalphaAndEmptyToAbedgeWithAlpha() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(
      "ab_edgeWithAlpha:G {ab_edgeWithAlpha : \"graph\"} [" +
        "(a:A {atype : \"avalue\"})-[alpha:AlphaEdge {alphatype : \"alphavalue\"}]->(b:B {btype :" +
        " \"bvalue\"}) " + "]" +

        "empty:G[]");
    LogicalGraph searchGraph = loader.getLogicalGraphByVariable("ab_edgeWithAlpha");
    GraphCollection patternGraph = loader.getGraphCollectionByVariables("empty");
    GraphGraphCollectionToGraphOperator f = op();
    LogicalGraph output = f.execute(searchGraph, patternGraph);
    LogicalGraph expected = loader.getLogicalGraphByVariable("ab_edgeWithAlpha");
    collectAndAssertTrue(output.equalsByData(expected));
  }

  @Test
  public void abEdgeWithAlphaAndEmptyVerteToAbedgeWithAlpha() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(
      "ab_edgeWithAlpha: [" +
        "(a:A {atype : \"avalue\"})-[alpha:AlphaEdge {alphatype : \"alphavalue\"}]->(b:B {btype :" +
        " \"bvalue\"}) ]" +

        "emptyVertex:G {emptyVertex : \"graph\"}[()]");
    LogicalGraph searchGraph = loader.getLogicalGraphByVariable("ab_edgeWithAlpha");
    GraphCollection patternGraph = loader.getGraphCollectionByVariables("emptyVertex");
    GraphGraphCollectionToGraphOperator f = op();
    LogicalGraph output = f.execute(searchGraph, patternGraph);
    LogicalGraph expected = loader.getLogicalGraphByVariable("ab_edgeWithAlpha");
    collectAndAssertTrue(output.equalsByData(expected));
  }

  @Test
  public void abEdgeWithAlphaAndAbEdgeWithAlphaToFusededgeWithAlpha2() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(
      "ab_edgeWithAlpha:G {ab_edgeWithAlpha : \"graph\"}[" +
        "(a:A {atype : \"avalue\"})-[alpha:AlphaEdge {alphatype : \"alphavalue\"}]->(b:B {btype :" +
        " \"bvalue\"}) ]" +
        "ab_edgeWithAlpha2:G {ab_edgeWithAlpha : \"graph\"}[" +
        "(a:A {atype : \"avalue\"}) (b:B {btype :" +
        " \"bvalue\"}) ]" +

        "fused_edgeWithAlpha:G {ab_edgeWithAlpha : \"graph\"} [(z:G {ab_edgeWithAlpha : \"graph\"})" +
        "-[:AlphaEdge {alphatype :\"alphavalue\"}]->(z:G {ab_edgeWithAlpha : \"graph\"})]");
    LogicalGraph searchGraph = loader.getLogicalGraphByVariable("ab_edgeWithAlpha");
    GraphCollection patternGraph = loader.getGraphCollectionByVariables("ab_edgeWithAlpha2");
    GraphGraphCollectionToGraphOperator f = op();
    LogicalGraph output = f.execute(searchGraph, patternGraph);
    LogicalGraph expected = loader.getLogicalGraphByVariable("fused_edgeWithAlpha");
    collectAndAssertTrue(output.equalsByData(expected));
  }

  @Test
  public void abEdgeWithAlphaAndAbEdgeWithAlphaToFusededgeWithAlpha() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(
      "ab_edgeWithAlpha:G {ab_edgeWithAlpha : \"graph\"}[" +
        "(a:A {atype : \"avalue\"})" +
        "-[alpha:AlphaEdge {alphatype : \"alphavalue\"}]->(b:B {btype :\"bvalue\"}) " +
        "]" +

        "fused_edgeWithAlpha:G {ab_edgeWithAlpha : \"graph\"} [(:G {ab_edgeWithAlpha : \"graph\"})]");
    LogicalGraph searchGraph = loader.getLogicalGraphByVariable("ab_edgeWithAlpha");
    GraphCollection patternGraph = loader.getGraphCollectionByVariables("ab_edgeWithAlpha");
    GraphGraphCollectionToGraphOperator f = op();
    LogicalGraph output = f.execute(searchGraph, patternGraph);
    LogicalGraph expected = loader.getLogicalGraphByVariable("fused_edgeWithAlpha");
    collectAndAssertTrue(output.equalsByData(expected));
  }

  @Test
  public void abEdgeWithAlphaAndAbedgeWithBetaToAbedgeWithBetaLoop() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(
      "ab_edgeWithAlpha:G {ab_edgeWithAlpha : \"graph\"}[" +
        "(a:A {atype : \"avalue\"})"+
        "-[alpha:AlphaEdge {alphatype : \"alphavalue\"}]->(b:B {btype :\"bvalue\"}) " +
        "]" +

        "ab_edgeWithBeta:G {ab_edgeWithBeta : \"graph\"}[" +
        "(a:A {atype : \"avalue\"})" +
        "-[beta:BetaEdge {betatype : \"betavalue\"}]->(b:B {btype : \"bvalue\"}) " +
        "]" +

        "ab_edgeWithBeta_loop:G {ab_edgeWithAlpha : \"graph\"} [" +
        "(g2:G {ab_edgeWithBeta : \"graph\"})" +
        "-[:AlphaEdge {alphatype : \"alphavalue\"}]->(g2:G {ab_edgeWithBeta : \"graph\"}) " +
        "]"
    );
    LogicalGraph searchGraph = loader.getLogicalGraphByVariable("ab_edgeWithAlpha");
    GraphCollection patternGraph = loader.getGraphCollectionByVariables("ab_edgeWithBeta");
    GraphGraphCollectionToGraphOperator f = op();
    LogicalGraph output = f.execute(searchGraph, patternGraph);
    LogicalGraph expected = loader.getLogicalGraphByVariable("ab_edgeWithBeta_loop");
    collectAndAssertTrue(output.equalsByData(expected));
  }

  @Test
  public void abEdgeWithBetaAndEmptyToAbedgeWithBeta() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(
      "ab_edgeWithBeta: [" +
        "(a:A {atype : \"avalue\"})" +
        "-[beta:BetaEdge {betatype : \"betavalue\"}]->(b:B {btype :\"bvalue\"}) " + "]" +

        "empty:G[]");
    LogicalGraph searchGraph = loader.getLogicalGraphByVariable("ab_edgeWithBeta");
    GraphCollection patternGraph = loader.getGraphCollectionByVariables("empty");
    GraphGraphCollectionToGraphOperator f = op();
    LogicalGraph output = f.execute(searchGraph, patternGraph);
    LogicalGraph expected = loader.getLogicalGraphByVariable("ab_edgeWithBeta");
    collectAndAssertTrue(output.equalsByData(expected));
  }

  @Test
  public void abedgeWithBetaAndEmptyVertexToAbedgeWithBeta() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(
      "ab_edgeWithBeta:G {ab_edgeWithBeta : \"graph\"}[" +
        "(a:A {atype : \"avalue\"})" +
        "-[beta:BetaEdge {betatype : \"betavalue\"}]->(b:B {btype :\"bvalue\"}) " +
        "]" +

        "emptyVertex:G {emptyVertex : \"graph\"}[()]");
    LogicalGraph searchGraph = loader.getLogicalGraphByVariable("ab_edgeWithBeta");
    GraphCollection patternGraph = loader.getGraphCollectionByVariables("emptyVertex");
    GraphGraphCollectionToGraphOperator f = op();
    LogicalGraph output = f.execute(searchGraph, patternGraph);
    LogicalGraph expected = loader.getLogicalGraphByVariable("ab_edgeWithBeta");
    collectAndAssertTrue(output.equalsByData(expected));
  }

  @Test
  public void abEdgeWithBetaAndAbedgeWithBetaToFusedEdgeWithBeta() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(
      "ab_edgeWithBeta:G {ab_edgeWithBeta : \"graph\"}[" +
        "(a:A {atype : \"avalue\"})-[beta:BetaEdge {betatype : \"betavalue\"}]->(b:B {btype : " +
        "\"bvalue\"}) " + "]" + "" +
        "fused_edgeWithBeta:G {ab_edgeWithBeta : \"graph\"} [(:G {ab_edgeWithBeta : \"graph\"})]");
    LogicalGraph searchGraph = loader.getLogicalGraphByVariable("ab_edgeWithBeta");
    GraphCollection patternGraph = loader.getGraphCollectionByVariables("ab_edgeWithBeta");
    GraphGraphCollectionToGraphOperator f = op();
    LogicalGraph output = f.execute(searchGraph, patternGraph);
    LogicalGraph expected = loader.getLogicalGraphByVariable("fused_edgeWithBeta");
    collectAndAssertTrue(output.equalsByData(expected));
  }

  @Test
  public void abcdGraphAndAbcdGraphToAbdGraph() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString("abcdGraph:G {abcdGraph : \"graph\"}[" +
      "(a:A {atype : \"avalue\"})-[beta:BetaEdge {betatype : \"betavalue\"}]->(b:B {btype : " +
      "\"bvalue\"})  " +
      "(b:B {btype : \"bvalue\"})-[g:GammaEdge {gtype : \"gvalue\"}]->(c:C {ctype : \"cvalue\"}) " +
      "]" + "" + "abdGraph:G {abcdGraph : \"graph\"} [(:G {abcdGraph : \"graph\"})]");
    LogicalGraph searchGraph = loader.getLogicalGraphByVariable("abcdGraph");
    GraphCollection patternGraph = loader.getGraphCollectionByVariables("abcdGraph");
    GraphGraphCollectionToGraphOperator f = op();
    LogicalGraph output = f.execute(searchGraph, patternGraph);
    LogicalGraph expected = loader.getLogicalGraphByVariable("abdGraph");
    collectAndAssertTrue(output.equalsByData(expected));
  }

  @Test
  public void semicomplexAndLooplessPatternToFirstmatch() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString("semicomplex:G {semicomplex : \"graph\"}[" +
      "(a:A {atype : \"avalue\"})" +
        "-[alpha:AlphaEdge {alphatype : \"alphavalue\"}]->(b:B {btype : \"bvalue\"})  " +
      "(b:B {btype : \"bvalue\"})-[l:loop {ltype : \"lvalue\"}]->(b:B {btype : \"bvalue\"})  " +
      "(b:B {btype : \"bvalue\"})-->(c:C {ctype : \"cvalue\"})  " +
      "(c:C {ctype : \"cvalue\"})-->(e:E {etype : \"evalue\"})  " +
      "(c:C {ctype : \"cvalue\"})" +
        "-[beta:BetaEdge {betatype : \"betavalue\"}]->(d:D {dtype : \"dvalue\"})  " +
      "(d:D {dtype : \"dvalue\"})-->(e:E {etype : \"evalue\"}) " +
      "]" +

      "looplessPattern:G {looplessPattern : \"graph\"}[" +
      "(a:A {atype : \"avalue\"})" +
        "-[alpha:AlphaEdge {alphatype : \"alphavalue\"}]->(b:B {btype : \"bvalue\"})  " +
      "(d:D {dtype : \"dvalue\"})" +
      "]" +

      "firstmatch:G {semicomplex : \"graph\"} [" +
      "(g2:G {looplessPattern : \"graph\"})-->(c2:C {ctype : \"cvalue\"})  " +
      "(g2:G {looplessPattern : \"graph\"})-->(e2:E {etype : \"evalue\"})  " +
      "(g2:G {looplessPattern : \"graph\"})" +
        "-[:loop {ltype : \"lvalue\"}]->(g2:G {looplessPattern : \"graph\"})  " +
      "(c2:C {ctype : \"cvalue\"})" +
        "-[:BetaEdge {betatype : \"betavalue\"}]->(g2:G {looplessPattern : \"graph\"})  " +
      "(c2:C {ctype : \"cvalue\"})-->(e2:E {etype : \"evalue\"}) " +
      "]"
    );
    LogicalGraph searchGraph = loader.getLogicalGraphByVariable("semicomplex");
    GraphCollection patternGraph = loader.getGraphCollectionByVariables("looplessPattern");
    GraphGraphCollectionToGraphOperator f = op();
    LogicalGraph output = f.execute(searchGraph, patternGraph);
    LogicalGraph expected = loader.getLogicalGraphByVariable("firstmatch");
    collectAndAssertTrue(output.equalsByData(expected));
  }

  @Test
  public void trickyLooplessAndPatternToThirdmatch() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString("tricky:G {tricky : \"graph\"}[" +
      "(a:A {atype : \"avalue\"})" +
          "-[alpha:AlphaEdge {alphatype : \"alphavalue\"}]->(b:B {btype : \"bvalue\"})  " +
      "(d:D {dtype : \"dvalue\"})-[l:loop {ltype : \"lvalue\"}]->(b:B {btype : \"bvalue\"})  " +
      "(b:B {btype : \"bvalue\"})-->(c:C {ctype : \"cvalue\"})  " +
      "(c:C {ctype : \"cvalue\"})-->(e:E {etype : \"evalue\"})  " +
      "(c:C {ctype : \"cvalue\"})" +
          "-[beta:BetaEdge {betatype : \"betavalue\"}]->(d:D {dtype : \"dvalue\"})  " +
      "(d:D {dtype : \"dvalue\"})-->(e:E {etype : \"evalue\"}) " +
      "]" +

      "looplessPattern:G {looplessPattern : \"graph\"}[" +
      "(a:A {atype : \"avalue\"})" +
          "-[alpha:AlphaEdge {alphatype : \"alphavalue\"}]->(b:B {btype :\"bvalue\"})  " +
      "(d:D {dtype : \"dvalue\"})" +
      "]" +

      "thirdmatch:G {tricky : \"graph\"} [" +
      "(g2:G {looplessPattern : \"graph\"})-->(c2:C {ctype : \"cvalue\"})  " +
      "(g2:G {looplessPattern : \"graph\"})-->(e2:E {etype : \"evalue\"})  " +
      "(g2:G {looplessPattern : \"graph\"})" +
          "-[:loop {ltype : \"lvalue\"}]->(g2:G {looplessPattern : \"graph\"})  " +
      "(c2:C {ctype : \"cvalue\"})" +
          "-[:BetaEdge {betatype : \"betavalue\"}]->(g2:G {looplessPattern : \"graph\"})  " +
      "(c2:C {ctype : \"cvalue\"})-->(e2:E {etype : \"evalue\"}) " +
      "]"
    );
    LogicalGraph searchGraph = loader.getLogicalGraphByVariable("tricky");
    GraphCollection patternGraph = loader.getGraphCollectionByVariables("looplessPattern");
    GraphGraphCollectionToGraphOperator f = op();
    LogicalGraph output = f.execute(searchGraph, patternGraph);
    LogicalGraph expected = loader.getLogicalGraphByVariable("thirdmatch");
    collectAndAssertTrue(output.equalsByData(expected));
  }

  @Test
  public void sourceAndPatternToSourceFusewithPattern() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString("source:G {source : \"graph\"}[" +
      "(a:A {atype : \"avalue\"})" +
          "-[alpha:AlphaEdge {alphatype : \"alphavalue\"}]->(b:B {btype : \"bvalue\"})  " +
      "(a:A {atype : \"avalue\"})" +
          "-[beta:BetaEdge {betatype : \"betavalue\"}]->(b:B {btype : \"bvalue\"})  " +
      "(a:A {atype : \"avalue\"})-[l:loop {ltype : \"lvalue\"}]->(c:C {ctype : \"cvalue\"})  " +
      "(c:C {ctype : \"cvalue\"})-[g:GammaEdge {gtype : \"gvalue\"}]->(d:D {dtype : \"dvalue\"}) " +
      "]" +

      "pattern:G {pattern : \"graph\"}[" +
      "(a:A {atype : \"avalue\"})" +
          "-[alpha:AlphaEdge {alphatype : \"alphavalue\"}]->(b:B {btype : \"bvalue\"}) " +
      "]" +

      "source_fusewith_pattern:G {source : \"graph\"} [" +
      "(k2:G {pattern : \"graph\"})" +
        "-[:BetaEdge {betatype : \"betavalue\"}]->(k2:G {pattern :\"graph\"})  " +
      "(k2:G {pattern : \"graph\"})-[:loop {ltype : \"lvalue\"}]->(c2:C {ctype : \"cvalue\"}) " +
      "(c2:C {ctype : \"cvalue\"})-[:GammaEdge {gtype : \"gvalue\"}]->(d2:D {dtype : \"dvalue\"})" +
      "]"
    );
    LogicalGraph searchGraph = loader.getLogicalGraphByVariable("source");
    GraphCollection patternGraph = loader.getGraphCollectionByVariables("pattern");
    GraphGraphCollectionToGraphOperator f = op();
    LogicalGraph output = f.execute(searchGraph, patternGraph);
    LogicalGraph expected = loader.getLogicalGraphByVariable("source_fusewith_pattern");
    collectAndAssertTrue(output.equalsByData(expected));
  }

  @Test
  public void patternAndSourceToPatternfusewithsource() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString("pattern:G {pattern : \"graph\"}[" +
      "(a:A {atype : \"avalue\"})" +
      "-[alpha:AlphaEdge {alphatype : \"alphavalue\"}]->(b:B {btype :\"bvalue\"}) " +
      "]" +

      "source:G {source : \"graph\"}[" +
      "(a:A {atype : \"avalue\"})" +
        "-[alpha:AlphaEdge {alphatype : \"alphavalue\"}]->(b:B {btype : \"bvalue\"})  " +
      "(a:A {atype : \"avalue\"})" +
        "-[beta:BetaEdge {betatype : \"betavalue\"}]->(b:B {btype : \"bvalue\"})  " +
      "(a:A {atype : \"avalue\"})-[l:loop {ltype : \"lvalue\"}]->(c:C {ctype : \"cvalue\"})  " +
      "(c:C {ctype : \"cvalue\"})-[g:GammaEdge {gtype : \"gvalue\"}]->(d:D {dtype : \"dvalue\"}) " +
      "]" +

      "pattern_fusewith_source:G {pattern : \"graph\"} [(u:G {source : \"graph\"})]");
    LogicalGraph searchGraph = loader.getLogicalGraphByVariable("pattern");
    GraphCollection patternGraph = loader.getGraphCollectionByVariables("source");
    GraphGraphCollectionToGraphOperator f = op();
    LogicalGraph output = f.execute(searchGraph, patternGraph);
    LogicalGraph expected = loader.getLogicalGraphByVariable("pattern_fusewith_source");
    collectAndAssertTrue(output.equalsByData(expected));
  }

}
