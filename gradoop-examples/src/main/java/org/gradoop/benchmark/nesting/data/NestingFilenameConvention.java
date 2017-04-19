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
package org.gradoop.benchmark.nesting.data;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.Path;
import org.gradoop.benchmark.nesting.functions.AssociateFileToGraph;
import org.gradoop.benchmark.nesting.functions.ImportEdgeToVertex;
import org.gradoop.benchmark.nesting.functions.SelectElementsInHeads;
import org.gradoop.benchmark.nesting.functions.StringAsEdge;
import org.gradoop.benchmark.nesting.functions.StringAsVertex;
import org.gradoop.benchmark.nesting.functions.TripleWithGraphHeadToId;
import org.gradoop.benchmark.nesting.functions.Value1Of3AsFilter;
import org.gradoop.benchmark.nesting.serializers.Bogus;
import org.gradoop.benchmark.nesting.serializers.DeserializeGradoopidFromFile;
import org.gradoop.benchmark.nesting.serializers.DeserializePairOfIdsFromFile;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.examples.AbstractRunner;
import org.gradoop.flink.io.impl.graph.tuples.ImportEdge;
import org.gradoop.flink.io.impl.graph.tuples.ImportVertex;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.functions.tuple.Value2Of3;
import org.gradoop.flink.model.impl.functions.utils.Self;
import org.gradoop.flink.model.impl.operators.nest.functions.ConstantZero;
import org.gradoop.flink.model.impl.operators.nest.model.indices.NestingIndex;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;

/**
 * Initializing all the variables and utils to be used for the benchmarks
 */
public abstract class NestingFilenameConvention extends AbstractRunner {

  /**
   * Represents the path suffix describing the files for the headers
   */
  protected static final String INDEX_HEADERS_SUFFIX = "-heads.bin";

  /**
   * Represents the path suffix describing the files for the vertices
   */
  protected static final String INDEX_VERTEX_SUFFIX = "-vertex.bin";

  /**
   * Represents the path suffix describing the edges
   */
  protected static final String INDEX_EDGE_SUFFIX = "-edges.bin";

  /**
   * Represents the file prefix for the files describing pieces of information for the
   * left operand
   */
  protected static final String LEFT_OPERAND = "left";

  /**
   * Represents the file prefix for the files describing pieces of informations for the
   * right operand
   */
  protected static final String RIGHT_OPERAND = "right";

  /**
   * Global environment
   */
  protected static final ExecutionEnvironment ENVIRONMENT;

  /**
   * GradoopFlink configuration
   */
  protected static final GradoopFlinkConfig CONFIGURATION;

  static {
    ENVIRONMENT = getExecutionEnvironment();
    CONFIGURATION = GradoopFlinkConfig.createConfig(ENVIRONMENT);
  }

  /**
   * File where to store the benchmarks
   */
  private final String csvPath;

  /**
   * Bas Path
   */
  private final String basePath;

  /**
   * Default constructor for running the tests
   * @param csvPath   File where to store the intermediate results
   * @param basePath  Base path where the indexed data is loaded
   */
  public NestingFilenameConvention(String basePath, String csvPath) {
    this.csvPath = csvPath;
    this.basePath = basePath;
  }

  /**
   * Generating the base path for the strings
   * @param path            Base path
   * @param isLeftOperand   Checks if it is a left operand
   * @return                Initialized and finalized string
   */
  public static String generateOperandBasePath(String path, boolean isLeftOperand) {
    return path +
            (path.endsWith(Path.SEPARATOR) ? "" : Path.SEPARATOR) +
            (isLeftOperand ? LEFT_OPERAND : RIGHT_OPERAND);
  }

  /**
   * Loads an index located in a given specific folder + operand prefix
   * @param filename  Foder
   * @return          Loaded index
   */
  public static NestingIndex loadNestingIndex(String filename) {
    DataSet<GradoopId> headers = ENVIRONMENT
      .readFile(new DeserializeGradoopidFromFile(), filename + INDEX_HEADERS_SUFFIX);
    DataSet<Tuple2<GradoopId, GradoopId>> vertexIndex = ENVIRONMENT
      .readFile(new DeserializePairOfIdsFromFile(), filename + INDEX_VERTEX_SUFFIX);
    DataSet<Tuple2<GradoopId, GradoopId>> edgeIndex = ENVIRONMENT
      .readFile(new DeserializePairOfIdsFromFile(), filename + INDEX_EDGE_SUFFIX);

    return new NestingIndex(headers, vertexIndex, edgeIndex);
  }

  /**
   * Returns…
   * @return the basic path containing the path where the serialized information is stored
   */
  public String getBasePath() {
    return basePath;
  }


  public <T> void register(DataSet<T> toRegister, String registerAs, int phaseNo) throws Exception {
    String name = getClass().getName()+": "+registerAs+"-"+phaseNo;
    toRegister.map(new Self<T>()).output(new Bogus<T>(name)).name(name);
  }

  public void benchmark(int phaseNo) throws Exception {
    //String plan = ENVIRONMENT.getExecutionPlan();
    ENVIRONMENT.execute(getClass().getSimpleName());

    /*String planFile = System.getProperty("user.home");
    planFile += planFile.endsWith(Path.SEPARATOR) ? "" : Path.SEPARATOR;
    planFile += getClass().getSimpleName() +"_plan.json";
    Files.write(Paths.get(planFile), plan.getBytes(Charset.forName("UTF-8")), StandardOpenOption
      .TRUNCATE_EXISTING, StandardOpenOption.CREATE);
*/
    // Writing the result of the benchmark to the file
    String line = getClass().getSimpleName() + "," +
      "Benchmark," +
      phaseNo + "," +
      this.basePath + "," +
      ENVIRONMENT.getLastJobExecutionResult().getNetRuntime() + "\n";

    Files.write(Paths.get(this.csvPath), line.getBytes(Charset.forName("UTF-8")),
      StandardOpenOption.CREATE, StandardOpenOption.APPEND);
  }

  /**
   * Loads the left operand from the partially stored data
   * @param start       Data To Be loaded
   * @param vertices    Mappings for the vertices
   * @param edges       Mappings for the edges
   * @return            The right operand (graph search)
   * @throws Exception
   */
  public static LogicalGraph loadLeftOperand(GraphCollectionDelta start, DataSet<Vertex> vertices,
    DataSet<Edge> edges) throws Exception {
    DataSet<GraphHead> operandHeads = start.getHeads()
      .filter(new Value1Of3AsFilter(true))
      .map(new Value2Of3<>());

    DataSet<Vertex> operandVertices = vertices
      .joinWithTiny(operandHeads)
      .where(new ConstantZero<>()).equalTo(new ConstantZero<>())
      .with(new SelectElementsInHeads<>());

    DataSet<Edge> operandEdges = edges
      .joinWithTiny(operandHeads)
      .where(new ConstantZero<>()).equalTo(new ConstantZero<>())
      .with(new SelectElementsInHeads<>());

    return LogicalGraph.fromDataSets(operandHeads, operandVertices, operandEdges, CONFIGURATION);
  }

  /**
   * Loads the right operand from the partially stored data
   * @param start       Data To Be loaded
   * @param vertices    Mappings for the vertices
   * @param edges       Mappings for the edges
   * @return            The left operand (graph Pattern)
   * @throws Exception
   */
  public static GraphCollection loadRightOperand(GraphCollectionDelta start,
    DataSet<Vertex> vertices, DataSet<Edge> edges) throws Exception {
    DataSet<GraphHead> operandHeads = start.getHeads()
      .filter(new Value1Of3AsFilter(true))
      .map(new Value2Of3<>());

    DataSet<Vertex> operandVertices = vertices
      .joinWithTiny(operandHeads)
      .where(new ConstantZero<>()).equalTo(new ConstantZero<>())
      .with(new SelectElementsInHeads<>());

    DataSet<Edge> operandEdges = edges
      .joinWithTiny(operandHeads)
      .where(new ConstantZero<>()).equalTo(new ConstantZero<>())
      .with(new SelectElementsInHeads<>());

    return GraphCollection.fromDataSets(operandHeads, operandVertices, operandEdges, CONFIGURATION);
  }

  /**
   * Updates the graph definition
   * @param delta                 Previous updated version
   * @param edgesGlobalFile     File defining the edges
   * @param verticesGlobalFile  File defining the vertices
   * @return                      Updates the graph definition
   */
  protected static GraphCollectionDelta deltaUpdateGraphCollection(GraphCollectionDelta delta,
    String edgesGlobalFile, String verticesGlobalFile) {

    GraphCollectionDelta deltaPlus = extractGraphFromFiles(edgesGlobalFile,
      verticesGlobalFile, false);

    return new GraphCollectionDelta(delta.getHeads().union(deltaPlus.getHeads()),
      delta.getVertices().union(deltaPlus.getVertices()),
      delta.getEdges().union(deltaPlus.getEdges()));
  }

  /**
   * Extracts the partial graph definition form files
   * @param edgesGlobalFile       Edges file
   * @param verticesGlobalFile    Vertices file
   * @param isLeftOperand           If the operand represented is the left one
   * @return  Instance of the operand
   */
  protected static GraphCollectionDelta extractGraphFromFiles(String edgesGlobalFile,
    String verticesGlobalFile, boolean isLeftOperand) {
    // This information is going to be used when serializing the operands
    DataSet<Tuple3<String, Boolean, GraphHead>> heads =
      ENVIRONMENT.fromElements(edgesGlobalFile)
        .map(new AssociateFileToGraph(isLeftOperand, CONFIGURATION.getGraphHeadFactory()));

    // Extracting the head id. Required to create a LogicalGraph
    DataSet<GradoopId> head = heads.map(new TripleWithGraphHeadToId());

    // Edges with graph association
    DataSet<Tuple2<ImportEdge<String>, GradoopId>> edges =
      ENVIRONMENT.readTextFile(edgesGlobalFile)
        .flatMap(new StringAsEdge())
        .cross(head);

    // Vertices with graph association
    DataSet<Tuple2<ImportVertex<String>, GradoopId>> vertices =
      edges
        .flatMap(new ImportEdgeToVertex<>());

    if (verticesGlobalFile != null) {
      vertices = ENVIRONMENT.readTextFile(verticesGlobalFile)
        .map(new StringAsVertex())
        .cross(head)
        .union(vertices);
    }
    return new GraphCollectionDelta(heads, vertices, edges);
  }

}