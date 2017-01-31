package org.gradoop.flink.model.impl.operators.join.edgesemantics;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.java.operators.join.JoinType;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.flink.model.impl.operators.join.tuples.CombiningEdgeTuples;

/**
 * Created by Giacomo Bergami on 30/01/17.
 */
public enum PredefinedEdgeSemantics {
  CONJUNCTIVE,
  DISJUNCTIVE;
}
