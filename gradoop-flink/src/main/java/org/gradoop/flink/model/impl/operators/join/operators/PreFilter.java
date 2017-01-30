package org.gradoop.flink.model.impl.operators.join.operators;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.api.entities.EPGMElement;

import java.util.function.Function;

/**
 * Defining a non-serializable function allowing to pre-evaluate the vertices if required.
 *
 * Created by Giacomo Bergami on 30/01/17.
 */
public interface PreFilter<K extends EPGMElement,P> extends Function<DataSet<K>,DataSet<Tuple2<K,P
  >>> {

}
