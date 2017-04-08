package org.gradoop.benchmark.nesting.functions;

import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.io.impl.graph.tuples.ImportVertex;

/**
 * Created by vasistas on 08/04/17.
 */
@FunctionAnnotation.ForwardedFields("f0.f0 -> *")
public class SelectImportVertexId
  implements KeySelector<Tuple2<ImportVertex<String>, GradoopId>, String> {
  @Override
  public String getKey(Tuple2<ImportVertex<String>, GradoopId> value) throws Exception {
    return value.f0.getId();
  }
}
