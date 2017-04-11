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
package org.gradoop.benchmark.nesting.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.GraphHeadFactory;

/**
 * Created by vasistas on 08/04/17.
 */
@FunctionAnnotation.ForwardedFields("* -> f0")
public class AssociateFileToGraph
  implements MapFunction<String, Tuple3<String, Boolean, GraphHead>> {

  private final GraphHeadFactory ghf;
  private final Tuple3<String, Boolean, GraphHead> id;

  public AssociateFileToGraph(boolean isLeftOperand, GraphHeadFactory factory) {
    id = new Tuple3<>();
    ghf = factory;
    id.f1 = isLeftOperand;
  }

  @Override
  public Tuple3<String, Boolean, GraphHead> map(String value) throws Exception {
    id.f0 = value;
    id.f2 = ghf.createGraphHead();
    return id;
  }


}
