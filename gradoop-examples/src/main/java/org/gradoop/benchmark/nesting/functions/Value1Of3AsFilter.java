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

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.pojo.GraphHead;

/**
 * Returns either the elements pertaining to the left operand or the ones for the right
 * operand by using a default parameter.
 */
public class Value1Of3AsFilter implements FilterFunction<Tuple3<String, Boolean, GraphHead>> {

  /**
   * When the parameter is set to true, it defines if the mapping function will return
   * the left operand, the left one is returned otherwise.
   */
  private final boolean isLeftOperand;

  /**
   * Default parameter
   * @param isLeftOperand see {@code isLeftOperand}
   */
  public Value1Of3AsFilter(boolean isLeftOperand) {
    this.isLeftOperand = isLeftOperand;
  }

  @Override
  public boolean filter(Tuple3<String, Boolean, GraphHead> value) throws Exception {
    return value.f1 == isLeftOperand;
  }
}
