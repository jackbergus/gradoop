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

package org.gradoop.flink.model.impl.nested.operators;

import org.gradoop.flink.model.impl.nested.datastructures.IdGraphDatabase;

/**
 * Defines a binary operator
 */
public abstract class BinaryOp extends Op {

  /**
   * Public access to the internal operation. The data lake is not exposed
   * @param left    Left argument
   * @param right   Right argument
   * @return        Result as a graph with just ids. The DataLake, representing the computation
   *                state, is updated with either new vertices or new edges
   */
  public IdGraphDatabase with(IdGraphDatabase left, IdGraphDatabase right) {
    return runWithArgAndLake(mother, left, right);
  }

}