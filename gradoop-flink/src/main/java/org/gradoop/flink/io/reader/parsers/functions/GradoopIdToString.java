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

package org.gradoop.flink.io.reader.parsers.functions;

import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.api.functions.Function;

/**
 * Mapping a GradoopId to a String
 */
public class GradoopIdToString implements Function<GradoopId, String> {
  @Override
  public String apply(GradoopId entity) {
    return entity.toString();
  }
}
