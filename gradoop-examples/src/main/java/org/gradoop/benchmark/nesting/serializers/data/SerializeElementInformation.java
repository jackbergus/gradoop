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
package org.gradoop.benchmark.nesting.serializers.data;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.common.model.api.entities.EPGMElement;
import org.gradoop.common.model.impl.properties.Property;

import java.util.Iterator;

/**
 * Serializing a vertex in a proper way
 *
 * @param <VH> Representing either a Vertex or a GraphHead
 */
public class SerializeElementInformation<VH extends EPGMElement> implements MapFunction<VH, String> {

  /**
   * Reusable builder
   */
  private StringBuilder sb;

  /**
   * Default constructor
   */
  public SerializeElementInformation() {
    sb = new StringBuilder();
  }

  @Override
  public String map(VH vertex) throws Exception {
    sb.setLength(0);
    sb.append(vertex.getId().toString())
      .append(',')
      .append(vertex.getLabel() == null ? "" : ("\"" + vertex.getLabel() + "\""));
    if (vertex.getProperties() != null) {
      Iterator<Property> properties = vertex.getProperties().iterator();
      while (properties.hasNext()) {
        Property p = properties.next();
        sb.append(',')
          .append(p.getKey())
          .append(',')
          .append(p.getValue());
      }
    }
    return sb.toString();
  }

}