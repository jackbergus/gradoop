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

package org.gradoop.flink.io.reader.parsers.memetracker.functions;

import org.gradoop.flink.io.reader.parsers.functions.ToEdgesFromAdjList;
import org.gradoop.flink.io.reader.parsers.memetracker.MemeTrackerEdge;
import org.gradoop.flink.io.reader.parsers.memetracker.MemeTrackerRecordParser;

/**
 * Providing a concrete instantiation of the type parameters
 */
public class FromMemeAdjacencyToEdges extends ToEdgesFromAdjList<String, MemeTrackerEdge, MemeTrackerRecordParser> {
  /**
   * Default constructor
   */
  public FromMemeAdjacencyToEdges() {
    super(new MemeEdgeToEdge());
  }
}
