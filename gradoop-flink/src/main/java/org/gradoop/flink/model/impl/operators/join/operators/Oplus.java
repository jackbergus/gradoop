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

package org.gradoop.flink.model.impl.operators.join.operators;

import org.gradoop.common.model.api.entities.EPGMElement;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.api.functions.Function;

import java.util.HashSet;
import java.util.function.Supplier;

/**
 * Created by Giacomo Bergami on 30/01/17.
 */
public interface  Oplus<K extends EPGMElement> extends Function<K,Function<K,K>> {

  K supplyEmpty();
  String concatenateLabels(String labelLeft, String labelRight);

  public static <K extends EPGMElement> Oplus<K> generate(Supplier<K> supplier, Function<String,
    Function<String,String>> labelConcatenation) {
    return new Oplus<K>() {
      @Override
      public K supplyEmpty() {
        return supplier.get();
      }

      @Override
      public String concatenateLabels(String labelLeft, String labelRight) {
        return labelConcatenation.apply(labelLeft).apply(labelRight);
      }

    };
  }

  @Override
  default Function<K, K> apply(final K left) {
    return new Function<K, K>() {
      @Override
      public K apply(final K right) {
        K toret = supplyEmpty();
        toret.setId(GradoopId.get());
        toret.setLabel(concatenateLabels(left.getLabel(),right.getLabel()));
        HashSet<String> ll = new HashSet<String>();
        left.getProperties().getKeys().forEach(ll::add);
        HashSet<String> rr = new HashSet<String>();
        right.getProperties().getKeys().forEach(rr::add);
        ll.retainAll(rr);
        for (String x : left.getProperties().getKeys()) {
          toret.setProperty(x,left.getPropertyValue(x));
        }
        for (String x : right.getProperties().getKeys()) {
          toret.setProperty(x,right.getPropertyValue(x));
        }
        return (toret);
      }
    };
  }


}
