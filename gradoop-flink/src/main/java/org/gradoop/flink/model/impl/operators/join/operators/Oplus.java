package org.gradoop.flink.model.impl.operators.join.operators;

import org.gradoop.common.model.api.entities.EPGMElement;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.api.functions.Function;
import sun.jvm.hotspot.utilities.Assert;

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
