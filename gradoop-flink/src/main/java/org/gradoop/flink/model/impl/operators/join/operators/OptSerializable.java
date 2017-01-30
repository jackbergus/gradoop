package org.gradoop.flink.model.impl.operators.join.operators;

import java.io.Serializable;
import java.util.Objects;

/**
 * Created by Giacomo Bergami on 30/01/17.
 */
public class OptSerializable<K extends Serializable> {
  public final boolean isThereElement;
  public final K elem;

  public OptSerializable(boolean isThereElement, K elem) {
    this.isThereElement = isThereElement;
    this.elem = isThereElement ? elem : null;
  }

  public K get()  {
    return this.elem;
  }

  public boolean isPresent() {
    return isThereElement;
  }

  public static <K extends Serializable> OptSerializable<K> empty() {
    return new OptSerializable<K>(false,null);
  }

  public static <K extends Serializable> OptSerializable<K> value(K val) {
    return new OptSerializable<K>(true,val);
  }

  @Override
  public int hashCode() {
    return elem!=null ? elem.hashCode() : 0;
  }

  @Override
  public boolean equals(Object o) {
    if (o==null) return !isThereElement;
    else {
      if (o.equals(elem)) return true;
      else if (o instanceof OptSerializable) {
        OptSerializable<K> tr = (OptSerializable<K>) o;
        return tr != null && Objects.equals(tr.elem, elem) && isThereElement == tr.isThereElement;
      } else return false;
    }
  }

}
