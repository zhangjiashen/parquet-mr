/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.parquet.io;

import java.util.LinkedList;

public class CellValueBuffer<T> {
  private final LinkedList<CellValue<T>> cellValues;

  public CellValueBuffer() {
    this.cellValues = new LinkedList<>();
  }

  public void addValue(T value, int repLevel, int defLevel) {
    CellValue<T> stateValue = new CellValue<>(value, repLevel, defLevel);
    cellValues.add(stateValue);
  }

  public CellValue<T> next() {
    return cellValues.removeFirst();
  }

  public boolean hasNext(){
    return !cellValues.isEmpty();
  }

  public CellValue<T> peek() {
    return cellValues.peek();
  }

  public static class CellValue<T> {
    private final T value;
    private final int defLevel;
    private final int repLevel;

    CellValue(T value, int repLevel, int defLevel) {
      this.value = value;
      this.repLevel = repLevel;
      this.defLevel = defLevel;
    }

    public int getRepLevel() {
      return repLevel;
    }

    public int getDefLevel() {
      return defLevel;
    }

    public T getValue() {
      return value;
    }
  }
}
