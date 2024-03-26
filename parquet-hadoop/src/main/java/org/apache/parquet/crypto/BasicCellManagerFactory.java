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
package org.apache.parquet.crypto;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.column.CellManager;
import org.apache.parquet.column.impl.BasicCellManager;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.schema.MessageType;

/**
 * DO NOT USE FOR PRODUCTION - BasicCellManager encryption is not secure is meant
 * for example / testing purposes only.
 */
public class BasicCellManagerFactory implements CellManagerFactory {

  @Override
  public CellManager get(Configuration conf, MessageType fileSchema) {
    return new BasicCellManager();
  }

  @Override
  public CellManager get(Configuration conf, WriteSupport.WriteContext writeContext) {
    return new BasicCellManager();
  }
}
