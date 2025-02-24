/*
 * Copyright (c) 2017-2019 TIBCO Software Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package io.snappydata.thrift.common;

import java.nio.ByteBuffer;

import org.apache.thrift.TException;

/**
 * Adds method to read binary value as a direct ByteBuffer if possible.
 */
public interface TProtocolDirectBinary {

  /**
   * Read a binary value as a direct ByteBuffer if possible.
   */
  ByteBuffer readDirectBinary() throws TException;
}
