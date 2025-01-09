/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.util;

import java.util.Arrays;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

/**
 * This class encapsulates a byte array and overrides hashCode and equals so that it's identity is
 * based on the data rather than the array instance.
 *
 * HashedBytes通常是一个封装字节数组（byte array）的类，在 HBase 中，行键（row key）以字节数组的形式存在。
 * 为了高效地管理和比较这些字节数组，HBase 可能使用类似 HashedBytes 的类来封装并提供快速的哈希码计算和比较功能。
 * 使用这种封装类的好处是，能够更高效地管理哈希操作，
 * 比如避免直接对字节数组进行逐个字节比较，而是通过预先计算的哈希值进行快速比较。
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
public class HashedBytes {

  private final byte[] bytes;
  private final int hashCode;

  public HashedBytes(byte[] bytes) {
    this.bytes = bytes;
    hashCode = Bytes.hashCode(bytes);
  }

  public byte[] getBytes() {
    return bytes;
  }

  @Override
  public int hashCode() {
    return hashCode;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null || getClass() != obj.getClass()) return false;
    HashedBytes other = (HashedBytes) obj;
    return (hashCode == other.hashCode) && Arrays.equals(bytes, other.bytes);
  }

  @Override
  public String toString() {
    return Bytes.toStringBinary(bytes);
  }
}
