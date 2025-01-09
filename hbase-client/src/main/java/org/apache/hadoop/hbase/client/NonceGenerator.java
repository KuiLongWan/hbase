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
package org.apache.hadoop.hbase.client;

import org.apache.yetus.audience.InterfaceAudience;

/**
 * NonceGenerator interface. In general, nonce group is an ID (one per client, or region+client, or
 * whatever) that could be used to reduce collision potential, or be used by compatible server nonce
 * manager to optimize nonce storage and removal. See HBASE-3787.
 *
 * KLRD: 作用是生成唯一的随机数（nonce），用于防止重放攻击以及保证请求的幂等性。它主要用于以下几个场景：
 *   1.请求唯一性：在分布式系统中，可能会有多个客户端同时向服务器发送请求。
 *                为了避免重复请求导致的数据不一致或冲突，NonceGenerator 生成的唯一随机数可以确保每个请求都是独一无二的。
 *   2.幂等性保证：在网络通信过程中，某些请求可能会因为网络问题而被重复发送。
 *                为了保证请求的幂等性（即多次请求的结果相同），服务器端可以通过验证 nonce 是否已被使用来决定是否处理请求。
 *   3.防重放攻击：重放攻击是指攻击者截获网络请求并多次发送相同的请求。
 *                使用 nonce 可以确保每个请求只能被处理一次，从而防止重放攻击带来的风险。
 *
 * Nonce(Number once)：在密码学中Nonce是一个只被使用一次的任意或非重复的随机数值
 */
@InterfaceAudience.Private
public interface NonceGenerator {

  static final String CLIENT_NONCES_ENABLED_KEY = "hbase.client.nonces.enabled";

  /** Returns the nonce group (client ID) of this client manager. */
  long getNonceGroup();

  /** Returns New nonce. */
  long newNonce();
}
