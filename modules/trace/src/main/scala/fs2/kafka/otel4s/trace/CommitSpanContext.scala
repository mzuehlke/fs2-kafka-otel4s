/*
 * Copyright 2026 Typelevel
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package fs2.kafka.otel4s.trace

/** Consumer-side metadata used to derive a tracing `commit` / `settle` span.
  *
  * This context describes a committed chunk of consumed records. It combines dynamic chunk information such as topics,
  * partitions, and record count with static consumer metadata captured from the bound Kafka consumer.
  */
sealed trait CommitSpanContext {

  /** Topics covered by the committed chunk.
    */
  def topics: Set[String]

  /** Partitions covered by the committed chunk.
    */
  def partitions: Set[Int]

  /** Number of records in the committed chunk.
    */
  def recordCount: Int

  /** Canonical string form of the Kafka message key when this span describes a single record and the key can be
    * represented safely.
    */
  def messageKey: Option[String]

  /** Kafka record offset when this span describes exactly one record.
    */
  def offset: Option[Long]

  /** Kafka `client.id` captured from the bound consumer, when configured.
    */
  def clientId: Option[String]

  /** Kafka `group.id` captured from the bound consumer, when configured.
    */
  def groupId: Option[String]

}

object CommitSpanContext {

  private[otel4s] def apply(
      topics: Set[String],
      partitions: Set[Int],
      recordCount: Int,
      messageKey: Option[String],
      offset: Option[Long],
      clientId: Option[String],
      groupId: Option[String]
  ): CommitSpanContext =
    Impl(
      topics,
      partitions,
      recordCount,
      messageKey,
      offset,
      clientId,
      groupId
    )

  final private case class Impl(
      topics: Set[String],
      partitions: Set[Int],
      recordCount: Int,
      messageKey: Option[String],
      offset: Option[Long],
      clientId: Option[String],
      groupId: Option[String]
  ) extends CommitSpanContext

}
