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

/** Controls how producer batches instrument records without recognized trace context in their Kafka headers.
  *
  * Existing valid record trace context is preserved in every mode.
  */
sealed abstract class BatchSpanMode

object BatchSpanMode {

  /** Creates one `PRODUCER` `create` span for each batch record without trace context in its headers.
    *
    * The batch `send` span is `CLIENT` and links to one record trace context per record. This is the default and the
    * higher-fidelity OpenTelemetry batch model.
    */
  case object PerRecordSpans extends BatchSpanMode

  /** Creates no per-record `create` spans.
    *
    * Records without trace context receive the batch `send` span's context. If at least one record receives that
    * context, the send span is `PRODUCER`; if every record already has a context, it remains `CLIENT`.
    */
  case object SharedSendSpan extends BatchSpanMode

}
