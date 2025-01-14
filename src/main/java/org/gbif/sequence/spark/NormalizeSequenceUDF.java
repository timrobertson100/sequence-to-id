/*
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
package org.gbif.sequence.spark;

import org.apache.spark.sql.api.java.UDF1;

/** Normalises a sequence based on https://github.com/gbif/pipelines/issues/1099. */
public class NormalizeSequenceUDF implements UDF1<String, String> {
  @Override
  public String call(String sequence) {
    if (sequence != null && sequence.length() > 0) {
      return sequence.toUpperCase().replaceAll("[^ACGTURYSWKMBDHVN]", "");
    } else return null;
  }
}
