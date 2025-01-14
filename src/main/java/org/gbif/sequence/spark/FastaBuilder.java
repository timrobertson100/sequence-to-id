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

import java.io.IOException;
import java.io.Serializable;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import lombok.Builder;

@Builder(toBuilder = true)
public class FastaBuilder implements Serializable {
  private final String hiveDB;
  private final String targetFile;

  public static void main(String[] args) throws IOException {
    FastaBuilder.builder().hiveDB("prod_h").targetFile("/tmp/sequences.fasta").build().run();
  }

  public void run() throws IOException {
    SparkSession spark =
        SparkSession.builder().appName("FASTA Builder").enableHiveSupport().getOrCreate();
    spark.sql("use " + hiveDB);
    spark.sparkContext().conf().set("hive.exec.compress.output", "false");
    spark.udf().register("normalize", new NormalizeSequenceUDF(), DataTypes.StringType);

    Dataset<Row> results =
        spark.sql(
            "SELECT normalize(dnasequence) AS seq, md5(normalize(dnasequence)) AS dnaSequenceID "
                + "FROM occurrence_ext_gbif_dnaderiveddata "
                + "WHERE dnasequence IS NOT NULL AND length(dnasequence)>0 "
                + "GROUP BY normalize(dnasequence), md5(normalize(dnasequence))");

    results.write().csv(targetFile);
  }
}
