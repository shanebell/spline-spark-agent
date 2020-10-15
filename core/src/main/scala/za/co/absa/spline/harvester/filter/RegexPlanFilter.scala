/*
 * Copyright 2020 ABSA Group Limited
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

package za.co.absa.spline.harvester.filter
import za.co.absa.commons.lang.ARM
import za.co.absa.spline.harvester.filter.CaptureGroupReplacer._
import za.co.absa.spline.producer.model._

import scala.io.Source
import scala.util.matching.Regex

class RegexPlanFilter(writeRegexes: Seq[Regex], readRegexes: Seq[Regex], dataRegexes: Seq[Regex]) extends PlanFilter {

  override def filter(plan: ExecutionPlan): ExecutionPlan =
    plan.copy(operations = filterOperations(plan.operations))

  private def filterOperations(operations: Operations): Operations =
    Operations(
      filterWriteOp(operations.write),
      operations.reads.map(_.map(filterReadOp)),
      operations.other.map(_.map(filterDataOp))
    )

  private def filterWriteOp(writeOp: WriteOperation): WriteOperation =
    writeOp.copy(
      outputSource = replace(writeOp.outputSource, writeRegexes),
      params = writeOp.params.map(filterParams(_, writeRegexes)),
      extra = writeOp.extra.map(filterParams(_, readRegexes))
    )

  private def filterReadOp(readOp: ReadOperation): ReadOperation =
    readOp.copy(
      inputSources = readOp.inputSources.map(replace(_, readRegexes)),
      params = readOp.params.map(filterParams(_, readRegexes)),
      extra = readOp.extra.map(filterParams(_, readRegexes))
    )

  private def filterDataOp(dataOp: DataOperation): DataOperation =
    dataOp.copy(
      params = dataOp.params.map(filterParams(_, dataRegexes)),
      extra = dataOp.extra.map(filterParams(_, readRegexes))
    )

  private def filterParams(params: Map[String, Any], regexes: Seq[Regex]): Map[String, Any] =
    params.mapValues(filterAny(_, regexes))

  private def filterAny(value: Any, regexes: Seq[Regex]): Any = value match {
    case s: String => replace(s, regexes)
    case n: Number => n
    case b: Boolean => b
    case opt: Option[_] => opt.map(filterAny(_, regexes))
    case m: Map[_,_] => m.mapValues(filterAny(_, regexes))
    case seq: Traversable[_] => seq.map(filterAny(_, regexes))
    case t: Any => throw new UnsupportedOperationException(s"Handling for ${t.getClass} not implemented!")
  }
}

object RegexPlanFilter {

  def apply(regExDefinitionsPath: String) = {
    val regexMap = ARM.using(Source.fromFile(regExDefinitionsPath)) {
      bufferedSource =>
        bufferedSource.getLines.toSeq
          .map(lineToTypeAndRegex)
          .groupBy(_._1)
          .mapValues(_.map(_._2))
    }

    val commonRegexes = regexMap.getOrElse(FilterType.All, Seq.empty)
    val readRegexes = regexMap.getOrElse(FilterType.Read, Seq.empty) ++ commonRegexes
    val writeRegexes = regexMap.getOrElse(FilterType.Write, Seq.empty) ++ commonRegexes
    val dataRegexes = regexMap.getOrElse(FilterType.Data, Seq.empty) ++ commonRegexes

    new RegexPlanFilter(writeRegexes, readRegexes, dataRegexes)
  }

  private def lineToTypeAndRegex(line: String) = {
    val i = line.indexOf("::")
    val typeString = line.substring(0, i).trim
    val regexString = line.substring(i + 2, line.length).trim

    (FilterType.fromString(typeString), new Regex(regexString))
  }

}

