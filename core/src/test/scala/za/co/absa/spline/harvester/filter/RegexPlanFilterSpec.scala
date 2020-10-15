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

import java.util.UUID

import org.apache.commons.io.FileUtils
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import za.co.absa.commons.io.TempFile
import za.co.absa.commons.lang.OptionImplicits.NonOptionWrapper
import za.co.absa.spline.harvester.filter.CaptureGroupReplacer.Replacement
import za.co.absa.spline.producer.model._

import scala.collection.JavaConverters._

class RegexPlanFilterSpec extends AnyFlatSpec with Matchers {

    val templateWrite = WriteOperation("http://source/1188999", Seq.empty, false, 42, List.empty, None, None)
    val templateRead = ReadOperation(List.empty, List("http://source/1188999", "src2"), 42, None, None, None)
    val templateData = DataOperation(42, None, None, None, None)

    def toPlan(
      write: WriteOperation,
      reads: List[ReadOperation] = List.empty,
      other: List[DataOperation] = List.empty
    ) = {
      val ops = Operations(write, reads.asOption, other.asOption)
      ExecutionPlan(UUID.randomUUID(), ops, SystemInfo("foo","bar"), None, None)
    }

    val digitRegex = """(\d+)""".r
    val digitFilter = new RegexPlanFilter(Seq(digitRegex), Seq(digitRegex), Seq(digitRegex))

    it should "filter out a secret in write source" in {
      val filteredPlan = digitFilter.filter(toPlan(templateWrite))

      filteredPlan.operations.write.outputSource shouldBe s"http://source/${Replacement}"
    }

    it should "filter out a secret in read source" in {
      val filteredPlan = digitFilter.filter(toPlan(templateWrite, List(templateRead)))

      val read = filteredPlan.operations.reads.get(0)
      read.inputSources(0) shouldBe s"http://source/${Replacement}"
      read.inputSources(1) shouldBe s"src${Replacement}"
    }

  it should "use regex by operation type" in {
    val myTmpFile = TempFile().deleteOnExit.path

    val lines = Seq(
      """WriteOperation :: \/\/(\w+)\/""",
      """DataOperation :: (:+)""",
      """DataOperation :: (%+)""",
      """ReadOperation :: (\d+)""",
      """:: (http)"""
    )

    FileUtils.writeLines(myTmpFile.toFile, lines.asJava)

    val filter = RegexPlanFilter(myTmpFile.toString)

    val dataOp = templateData.copy(params = Some(Map("foo" -> "a:b%c", "bar" -> "https")))
    val plan = toPlan(templateWrite, List(templateRead), List(dataOp))

    val filteredPlan = filter.filter(plan)

    filteredPlan.operations.write.outputSource shouldBe s"${Replacement}://${Replacement}/1188999"

    val read = filteredPlan.operations.reads.get(0)
    read.inputSources(0) shouldBe s"${Replacement}://source/${Replacement}"
    read.inputSources(1) shouldBe s"src${Replacement}"

    val data = filteredPlan.operations.other.get(0)
    data.params.get("foo") shouldBe s"a${Replacement}b${Replacement}c"
    data.params.get("bar") shouldBe s"${Replacement}s"

  }

  it should "handle several nested structures" in {

    val map = Map("a" -> true, "b" -> 45, "c" -> Some(Seq("foo45bar%%baz", "33x")), "d"-> Map(33 -> "B52"))
    val dataOp = templateData.copy(params = Some(map))
    val plan = toPlan(templateWrite, List(templateRead), List(dataOp))

    val filteredPlan = digitFilter.filter(plan)

    val data = filteredPlan.operations.other.get(0)
    val strings = data.params.get("c").asInstanceOf[Some[Seq[String]]].get
    strings(0) shouldBe s"foo${Replacement}bar"
    strings(1) shouldBe s"${Replacement}x"

    val b52Filtered = data.params.get("d").asInstanceOf[Map[Int, String]](33)
    b52Filtered shouldBe s"B${Replacement}"
  }
}
