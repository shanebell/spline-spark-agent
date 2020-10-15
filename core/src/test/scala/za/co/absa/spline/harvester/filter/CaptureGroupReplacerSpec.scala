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

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import za.co.absa.spline.harvester.filter.CaptureGroupReplacer._

class CaptureGroupReplacerSpec extends AnyFlatSpec with Matchers {

  it should "not modify the input when there is no match" in {
    val input = "some string"
    replace(input, Seq("""(\d+)""".r)) should be theSameInstanceAs input
  }

  it should "replace the capturing groups" in {
    replace("foo@bar.cz", Seq("""[^@]+@(\w+).\w+""".r)) shouldBe s"foo@${Replacement}.cz"

    replace(
      "ftp://username:pumba@hostname/",
      Seq("""ftp:\/\/[^:]+:([^@]+)@""".r)
    ) shouldBe s"ftp://username:${Replacement}@hostname/"
  }

  it should "replace only the first occurrence" in {
    val input = "some 42 string 66!"
    replace(input, Seq("""(\d+)""".r)) shouldBe s"some ${Replacement} string 66!"
  }

  it should "replace multiple capturing groups" in {
    val regexes = Seq("""@([^:]+):[\d]+:([^\s\/]+)""".r)
    val str = "something@pumba:34:timon/"

    replace(str, regexes) shouldBe s"something@${Replacement}:34:${Replacement}/"
  }

  it should "replace groups from multiple regexes" in {

    val regexes = Seq("""pumba=([^&\s]+)""".r, """timon=([^&\s]+)""".r)
    val str = "something&foo=42&pumba=lala&bar=66&timon=33"

    replace(str, regexes) shouldBe s"something&foo=42&pumba=${Replacement}&bar=66&timon=${Replacement}"
  }
}
