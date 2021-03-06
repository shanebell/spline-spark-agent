/*
 * Copyright 2019 ABSA Group Limited
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
package za.co.absa.spline.harvester.json

import org.apache.commons.lang3.StringUtils._
import org.json4s.TypeHints

case class SplineShortTypeHints(hints: List[Class[_]]) extends TypeHints {
  def hintFor(clazz: Class[_]): String = {
    val className = clazz.getName
    className.substring(1 + lastOrdinalIndexOf(className, ".", 2))
  }

  def classFor(hint: String): Option[Class[_]] = hints find (hintFor(_) == hint)
}