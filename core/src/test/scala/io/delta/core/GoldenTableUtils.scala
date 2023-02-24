/*
 * Copyright (2020-present) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.delta.core

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration

object GoldenTableUtils {

  /**
   * Load the golden table as a class resource so that it works in IntelliJ and SBT tests.
   *
   * If this is causing a `java.lang.NullPointerException` while debugging in IntelliJ, you
   * probably just need to SBT test that specific test first.
   */
  val goldenTable = new File("../golden-tables/src/test/resources/golden").getCanonicalFile

  /**
   * Create the full table path for the given golden table and execute the test function.
   *
   * @param name The name of the golden table to load.
   * @param testFunc The test to execute which takes the full table path as input arg.
   */
  def withGoldenTable(name: String)(testFunc: String => Unit): Unit = {
    val tablePath = new File(goldenTable, name).getCanonicalPath
    testFunc(tablePath)
  }
}
