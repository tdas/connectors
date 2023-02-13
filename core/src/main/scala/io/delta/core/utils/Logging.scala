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

package io.delta.core.utils

/**
 * Utility trait for classes that want to log data. Creates a SLF4J logger for the class and allows
 * logging messages at different levels using methods that only evaluate parameters lazily if the
 * log level is enabled.
 */
trait Logging {
  // scalastyle:off println

  protected def logInfo(msg: => String): Unit = {
    println("INFO: " + msg)
  }

  protected def logWarning(msg: => String): Unit = {
    println("WARN: " + msg)
  }

  protected def logError(msg: => String): Unit = {
    println("ERROR: " + msg)
  }

  // Log methods that take Throwables (Exceptions/Errors) too
  protected def logInfo(msg: => String, throwable: Throwable): Unit = {
    println(s"INFO: $msg - $throwable")
  }

  protected def logWarning(msg: => String, throwable: Throwable): Unit = {
    println(s"WARN: $msg - $throwable")
  }

  protected def logError(msg: => String, throwable: Throwable): Unit = {
    println(s"ERROR: $msg - $throwable")
  }
}
