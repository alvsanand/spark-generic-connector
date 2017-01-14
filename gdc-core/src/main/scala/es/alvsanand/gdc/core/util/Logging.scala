/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package es.alvsanand.gdc.core.util

import org.slf4j.{Logger, LoggerFactory}

/**
  * Simple trait that logs info data easing the task to the rest of classes.
  */
trait Logging {
  @transient private var log_ : Logger = null

  /**
    * Log a message with INFO level.
    * @param msg The message
    */
  protected def logInfo(msg: => String) {
    if (log.isInfoEnabled) log.info(msg)
  }

  /**
    * Log a message with DEBUG level.
    * @param msg The message
    */
  protected def logDebug(msg: => String) {
    if (log.isDebugEnabled) log.debug(msg)
  }

  /**
    * Log a message with TRACE level.
    * @param msg The message
    */
  protected def logTrace(msg: => String) {
    if (log.isTraceEnabled) log.trace(msg)
  }

  /**
    * Log a message with WARNING level.
    * @param msg The message
    */
  protected def logWarning(msg: => String) {
    if (log.isWarnEnabled) log.warn(msg)
  }

  /**
    * Internal method which creates an Logger based on the name of the class.
    * @return The Logger asociated to the class.
    */
  private def log: Logger = {
    if (log_ == null) {
      log_ = LoggerFactory.getLogger(logName)
    }
    log_
  }

  /**
    * Internal method which obtains the name of the class.
    * @return The name of the class
    */
  private def logName = {
    // Ignore trailing $'s in the class names for Scala objects
    this.getClass.getName.stripSuffix("$")
  }

  /**
    * Log a message with ERROR level.
    * @param msg The message
    */
  protected def logError(msg: => String) {
    if (log.isErrorEnabled) log.error(msg)
  }

  /**
    * Log a message with INFO level.
    * @param msg The message
    * @param throwable The Throwable
    */
  protected def logInfo(msg: => String, throwable: Throwable) {
    if (log.isInfoEnabled) log.info(msg, throwable)
  }

  /**
    * Log a message with DEBUG level.
    * @param msg The message
    * @param throwable The Throwable
    */
  protected def logDebug(msg: => String, throwable: Throwable) {
    if (log.isDebugEnabled) log.debug(msg, throwable)
  }

  /**
    * Log a message with TRACE level.
    * @param msg The message
    * @param throwable The Throwable
    */
  protected def logTrace(msg: => String, throwable: Throwable) {
    if (log.isTraceEnabled) log.trace(msg, throwable)
  }

  /**
    * Log a message with WARNING level.
    * @param msg The message
    * @param throwable The Throwable
    */
  protected def logWarning(msg: => String, throwable: Throwable) {
    if (log.isWarnEnabled) log.warn(msg, throwable)
  }

  /**
    * Log a message with ERROR level.
    * @param msg The message
    * @param throwable The Throwable
    */
  protected def logError(msg: => String, throwable: Throwable) {
    if (log.isErrorEnabled) log.error(msg, throwable)
  }
}
