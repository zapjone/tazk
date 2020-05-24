package com

import java.util.Properties

/**
 *
 * @author zhangap 
 * @version 1.0, 2020/5/22
 *
 */
package object tazk {

  private object TazkBuildInfo {

    val (
      tazk_version: String,
      tazk_branch: String,
      tazk_revision: String,
      tazk_build_user: String,
      tazk_repo_url: String,
      tazk_build_date: String) = {

      val resourceStream = Thread.currentThread().getContextClassLoader.
        getResourceAsStream("tazk-version-info.properties")
      if (resourceStream == null) {
        throw new TazkException("Could not find tazk-version-info.properties")
      }

      try {
        val unknownProp = "<unknown>"
        val props = new Properties()
        props.load(resourceStream)
        (
          props.getProperty("version", unknownProp),
          props.getProperty("branch", unknownProp),
          props.getProperty("revision", unknownProp),
          props.getProperty("user", unknownProp),
          props.getProperty("url", unknownProp),
          props.getProperty("date", unknownProp)
        )
      } catch {
        case e: Exception =>
          throw new TazkException("Error loading properties from tazk-version-info.properties", e)
      } finally {
        if (resourceStream != null) {
          try {
            resourceStream.close()
          } catch {
            case e: Exception =>
              throw new TazkException("Error closing tazk build info resource stream", e)
          }
        }
      }
    }
  }

  val TAZK_VERSION = TazkBuildInfo.tazk_version
  val TAZK_BRANCH = TazkBuildInfo.tazk_branch
  val TAZK_REVISION = TazkBuildInfo.tazk_revision
  val TAZK_BUILD_USER = TazkBuildInfo.tazk_build_user
  val TAZK_REPO_URL = TazkBuildInfo.tazk_repo_url
  val TAZK_BUILD_DATE = TazkBuildInfo.tazk_build_date

}
