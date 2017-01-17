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

package es.alvsanand.sgc.google.dcm_data_transfer

import java.io.Serializable
import java.text.SimpleDateFormat

import scala.util.Try

/**
  * This object contains all the DoubleClick Data Transfer file type. For more info see
  * [[https://developers.google.com/doubleclick-advertisers/udt/overview]].
  */
object DataTransferFileTypes {

  /**
    * Obtain the type of DoubleClick Data Transfer file.
    * @param file The name of the file.
    * @return The type of DoubleClick Data Transfer file. None if the file cannot be parsed.
    */
  def getType(file: String): Option[DataTransferFileType] = file match {
    case "ACTIVITY" => Option(ACTIVITY)
    case "CLICK" => Option(CLICK)
    case "IMPRESSION" => Option(IMPRESSION)
    case "MATCH_TABLE_ACTIVITY_CATS" => Option(MATCH_TABLE_ACTIVITY_CATS)
    case "MATCH_TABLE_ACTIVITY_TYPES" => Option(MATCH_TABLE_ACTIVITY_TYPES)
    case "MATCH_TABLE_AD_PLACEMENT_ASSIGNMENTS" => Option(MATCH_TABLE_AD_PLACEMENT_ASSIGNMENTS)
    case "MATCH_TABLE_ADS" => Option(MATCH_TABLE_ADS)
    case "MATCH_TABLE_ADVERTISERS" => Option(MATCH_TABLE_ADVERTISERS)
    case "MATCH_TABLE_BROWSERS" => Option(MATCH_TABLE_BROWSERS)
    case "MATCH_TABLE_CAMPAIGNS" => Option(MATCH_TABLE_CAMPAIGNS)
    case "MATCH_TABLE_CITIES" => Option(MATCH_TABLE_CITIES)
    case "MATCH_TABLE_CREATIVE_AD_ASSIGNMENTS" => Option(MATCH_TABLE_CREATIVE_AD_ASSIGNMENTS)
    case "MATCH_TABLE_CREATIVES" => Option(MATCH_TABLE_CREATIVES)
    case "MATCH_TABLE_CUSTOM_CREATIVE_FIELDS" => Option(MATCH_TABLE_CUSTOM_CREATIVE_FIELDS)
    case "MATCH_TABLE_CUSTOM_FLOODLIGHT_VARIABLES" => {
      Option(MATCH_TABLE_CUSTOM_FLOODLIGHT_VARIABLES)
    }
    case "MATCH_TABLE_CUSTOM_RICH_MEDIA" => Option(MATCH_TABLE_CUSTOM_RICH_MEDIA)
    case "MATCH_TABLE_DESIGNATED_MARKET_AREAS" => Option(MATCH_TABLE_DESIGNATED_MARKET_AREAS)
    case "MATCH_TABLE_KEYWORD_VALUE" => Option(MATCH_TABLE_KEYWORD_VALUE)
    case "MATCH_TABLE_OPERATING_SYSTEMS" => Option(MATCH_TABLE_OPERATING_SYSTEMS)
    case "MATCH_TABLE_PAID_SEARCH" => Option(MATCH_TABLE_PAID_SEARCH)
    case "MATCH_TABLE_PLACEMENT_COST" => Option(MATCH_TABLE_PLACEMENT_COST)
    case "MATCH_TABLE_PLACEMENTS" => Option(MATCH_TABLE_PLACEMENTS)
    case "MATCH_TABLE_SITES" => Option(MATCH_TABLE_SITES)
    case "MATCH_TABLE_STATES" => Option(MATCH_TABLE_STATES)
    case _ => None
  }

  /**
    * Obtain a DataTransferSlot of DoubleClick Data Transfer file.
    * @param file The name of the file.
    * @return The DataTransferSlot. None if the file cannot be parsed.
    */
  def getDataTransferFile(file: String): Option[DataTransferSlot] =
    getTypeAndDate(file) match {
      case Some((t: DataTransferFileType, d: String)) => {
        val date = Try((new SimpleDateFormat(t.dateFormat)).parse(d)).toOption

        date match {
          case Some(d) => Option(new DataTransferSlot(file, d, Option(t)))
          case None => None
        }
      }
      case _ => None
    }

  /**
    * Obtain the Data Transfer file type and the date.
    * @param file The name of the file.
    * @return The Data Transfer file type and the date. None if the file cannot be parsed.
    */
  private def getTypeAndDate(file: String): Option[(DataTransferFileType, String)] =
    file match {
      case ACTIVITY.regex(d) => Option(ACTIVITY, d)
      case CLICK.regex(d) => Option(CLICK, d)
      case IMPRESSION.regex(d) => Option(IMPRESSION, d)
      case MATCH_TABLE_ACTIVITY_CATS.regex(d) => Option(MATCH_TABLE_ACTIVITY_CATS, d)
      case MATCH_TABLE_ACTIVITY_TYPES.regex(d) => Option(MATCH_TABLE_ACTIVITY_TYPES, d)
      case MATCH_TABLE_AD_PLACEMENT_ASSIGNMENTS.regex(d) => {
        Option(MATCH_TABLE_AD_PLACEMENT_ASSIGNMENTS, d)
      }
      case MATCH_TABLE_ADS.regex(d) => Option(MATCH_TABLE_ADS, d)
      case MATCH_TABLE_ADVERTISERS.regex(d) => Option(MATCH_TABLE_ADVERTISERS, d)
      case MATCH_TABLE_BROWSERS.regex(d) => Option(MATCH_TABLE_BROWSERS, d)
      case MATCH_TABLE_CAMPAIGNS.regex(d) => Option(MATCH_TABLE_CAMPAIGNS, d)
      case MATCH_TABLE_CITIES.regex(d) => Option(MATCH_TABLE_CITIES, d)
      case MATCH_TABLE_CREATIVE_AD_ASSIGNMENTS.regex(d) => {
        Option(MATCH_TABLE_CREATIVE_AD_ASSIGNMENTS, d)
      }
      case MATCH_TABLE_CREATIVES.regex(d) => Option(MATCH_TABLE_CREATIVES, d)
      case MATCH_TABLE_CUSTOM_CREATIVE_FIELDS.regex(d) => {
        Option(MATCH_TABLE_CUSTOM_CREATIVE_FIELDS, d)
      }
      case MATCH_TABLE_CUSTOM_FLOODLIGHT_VARIABLES.regex(d) => {
        Option(MATCH_TABLE_CUSTOM_FLOODLIGHT_VARIABLES, d)
      }
      case MATCH_TABLE_CUSTOM_RICH_MEDIA.regex(d) => Option(MATCH_TABLE_CUSTOM_RICH_MEDIA, d)
      case MATCH_TABLE_DESIGNATED_MARKET_AREAS.regex(d) => {
        Option(MATCH_TABLE_DESIGNATED_MARKET_AREAS, d)
      }
      case MATCH_TABLE_KEYWORD_VALUE.regex(d) => Option(MATCH_TABLE_KEYWORD_VALUE, d)
      case MATCH_TABLE_OPERATING_SYSTEMS.regex(d) => Option(MATCH_TABLE_OPERATING_SYSTEMS, d)
      case MATCH_TABLE_PAID_SEARCH.regex(d) => Option(MATCH_TABLE_PAID_SEARCH, d)
      case MATCH_TABLE_PLACEMENT_COST.regex(d) => Option(MATCH_TABLE_PLACEMENT_COST, d)
      case MATCH_TABLE_PLACEMENTS.regex(d) => Option(MATCH_TABLE_PLACEMENTS, d)
      case MATCH_TABLE_SITES.regex(d) => Option(MATCH_TABLE_SITES, d)
      case MATCH_TABLE_STATES.regex(d) => Option(MATCH_TABLE_STATES, d)
      case _ => None
    }

  /**
    * This class represents a DoubleClick Data Transfer file type
    * @param name The name of type
    * @param regex The regex used to obtain the type and date
    * @param dateFormat The regex used to parse the date
    */
  sealed abstract class DataTransferFileType
  (val name: String,
   val regex: scala.util.matching.Regex,
   val dateFormat: String) extends Serializable

  case object ACTIVITY
    extends DataTransferFileType("activity",
      ".*dcm_account[0-9]+_activity_([0-9]{8})_.*".r,
      "yyyyMMdd")

  case object CLICK
    extends DataTransferFileType("click",
      ".*dcm_account[0-9]+_click_([0-9]{10})_.*".r,
      "yyyyMMddHH")

  case object IMPRESSION
    extends DataTransferFileType("impression",
      ".*dcm_account[0-9]+_impression_([0-9]{10})_.*".r,
      "yyyyMMddHH")

  case object MATCH_TABLE_ACTIVITY_CATS
    extends DataTransferFileType("match_table_activity_cats",
      ".*dcm_account[0-9]+_match_table_activity_cats_([0-9]{8})_.*".r,
      "yyyyMMdd")

  case object MATCH_TABLE_ACTIVITY_TYPES
    extends DataTransferFileType("match_table_activity_types",
      ".*dcm_account[0-9]+_match_table_activity_types_([0-9]{8})_.*".r,
      "yyyyMMdd")

  case object MATCH_TABLE_AD_PLACEMENT_ASSIGNMENTS
    extends DataTransferFileType("match_table_ad_placement_assignments",
      ".*dcm_account[0-9]+_match_table_ad_placement_assignments_([0-9]{8})_.*".r,
      "yyyyMMdd")

  case object MATCH_TABLE_ADS
    extends DataTransferFileType("match_table_ads",
      ".*dcm_account[0-9]+_match_table_ads_([0-9]{8})_.*".r,
      "yyyyMMdd")

  case object MATCH_TABLE_ADVERTISERS
    extends DataTransferFileType("match_table_advertisers",
      ".*dcm_account[0-9]+_match_table_advertisers_([0-9]{8})_.*".r,
      "yyyyMMdd")

  case object MATCH_TABLE_BROWSERS
    extends DataTransferFileType("match_table_browsers",
      ".*dcm_account[0-9]+_match_table_browsers_([0-9]{8})_.*".r,
      "yyyyMMdd")

  case object MATCH_TABLE_CAMPAIGNS
    extends DataTransferFileType("match_table_campaigns",
      ".*dcm_account[0-9]+_match_table_campaigns_([0-9]{8})_.*".r,
      "yyyyMMdd")

  case object MATCH_TABLE_CITIES
    extends DataTransferFileType("match_table_cities",
      ".*dcm_account[0-9]+_match_table_cities_([0-9]{8})_.*".r,
      "yyyyMMdd")

  case object MATCH_TABLE_CREATIVE_AD_ASSIGNMENTS
    extends DataTransferFileType("match_table_creative_ad_assignments",
      ".*dcm_account[0-9]+_match_table_creative_ad_assignments_([0-9]{8})_.*".r,
      "yyyyMMdd")

  case object MATCH_TABLE_CREATIVES
    extends DataTransferFileType("match_table_creatives",
      ".*dcm_account[0-9]+_match_table_creatives_([0-9]{8})_.*".r,
      "yyyyMMdd")

  case object MATCH_TABLE_CUSTOM_CREATIVE_FIELDS
    extends DataTransferFileType("match_table_custom_creative_fields",
      ".*dcm_account[0-9]+_match_table_custom_creative_fields_([0-9]{8})_.*".r,
      "yyyyMMdd")

  case object MATCH_TABLE_CUSTOM_FLOODLIGHT_VARIABLES
    extends DataTransferFileType("match_table_custom_floodlight_variables",
      ".*dcm_account[0-9]+_match_table_custom_floodlight_variables_([0-9]{8})_.*".r,
      "yyyyMMdd")

  case object MATCH_TABLE_CUSTOM_RICH_MEDIA
    extends DataTransferFileType("match_table_custom_rich_media",
      ".*dcm_account[0-9]+_match_table_custom_rich_media_([0-9]{8})_.*".r,
      "yyyyMMdd")

  case object MATCH_TABLE_DESIGNATED_MARKET_AREAS
    extends DataTransferFileType("match_table_designated_market_areas",
      ".*dcm_account[0-9]+_match_table_designated_market_areas_([0-9]{8})_.*".r,
      "yyyyMMdd")

  case object MATCH_TABLE_KEYWORD_VALUE
    extends DataTransferFileType("match_table_keyword_value",
      ".*dcm_account[0-9]+_match_table_keyword_value_([0-9]{8})_.*".r,
      "yyyyMMdd")

  case object MATCH_TABLE_OPERATING_SYSTEMS
    extends DataTransferFileType("match_table_operating_systems",
      ".*dcm_account[0-9]+_match_table_operating_systems_([0-9]{8})_.*".r,
      "yyyyMMdd")

  case object MATCH_TABLE_PAID_SEARCH
    extends DataTransferFileType("match_table_paid_search",
      ".*dcm_account[0-9]+_match_table_paid_search_([0-9]{8})_.*".r,
      "yyyyMMdd")

  case object MATCH_TABLE_PLACEMENT_COST
    extends DataTransferFileType("match_table_placement_cost",
      ".*dcm_account[0-9]+_match_table_placement_cost_([0-9]{8})_.*".r,
      "yyyyMMdd")

  case object MATCH_TABLE_PLACEMENTS
    extends DataTransferFileType("match_table_placements",
      ".*dcm_account[0-9]+_match_table_placements_([0-9]{8})_.*".r,
      "yyyyMMdd")

  case object MATCH_TABLE_SITES
    extends DataTransferFileType("match_table_sites",
      ".*dcm_account[0-9]+_match_table_sites_([0-9]{8})_.*".r,
      "yyyyMMdd")

  case object MATCH_TABLE_STATES
    extends DataTransferFileType("match_table_states",
      ".*dcm_account[0-9]+_match_table_states_([0-9]{8})_.*".r,
      "yyyyMMdd")
}
