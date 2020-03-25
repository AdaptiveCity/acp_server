package acp_server.feedmaker;

// ******************************************************************************************
// ******************************************************************************************
//
// ParseFeedXMLFlat
//
// See the README.md for the ACP vertx module FeedMaker.
//
// This class provides a 'parser' for FeedMaker to transform an XML file into a 'flat' JsonObject, i.e.
// all XML fields are replaced with Json Properties and the hierarchical structure discarded.
//
// Instantiated with ParseFeedXMLFlat(config, logger)
// where 'config' is the JsonObject from the vertx config() pertaining to the feed of this type (feed_xml_flat).
// That config will include:
//                                         "tag_record": "VehicleActivity",
//                                         "tag_map":    [ {"RecordedAtTime","acp_ts","datetime_utc_millis"},
//                                                         {"Latitude","acp_lat","float"},
//                                                         {"Longitude","acp_lng","float"},
//                                                         {"VehicleMonitoringRef","acp_id","string"}
//                                                       ],
// where
// 'tag_record' is the parent XML tag of the repeating data object of interest
// 'tag_map' is a set of 3-tuples requesting certain XML fields be mapped to new Json properties
// e.g. in this example <Latitude>44.123456</Latitude> will become { "acp_lat": 44.123456 }
//
// Supported formats include:
//    float: original value string is converted to a float
//    string: original value string is unchanged
//    datetime_utc_millis: original value (ISO string datetime) is converted to a UTC ISO string including milliseconds
//
// ******************************************************************************************
// ******************************************************************************************

import java.util.ArrayList;
import java.util.HashMap;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;

import io.vertx.core.json.JsonObject;
import io.vertx.core.json.JsonArray;
import io.vertx.core.buffer.Buffer;

// other tfc_server classes
import acp_server.util.Log;
import acp_server.util.Constants;

public class ParseFeedXMLFlat implements FeedParser {

    private String feed_type; // e.g. Constants.FEED_XML_FLAT

    private String area_id;

    private String tag_record;

    private JsonObject config;

    private Log logger;

    private HashMap<String, TagTransform> tag_map; // mappings of data fields e.g. "RecordedAtTime" -> "acp_ts"

    // Constructor

    ParseFeedXMLFlat(JsonObject config, Log logger)
    {
       this.config = config;

       this.feed_type = config.getString("feed_type");

       this.area_id = config.getString("area_id","");

       this.tag_record = config.getString("tag_record","");

       this.logger = logger;

       tag_map = new HashMap<String,TagTransform>();

       // Build tag_map HashMap so parser knows what tags to transform
       JsonArray config_map = config.getJsonArray("tag_map",new JsonArray());

       for (int i=0; i<config_map.size(); i++)
       {
           JsonObject tag_config = config_map.getJsonObject(i);

           tag_map.put(tag_config.getString("original_tag"), new TagTransform(tag_config));
       }

       logger.log(Constants.LOG_DEBUG, "ParseFeedXMLFlat started for feed_type "+feed_type+", "+
                  tag_map.size()+" tags to transform");
    }

    // Here is where we try and parse the page and return a JsonObject
    public JsonObject parse(Buffer buf)
    {

        String page = buf.toString();

        logger.log(Constants.LOG_DEBUG, "ParseFeedXMLFlat.parse() called for feed type "+feed_type);

        JsonArray records = new JsonArray();

        //logger.log(Constants.LOG_DEBUG, "ParseFeed xml_flat searching for "+tag_record);
        // <tag_record>..</tag_record> is the flattenable XML object that possibly repeats in the page 
        // cursor is our current position on the page as we step through parsing records
        int record_cursor = 0;
        // While we have some page left, continue parsing records
        while (record_cursor < page.length()) // this could be 'while (true)' as a 'break' should always occur anyway
        {
            //logger.log(Constants.LOG_DEBUG, "ParseFeed xml_flat searching for record from index "+record_cursor);
            // We will accumulate the flat Json from the XML into json_record
            // I.e. each XML <Foo>xyz</Foo>
            // becomes "Foo": "xyz"
            // and any nesting of XML objects is ignored.
            // This assumes the flattenable XML does NOT contain duplicate XML tags WITHIN records
            // although the records themselves can be repeated. This works for e.g. Siri-VM.
            JsonObject json_record = new JsonObject();
            // Move cursor forwards to the next occurrence of the tag_record
            record_cursor = page.indexOf("<"+tag_record+">", record_cursor);
            //logger.log(Constants.LOG_DEBUG, "ParseFeed xml_flat "+tag_record+" search result "+record_cursor);
            if (record_cursor < 0)
            {
                //logger.log(Constants.LOG_DEBUG, "ParseFeedXMLFlat.parse() no more "+tag_record+" records");
                // no more tag_record objects so finish
                break; // quit outermost records loop
            }
            int record_end = page.indexOf("</"+tag_record+">", record_cursor);
            if (record_end < 0)
            {
                // wtf, we got an opening tag_record but not a closing one, finish anyway
                logger.log(Constants.LOG_WARN, "ParseFeedXMLFlat.parse incomplete "+tag_record+" XML object");
                break; // quit outermost records loop
            }
            // Ok, we think we have a record between 'record_cursor' and 'record_end'
            // record_cursor is currently pointing at opening '<' of '<tag_record>'
            //logger.log(Constants.LOG_DEBUG, "ParseFeed xml_flat record at "+record_cursor);

            // Now loop within this tag_record object picking out the atomic objects <foo>X</foo>

            // Basic technique is to step through the tags, and only make a Json property out of
            // consecutive opening and closing tags that match.
            String current_tag = "";

            while (record_cursor < record_end)
            {
                // Searching forwards inside the 'tag_record' XML object
                // We will find the next <..> or </..> tag
                // Note we are moving the cursor forward each time at the earliest opportunity
                int next_cursor = page.indexOf("<", record_cursor);
                //logger.log(Constants.LOG_DEBUG, "ParseFeedXMLFlat.parse() next_cursor at "+next_cursor);
                // This could be the tag_record closing tag
                if (next_cursor >= record_end)
                {
                    //logger.log(Constants.LOG_DEBUG, "ParseFeedXMLFlat.parse() no more properties in this "+tag_record);
                    record_cursor = record_end;
                    break;
                }
                // tag could be <foo> or <foo route=66>, either way we want the "foo"
                // tag_close is index of the closing '>'
                int tag_close = page.indexOf(">", next_cursor);
                //logger.log(Constants.LOG_DEBUG, "ParseFeedXMLFlat.parse() tag_close at "+tag_close);
                if (tag_close < 0)
                {
                    // wtf, we got a '<' but no '>'
                    logger.log(Constants.LOG_WARN, "ParseFeedXMLFlat.parse() incomplete tag in "+tag_record+" XML object");
                    record_cursor = page.length(); // force completion of this page
                    break;
                }

                // We found '<'...'>' but if that's actually '<'...'/>' then skip this self-closed object
                if (page.substring(tag_close - 1, tag_close).equals("/"))
                {
                    logger.log(Constants.LOG_DEBUG, "ParseFeedXMLFlat.parse() "+
                               "skipping self-closed "+page.substring(record_cursor, tag_close+1));
                    record_cursor = tag_close;
                    break;
                }

                // See if we find a space character inside the tag (e.g. <foo route=66>)
                int tag_space = page.indexOf(" ", next_cursor);
                
                int tag_end = (tag_space > 0) && (tag_space < tag_close) ? tag_space : tag_close;
                //logger.log(Constants.LOG_DEBUG, "ParseFeedXMLFlat.parse() tag_end at "+tag_end);
                // Note we KNOW we at least have a '>' at the end of the tag_record object from the code above
                // So check the tag we found is still within the current tag_record object
                if (tag_end < record_end)
                {
                    // Given a '<'..'>' (but not '<'..'/>')
                    String next_tag = page.substring(++next_cursor, tag_end);
                    //logger.log(Constants.LOG_DEBUG, "ParseFeedXMLFlat.parse() "+
                    //           "found tag "+next_tag);
                    // Process tag here...
                    if (next_tag.equals("/"+current_tag))
                    {
                        //logger.log(Constants.LOG_DEBUG, "ParseFeedXMLFlat.parse() "+
                        //           "found atomic tag "+current_tag+".."+tag_close);
                        // *************************************************************************
                        // ************* OK HERE WE FOUND A TAG WITH A VALUE ***********************
                        // *************************************************************************
                        String current_value = page.substring(record_cursor+1, next_cursor-1);
                        // So name of the tag is 'current_tag'
                        // And the string value is 'current_value'

                        // Add a new property to the current json record with this tag/value
                        json_record.put(current_tag, current_value);

                        // Now we'll see if we want to add *another* 'standard' property 
                        // because there's an entry in the HashMap 'tag_map'

                        if (tag_map.containsKey(current_tag))
                        {
                            // Yup, this is a 'tag' of interest, so transform to a new Json property
                            JsonObject new_property = tag_map.get(current_tag).transform(current_value);
                            logger.log(Constants.LOG_DEBUG, "ParseFeedXMLFlat.parse() transformed "+
                                       current_tag+"/"+current_value+" to "+new_property);

                            // Now we add this new property to the json_record we're building
                            json_record.mergeIn(new_property);
                        }
                        // *************************************************************************
                        // *************************************************************************
                        // *************************************************************************
                    }

                    current_tag = next_tag;
                    record_cursor = tag_close;
                }
                else
                {
                    logger.log(Constants.LOG_DEBUG, "ParseFeedXMLFlat.parse() "+
                               "at record end");
                    record_cursor = record_end;
                } // end if
            } // end while loop for properties within current record

            //json_record.put("feed_data", page);
            // Add the current record to the 'records' result list
            records.add(json_record);
            // shift cursor to the end of the current record before we loop to look for the next record
            record_cursor = record_end;
        }
        logger.log(Constants.LOG_DEBUG, "ParseFeedXMLFlat parse() completed for "+records.size()+" records");

        JsonObject msg = new JsonObject();
        msg.put("request_data", records);
        return msg;

    } // end parse

    // This class is used to hold the mapping of an XML tag (e.g. "RecordedAtTime") to
    // a 'standard' property for this platform (i.e. "acp_ts")
    class TagTransform {

        String input_tag;

        String output_tag;

        String format;

        // constructor
        TagTransform(JsonObject tag_config)
        {
            this.input_tag = tag_config.getString("original_tag");
            this.output_tag = tag_config.getString("new_tag");
            this.format = tag_config.getString("format");
        }

        public JsonObject transform(String input_value)
        {
            JsonObject jo = new JsonObject();
            try {
                // here is where we transform the input value based on 'format'
                switch (format)
                    {
                    case "int":
                        jo.put(output_tag, Long.parseLong(input_value));
                        break;
                    case "float":
                        jo.put(output_tag, Double.parseDouble(input_value));
                        break;
                    case "datetime_iso_to_utc":
                        // input  "2017-09-29T09:45:38+01:00"
                        // output "2017-09-29T09:45:38.000Z"
                        String iso_ts = Instant.from(DateTimeFormatter.ISO_DATE_TIME.parse(input_value)).toString();
                        jo.put(output_tag, iso_ts);
                        break;
                    case "datetime_iso_to_int_utc_seconds":
                        // input  "2017-09-29T09:45:38+01:00"
                        // output "1507125081"
                        long ts_seconds = Instant.from(DateTimeFormatter.ISO_DATE_TIME.parse(input_value)).getEpochSecond();
                        jo.put(output_tag, ts_seconds);
                        break;
                    default:
                        jo.put(output_tag, input_value);
                        break;
                    }
            }
            catch (Exception e){;}
            return jo;
        }
    } // end TagTransform

} // end ParseFeedXMLFlat

