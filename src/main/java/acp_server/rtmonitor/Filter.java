package acp_server.rtmonitor;

import java.util.*;
import java.time.*;
import java.time.format.*;
import io.vertx.core.json.JsonObject;
import io.vertx.core.json.JsonArray;
import acp_server.util.Constants;

import acp_server.util.Constants;
import acp_server.util.Position;
import acp_server.util.Log;

    // Client subscription filter e.g. { "test": "=", "key": "A>B", "value": "X" }
    class Filter {
        private Log logger;

        private String MODULE_NAME = "RTMonitor";
        private String MODULE_ID = "Monitor";

        public JsonObject filter_obj;

        Filter(JsonObject filter_obj)
        {
            logger = new Log(RTMonitor.LOG_LEVEL);

            this.filter_obj = filter_obj;
        }

        // Test a JsonObject records against this Filter
        public boolean test(JsonObject record)
        {
            // test can default to "="
            String test = filter_obj.getString("test");
            if (test == null)
            {
                test = "=";
            }

            switch (test)
            {
                case "=":
                    return test_equals(record);

                case "inside":
                    // return true if current record is 'inside' coordinates given in filter
                    return test_inside(record);

                case "in":
                    return test_in(record);

                default:
                    logger.log(Constants.LOG_WARN, MODULE_NAME+"."+MODULE_ID+
                        ": Filter.test '"+test+"' not recognised");
                    break;
            }
            return false;
        } // end Filter.test()

        // ************************************************************
        // e.g. { "test": "=", "key": "VehicleRef", "value": "SCNH-35224" }
        // ************************************************************
        private boolean test_equals(JsonObject record)
        {
            // Given a filter say { "test": "=", "key": "VehicleRef", "value": "SCNH-35224" }
            String key = filter_obj.getString("key");
            if (key == null)
            {
                return false;
            }

            //DEBUG TODO allow numeric value
            String value = filter_obj.getString("value");
            if (value == null)
            {
                return false;
            }

            // Try and pick out the property "key" from the data record
            String record_value = record.getString(key);
            if (record_value == null)
            {
                return false;
            }

            //DEBUG TODO allow different tests than just "="
            return record_value.equals(value);
        } // end Filter.test_equals()

        // ************************************************************
        // Test for record inside polygon
        // ************************************************************
        private boolean test_inside(JsonObject record)
        {
            // Example filter:
            //   { "test": "inside",
            //     "lat_key": "Latitude",
            //     "lng_key": "Longitude",
            //     "points": [
            //         {  "lat": 52.21411510, "lng": 0.09916394948 },
            //         {  "lat": 52.20885583, "lng": 0.14877408742 },
            //         {  "lat": 52.19170630, "lng": 0.13778775930 },
            //         {  "lat": 52.19496839, "lng": 0.10053724050 }
            //     ]
            //   }

            //DEBUG TODO move this into the Subscription constructor for speedup
            String lat_key = filter_obj.getString("lat_key", "acp_lat");

            String lng_key = filter_obj.getString("lng_key", "acp_lng");

            JsonArray points = filter_obj.getJsonArray("points");

            ArrayList<Position> polygon = new ArrayList<Position>();

            for (int i=0; i<points.size(); i++)
            {
                polygon.add(new Position(points.getJsonObject(i)));
            }

            double lat;
            double lng;

            try
            {
                lat = get_double(record, lat_key);

                lng = get_double(record, lng_key);
            }
            catch (Exception e)
            {
                return false;
            }

            Position pos = new Position(lat,lng);

            // ah, all ready, now we can call the 'inside' test of the Position.
            return pos.inside(polygon);

        } // end Filter.test_inside()

        // **************************************************************************
        // Test if key value is in list of strings
        // { "test": "in", "key": "acp_id", "values": [ "elsys-eye-044504" ... ] }
        // **************************************************************************
        private boolean test_in(JsonObject record)
        {
            String key = filter_obj.getString("key");
            if (key == null)
            {
                return false;
            }

            // Try and pick out the property "key" from the data record
            String record_key = record.getString(key);
            if (record_key == null)
            {
                return false;
            }

            JsonArray values = filter_obj.getJsonArray("values");

            for (int i=0; i<values.size(); i++)
            {
                if (record_key.equals(values.getString(i)))
                {
                    return true;
                }
            }
            return false;
        }

        // Get a 'double' from the data record property 'key'
        // e.g. return the value of a "Latitude" property.
        // Note this could be a string or a number...
        private double get_double(JsonObject record, String key)
        {
            try
            {
                return record.getDouble(key);
            }
            catch (ClassCastException e)
            {
                return Double.parseDouble(record.getString(key));
            }
            //throw new Exception("get_double failed to parse number");
        }

    } // end class Filter
