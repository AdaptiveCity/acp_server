package acp_server.msgfiler;

// *************************************************************************************************
// *************************************************************************************************
// *************************************************************************************************
// FilerUtils.java
// Version 0.12
// Author: Ian Lewis ijl20@cam.ac.uk
//
// Forms part of the 'tfc_server' next-generation Realtime Intelligent Traffic Analysis system
//
// FilerUtils provides the data storage procedures for MsgFiler and BatcherWorker
//
// Is configured via a config JsonObject which is stored as a FilerConfig
//   "source_address": the eventbus address to listen to for messages
//      e.g. "tfc.zone"
//   "flatten": the name of a JsonArray sub-field that is to be iterated into multiple messages
//   "records_data": a path to an array of data records within the message.
//   "source_filter" : a json object that specifies which subset of messages to write to disk
//      e.g. { "field": "msg_type", "compare": "=", "value": "zone_completion" }
//   "store_path" : a parameterized string giving the full filepath for storing the message
//      e.g. "/home/ijl20/tfc_server_data/data_zone/{{ts|yyyy}}/{{ts|MM}}/{{ts|dd}}"
//   "store_name" : a parameterized string giving the filename for storing the message
//      e.g. "{{module_id}}.txt"
//   "store_mode" : "write" | "append", defining whether the given file should be written or appended
//
//  In summary, "store_msg(msg)" will determine the data to be stored (with the most common
//  requirement being the whole message) and "build_string(pattern, msg)" will use config
//  parameters to create the required file_path and file_name.
//
// *************************************************************************************************
// *************************************************************************************************
// *************************************************************************************************

import java.time.*;
import java.time.format.*;
import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.core.json.JsonArray;
import io.vertx.core.file.FileSystem;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.file.FileSystemException;

import acp_server.util.Log;
import acp_server.util.Constants;

public class FilerUtils {

    private FilerConfig filer_config;

    private Vertx vertx;

    // If config has a "records_data" path to the data records, then
    // records_finder.get(msg) will return the required JsonArray in
    // the original message.
    private RecordsFinder records_finder;

    public FilerUtils (Vertx v, FilerConfig fc)
    {
        filer_config = fc;
        vertx = v;

        if (fc.records_data != null)
        {
            records_finder = new RecordsFinder(fc.records_data);
        }
    }

    // *************************************************************************************************
    // store_msg()
    // Store the message to the filesystem, either 
    // * as-is, i.e. whole received msg is stored.
    // * flattenin and iterating a defined base-level field (in filer_config.flatten, e.g.
    //   "request_data"). These records will be merged with the other proprties in the message.
    // * iterating records (i.e. array) at a defined path (in filer-config.records_data)
    // *************************************************************************************************
    public void store_msg(JsonObject msg)
    {
        // skip this message if if doesn't match the source_filter
        if (filer_config.source_filter != null && !(filer_config.source_filter.match(msg)))
            {
                return;
            }

        if (filer_config.flatten != null)
        {
            JsonArray flatten_array = msg.getJsonArray(filer_config.flatten);
            //System.out.println("MsgFiler."+filer_config.module_id+".FilerUtils store_msg(): flatten " +
            //               filer_config.flatten + " " +
            //               filer_config.store_mode + " " + 
            //               filer_config.store_path + " " + filer_config.store_name );

            // create a new JsonObject which is the original msg WITHOUT the field we'll flatten
            JsonObject flat_msg = msg.copy();
            flat_msg.remove(filer_config.flatten);

            // Iterate through the JsonObjects in the JsonArray field to flatten
            // and add those fields to the flat msg, creating immediate_msg to save
            for (int i=0; i<flatten_array.size(); i++)
            {
                // start with flat_msg, i.e. original excluding flatten_array
                JsonObject immediate_msg = flat_msg.copy();
                // merge in the fields from current element in flatten-array
                immediate_msg.mergeIn(flatten_array.getJsonObject(i));

                //System.out.println("will store "+immediate_msg.toString());
                store_immediate(immediate_msg);
            }
            //System.out.println("MsgFiler."+filer_config.module_id+": leaving store_msg\n");
        }
        else if (filer_config.records_data != null)
        {
            // Get the data records from the message
            JsonArray records = records_finder.get(msg);

            //System.out.println("MsgFiler."+filer_config.module_id+".FilerUtils store_msg(): records_data " +
            //               filer_config.records_data + ", " +
            //               filer_config.store_mode + ", " + 
            //               filer_config.store_path + "/" + filer_config.store_name );

            JsonObject base_msg = new JsonObject();

            if (filer_config.merge_base != null)
            {
                for (int i=0; i<filer_config.merge_base.size(); i++)
                {
                    String key = filer_config.merge_base.getString(i);
                    base_msg.put(key, msg.getValue(key));
                }
            }

            // Iterate through the JsonObjects in the JsonArray field
            for (int i=0; i<records.size(); i++)
            {
                // start with flat_msg, i.e. original excluding flatten_array
                JsonObject immediate_msg = records.getJsonObject(i);

                immediate_msg.mergeIn(base_msg);

                //System.out.println("will store "+immediate_msg.toString());
                store_immediate(immediate_msg);
            }
        }
        else // no flattening, so go ahead and store
        {
            store_immediate(msg);
        }
    }

    // Store the message as-is to the file system
    // This may be a flattened sub-record of the original message
    private void store_immediate(JsonObject msg)
    {
        //System.out.println("MsgFiler."+filer_config.module_id+": store_msg " +
        //                   filer_config.store_mode + " " + 
        //                   filer_config.store_path + " " + filer_config.store_name );
        //System.out.println(msg);

        // map the message values into the {{..}} placeholders in path and name
        String filepath;
        String filename;
        filepath = build_string(filer_config.store_path, msg);
        filename = build_string(filer_config.store_name, msg);

        //System.out.println("MsgFiler."+filer_config.module_id+": "+
        //                   filer_config.store_mode+ " " +filepath+"/"+filename);

        String msg_str = msg.toString();
        
        FileSystem fs = vertx.fileSystem();
        
        // if full directory path exists, then write file
        // otherwise create full path first
        
        fs.exists(filepath, result -> {
            if (result.succeeded() && result.result())
                {
                    //System.out.println("MsgFiler."+filer_config.module_id+": path "+filepath+" exists");
                    write_file(msg_str, filepath+"/"+filename, filer_config.store_mode);
                }
            else
                {
                    System.out.println("MsgFiler."+filer_config.module_id+": creating directory "+filepath);
                    fs.mkdirs(filepath, mkdirs_result -> {
                            if (mkdirs_result.succeeded())
                                {
                                    write_file(msg_str, filepath+"/"+filename, filer_config.store_mode);
                                }
                            else
                                {
                                    Log.log_err("MsgFiler."+filer_config.module_id+
                                                ": error creating path "+filepath);
                                }
                        });
                }
        });
    } // end store_msg()

    // *************************************************
    // store_msgBlocking()
    // Store the message SYNCHRONOUSLY to the filesystem
    // *************************************************
    //
    public void store_msgBlocking(JsonObject msg)
    {
        // skip this message if if doesn't match the source_filter
        if (filer_config.source_filter != null && !(filer_config.source_filter.match(msg)))
            {
                return;
            }
        //System.out.println("MsgFiler."+filer_config.module_id+": store_msgBlocking " +
        //                   filer_config.store_mode + " " + filer_config.store_path + " " + filer_config.store_name );
        //System.out.println(msg);

        // map the message values into the {{..}} placeholders in path and name

        String filepath = build_string(filer_config.store_path, msg);
        String filename = build_string(filer_config.store_name, msg);

        //System.out.println("MsgFiler."+filer_config.module_id+": "+filer_config.store_mode+ " " +filepath+"/"+filename);

        String msg_str = msg.toString();
        
        FileSystem fs = vertx.fileSystem();
        
        // if full directory path exists, then write file
        // otherwise create full path first
        
        if (fs.existsBlocking(filepath))
        {
            //System.out.println("MsgFiler."+filer_config.module_id+": path "+filepath+" exists");
            write_fileBlocking(msg_str, filepath+"/"+filename, filer_config.store_mode);
        }
        else
        {
            try {
                System.out.println("MsgFiler."+filer_config.module_id+": creating directory "+filepath);
                fs.mkdirsBlocking(filepath);
                write_fileBlocking(msg_str, filepath+"/"+filename, filer_config.store_mode);
            }
            catch (Exception e) {
                Log.log_err("MsgFiler."+filer_config.module_id+": error creating path "+filepath);
            }
        }

    } // end store_msg()

    // ************************************************************************************
    // build_string(String pattern, JsonObject msg)
    // ************************************************************************************
    // take a pattern and a message, and return the pattern populated with message values
    // e.g. "foo/bah/{{module_id}}" might become "foo/bah/zone_manager"
    // Patterns:
    //     {{<field_name>}}, populated via msg.getString(field_name)
    //     {{<field_name>|int}}, populated via msg.getLong(field_name)
    //     {{<field_name>|yyyy}}, get msg.getLong(field_name), parse it as a Unix timestamp, return year as "yyyy"
    //     {{<field_name>|MM}}, get msg.getLong(field_name), parse it as a Unix timestamp, return month as "MM"
    //     {{<field_name>|dd}}, get msg.getLong(field_name), parse it as a Unix timestamp, return day of month as "dd"
    private String build_string(String pattern, JsonObject msg)
    {
        final String PATTERN_START = "{{";
        final String PATTERN_END = "}}";
        
        int index = 0;
        String result = ""; // will hold the accumulated fully matched string
        
        while (index < pattern.length())
            {
                // get the indices of the start/end of the next {{..}}
                int pos_start = pattern.indexOf(PATTERN_START, index);
                int pos_end = pattern.indexOf(PATTERN_END, pos_start);
                // if pattern not found, then return string so far plus remainder
                if (pos_start < 0 || pos_end < 0)
                    {
                        result = result + pattern.substring(index);
                        return result;
                    }
                // we have a match for "{{..}}"
                // so accumulate the result up to the start of the "{{"
                // and find the value we have to replace "{{..}}" with
                result = result + pattern.substring(index, pos_start);
                // subst_pattern is the bit between the {{..}} e.g. "ts|yyyy"
                String subst_pattern = pattern.substring(pos_start + PATTERN_START.length(), pos_end);

                // filled_pattern is the value that should replace the {{..}} pattern e.g. "2016"
                String filled_pattern = fill_pattern(subst_pattern, msg);

                // add filled pattern to result so far
                result = result + filled_pattern;
                
                // move index along to just after the pattern
                index = pos_end + PATTERN_END.length();
            }
        return result;
    }

    // given a pattern and a msg, return the appropriate String
    // e.g. "ts|yyyy" -> "2016"
    // or "module_id" -> "zone"
    private String fill_pattern(String pattern, JsonObject msg)
    {
        final String PATTERN_FUN = "|";

        String field_name;

        // see if the pattern includes a function seperator, like "ts|yyyy"
        int fun_pos = pattern.indexOf(PATTERN_FUN);
        if (fun_pos < 0)
        {
            // simple case, no function separator, so just return msg field value
            return msg.getString(pattern);
        }
        else
        {
            field_name = pattern.substring(0, fun_pos);
        }

        // ok, we have a function to apply, so test each case

        if (pattern.endsWith(PATTERN_FUN+"int"))
        {
            Long field_value =  msg.getLong(field_name, 0L);
            return field_value.toString();
        }

        Instant ts;

        if (pattern.endsWith(PATTERN_FUN+"yyyy"))
        {
            ts = field_to_instant(msg, field_name);

            LocalDateTime local_time = LocalDateTime.ofInstant(ts, ZoneId.systemDefault());

            String year = local_time.format(DateTimeFormatter.ofPattern("yyyy"));

            return year;
        }

        if (pattern.endsWith(PATTERN_FUN+"MM"))
        {
            ts = field_to_instant(msg, field_name);

            LocalDateTime local_time = LocalDateTime.ofInstant(ts, ZoneId.systemDefault());

            String month = local_time.format(DateTimeFormatter.ofPattern("MM"));

            return month;
        }

        if (pattern.endsWith(PATTERN_FUN+"dd"))
        {
            ts = field_to_instant(msg, field_name);

            LocalDateTime local_time = LocalDateTime.ofInstant(ts, ZoneId.systemDefault());

            String day = local_time.format(DateTimeFormatter.ofPattern("dd"));

            return day;
        }
        return pattern;
    }

    // Convert EITHER unix timestamp or ISO 8601 string to an Instant
    private Instant field_to_instant(JsonObject msg, String field_name)
    {
        try
        {
            return  Instant.ofEpochSecond(msg.getLong(field_name, 0L));
        }
        catch (java.lang.ClassCastException e)
        {
            return Instant.parse(msg.getString(field_name));
        }
    }

    // *****************************************************************
    // write_file()
    // either overwrite (ASYNC) or append(SYNC) according to config_mode
    private void write_file(String msg, String file_path, String config_mode)
    {
        if (config_mode.equals(Constants.FILE_WRITE))
            {
                overwrite_file(msg, file_path);
            }
        else // append - this is a SYNCHRONOUS operation...
            {
                vertx.executeBlocking(fut -> {
                        append_file(msg, file_path);
                        fut.complete();
                    }, res -> { }
                    );
            }
    }        
        
    // *****************************************************************
    // write_fileBlocking()
    // either overwrite or append in SYNCHRONOUS mode
    private void write_fileBlocking(String msg, String file_path, String config_mode)
    {
        if (config_mode.equals(Constants.FILE_WRITE))
            {
                overwrite_fileBlocking(msg, file_path);
            }
        else // append - this is a SYNCHRONOUS operation...
            {
                append_file(msg, file_path); // is always SYNCHRONOUS anyway
            }
    }        
        
    // **********************************************************
    // overwrite_file()
    // will do an ASYNCHRONOUS operation, i.e. return immediately
    // Note: to extend the data API to 'now and previous' data sets,
    // this function SYNCHRONOUSLY moves any existing data file 'x' to 'x.prev'
    // before writing the new file.
    private void overwrite_file(String msg, String file_path)
    {
        FileSystem fs = vertx.fileSystem();

        // First we write the PREVIOUS file to file_path.prev
        // but skip errors if we can't delete the previous  .prev file (maybe this is the 1st)
        try
        {
             fs.deleteBlocking(file_path+Constants.PREV_FILE_SUFFIX);
        }
        catch (FileSystemException e)
        {
             // we tried to remove previous 'prev' file and failed, but it doesn't matter.
             //Log.log_err("MsgFiler."+filer_config.module_id+
             //              ": overwrite_file did not find existing previous file for "+file_path);
        }
        try
        {
             fs.moveBlocking(file_path, file_path+Constants.PREV_FILE_SUFFIX);
        }
        catch (FileSystemException e)
        {
             //Log.log_err("MsgFiler."+filer_config.module_id+
             //             ": overwrite_file did not find existing "+file_path);
        }

        Buffer buf = Buffer.buffer(msg);
        fs.writeFile(file_path, 
                     buf, 
                     result -> {
          if (result.succeeded()) {
              //System.out.println("MsgFiler: File "+file_path+" written");
          } else {
            Log.log_err("MsgFiler."+filer_config.module_id+": overwrite_file error ..." + result.cause());
          }
        });
    } // end overwrite_file

    // **********************************************************
    // overwrite_fileBlocking()
    // will do a SYNCHRONOUS operation
    private void overwrite_fileBlocking(String msg, String file_path)
    {
        FileSystem fs = vertx.fileSystem();
        Buffer buf = Buffer.buffer(msg);
        try {
            fs.writeFileBlocking(file_path, buf);
            //System.out.println("MsgFiler: File "+file_path+" written");
        } catch (Exception e) {
            Log.log_err("MsgFiler."+filer_config.module_id+": overwrite_fileBlocking error");
        }

    } // end overwrite_fileBlocking

    // *********************************************************************
    // append_file()
    // BLOCKING code that will open and append 'msg'+'\n' to file 'filepath'
    public void append_file(String msg, String file_path)
    {
        //System.out.println("MsgFiler."+filer_config.module_id+": append_file "+ file_path);
 
        BufferedWriter bw = null;

        try {
            // note FileWriter second arg 'true' => APPEND MODE
            bw = new BufferedWriter(new FileWriter(file_path, true));
            bw.write(msg);
            bw.newLine();
            bw.flush();
        } catch (IOException ioe) {
            Log.log_err("MsgFiler."+filer_config.module_id+": append_file failed for "+file_path);
        } finally {                       // always close the file
            if (bw != null) try {
                    bw.close();
                } catch (IOException ioe2) {
                    // just ignore it
                }
        } // end try/catch/finally

    } // end append_file

    // Helper class to provide JsonArray from source object given a records_data
    // e.g. records_data = "foo>request_data[0]>sites"
    // means get(source_object) will return the JsonArray at the location foo->request_data[0]->sites
    // where "foo" will be a JsonObject and "request_data" and "sites" must be JsonArrays (because
    // an index was provided for "request_data" and "sites" is the last element on the path.
    //
    // If the filter_path is simply "request_data", the .get(msg) method will simply return the 
    // JsonArray that must be present at that property.
    //
    class RecordsFinder {

        // The three types of things we can have on a Json path
        private int JSON_OBJECT = 0; // "foo": {obj}
        private int JSON_ARRAY = 1;  // "foo": [ obj,...]
        private int JSON_INDEXED_ARRAY = 2;  // "foo[0]"

        public String records_data;

        // path_steps contains the 'compiled' version of 
        private List<PathElement> path_steps;

        // Convert String records_data into a list of PathElements,
        // e.g. rd: "a>b[7]>c" becomes
        // OBJECT a
        // ARRAY b
        // INDEX 7
        // ARRAY c -- Note this is an ARRAY because it is the LAST element, otherwise it would be an OBJECT
        public RecordsFinder(String config_records_data)
        {
            records_data = config_records_data;

            String [] path_strings = records_data.split(">");

            path_steps = new ArrayList<PathElement>();

            for (int i=0; i<path_strings.length; i++)
            {
                path_steps.add(new PathElement(path_strings[i],i==path_strings.length-1));
            }

            System.out.println("MsgFiler.FilerUtils.RecordsFinder new: "+ records_data);
            System.out.println(toString());
        }

        public String toString()
        {
            String s = "";
            Iterator i = path_steps.iterator();
            while (i.hasNext())
            {
                s = s + i.next().toString()+" > ";
            }
            return s;
        }

        // Return the JsonArray at the end of "record_data" e.g. "request_data[0]>item>foo"
        public JsonArray get(JsonObject msg)
        {

            // We basically walk the Json object structure until we reach the end of path_steps.
            JsonObject current_object = msg;

            JsonArray current_array = new JsonArray();

            //Iterator i = path_steps.iterator();
            //while (i.hasNext())
            for (int i=0; i<path_steps.size(); i++)
            {
                //PathElement p = i.next();
                PathElement p = path_steps.get(i);

                if (p.element_type == JSON_OBJECT)
                {
                    //System.out.println("MsgFiler.FilerUtils.RecordsFinder get() OBJECT "+p.element_name);
                    current_object = current_object.getJsonObject(p.element_name);
                }
                else if (p.element_type == JSON_ARRAY)
                {
                    //System.out.println("MsgFiler.FilerUtils.RecordsFinder get() ARRAY "+p.element_name);
                    current_array = current_object.getJsonArray(p.element_name);
                }
                else if (p.element_type == JSON_INDEXED_ARRAY)
                {
                    // we currently support only a single index, e.g. "request_data[0]"
                    current_array = current_object.getJsonArray(p.element_name);
                    //System.out.println("MsgFiler.FilerUtils.RecordsFinder get() INDEXED_ARRAY "+p.element_name+"["+p.element_index+"]");
                    current_object = current_array.getJsonObject(p.element_index);
                }
            }
            return current_array;
        }


        // A PathElement is {object, name} | {array, name} | {index, number}
        // e.g.
        // foo => OBJECT unless last element in path
        // foo[7] => ARRAY followed by INDEX
        class PathElement {
            public int element_type; // JSON_OBJECT | JSON_ARRAY | JSON_INDEX

            public String element_name; // this is name of object/array
            public int element_index; // int value of array index
            public boolean element_indexed = false;

            public PathElement(String path_string, boolean last_element)
            {
                if (path_string.endsWith("]"))
                {
                    // e.g. foo[7] in a>foo[7]>b
                    element_type = JSON_INDEXED_ARRAY;

                    int open_bracket = path_string.indexOf('[');
                    int close_bracket = path_string.indexOf(']');
                    String array_index_string = path_string.substring(open_bracket+1,close_bracket);

                    element_index = Integer.parseInt(array_index_string); // "foo[7]" -> "7"

                    element_indexed = true;

                    element_name = path_string.substring(0,open_bracket); // "foo[7]" -> "foo"
                }
                else if (last_element)
                {
                    // e.g. "foo" from "a>b>foo"
                    element_type = JSON_ARRAY;
                    element_name = path_string;
                }
                else
                {
                    element_type = JSON_OBJECT;
                    element_name = path_string;
                }

            }

            public String toString()
            {
                if (element_type==JSON_OBJECT)
                {
                    return "OBJECT: " + element_name;
                }

                if (element_type == JSON_ARRAY)
                {
                    return "ARRAY: " + element_name;
                }

                if (element_type == JSON_INDEXED_ARRAY)
                {
                    return "INDEXED_ARRAY: "+ element_name + "["+element_index+"]";
                }

                return "MsgFiler.FilerUtils.RecordsFinder.PathElement.toString() BAD element_type";
            }

        } // end class PathElement

    } // end class RecordsFinder

} // end class FilerUtils
