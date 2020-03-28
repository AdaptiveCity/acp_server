package acp_server.console;

// *************************************************************************************************
// *************************************************************************************************
// *************************************************************************************************
// Console.java
//
// Author: Ian Lewis ijl20@cam.ac.uk
//
// Forms part of the 'acp_server' next-generation Adaptive City platform
//
// Provides an HTTP server that serves the system administrator, to view status from eventbus status messages
//
// Message format on the 'system status' eventbus address
//   { "module_name": "<e.g. feedhandler, msgfiler, or console>",
//     "module_id": <whatever unique identifier source instance has>,
//     "status": "UP" ,
//     "status_msg": "UP",
//     "status_amber_seconds": 35,   // optional - verticle has 
//     "status_red_seconds": 65,     // optional
//     "acp_ts": "1475138945.132"    // optional in source, will be added by Console if missing
//   }
//   where the status_amber-seconds and status_red_seconds are optional
// *************************************************************************************************
// *************************************************************************************************
// *************************************************************************************************

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Handler;

import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.http.HttpMethod;

import io.vertx.core.file.FileSystem;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.buffer.Buffer;

import io.vertx.core.json.JsonObject;
import io.vertx.core.json.JsonArray;

// vertx web
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.StaticHandler;

import java.io.*;
import java.time.*;
import java.time.format.*;

import io.vertx.core.json.JsonObject;
import io.vertx.core.json.JsonArray;

import acp_server.util.Log;
import acp_server.util.Constants;

public class Console extends AbstractVerticle {

    private final String VERSION = "1.10";
    
    public int LOG_LEVEL; // optional in config(), defaults to Constants.LOG_INFO

    private Integer HTTP_PORT; // from config()
    private String EB_SYSTEM_STATUS; // from config()
    private String EB_MANAGER; // from config()
    private String MODULE_NAME; // from config()
    private String MODULE_ID; // from config()

    private final int SYSTEM_STATUS_PERIOD = 8000; // publish status heartbeat every 8 s
    private final int SYSTEM_STATUS_AMBER_SECONDS = 15;
    private final int SYSTEM_STATUS_RED_SECONDS = 25;

    private String BASE_URI; // e.g. "/console", currently derived from MODULE_NAME in config
    private Log logger; // tfc module to handle logging

    // declare EventBus object
    private EventBus eb;

    // declare object to hold latest status message from each active module
    private StatusCache status_cache;
    
  @Override
  public void start(Future<Void> fut) throws Exception {

    if (!get_config())
          {
              Log.log_err("Console: "+MODULE_NAME+"."+ MODULE_ID + " failed to load initial config()");
              vertx.close();
              return;
          }

    logger = new Log(LOG_LEVEL);
    
    logger.log(Constants.LOG_INFO, MODULE_NAME+"."+MODULE_ID+": Version "+VERSION+" started on " +
                       EB_SYSTEM_STATUS+", port "+HTTP_PORT);

    BASE_URI = MODULE_NAME;
    
    eb = vertx.eventBus();
    
    HttpServer http_server = vertx.createHttpServer();

    Router router = Router.router(vertx);

    // general logging of get requests and initial hack filter
    router.route(HttpMethod.GET,"/*").handler(ctx -> {
            // wrap in 'try' block to filter out many hacking malformed URL requests
            try {
                // calling .absoluteURI is just a simple test that will give an exception for a malformed request
                String test =  ctx.request().absoluteURI();
                // so we only pass this request on to the next handler if call above hasn't thrown an exception
                ctx.next();
            } catch (Exception e) {
                // will do nothing here if ctx.request.absoluteURI() fails for current request
            }
        });
    
    // ************************************
    // create listener to system status eventbus address and
    // handler for GET from /console/status
    // ************************************

    // initialize object to hold all most recent status messages
    status_cache = new StatusCache();

    // subscribe to status updates on EventBus
    eb.consumer(EB_SYSTEM_STATUS, message -> {
            logger.log(Constants.LOG_DEBUG, MODULE_NAME+"."+MODULE_ID+": received status "+
                       message.body().toString());
            status_cache.add( new JsonObject(message.body().toString()) );
                });

    // **********************************************************************************************
    // here is the 'API' handler for /console/status which returns JSON packet of all status messages
    // **********************************************************************************************
    router.route(HttpMethod.GET, "/api/"+BASE_URI+"/status").handler( ctx -> {

        HttpServerResponse response = ctx.response();
        response.putHeader("content-type", "application/json");
        response.putHeader("Access-Control-Allow-Origin", "*");

        // build api JSON message including latest status values
        JsonObject jo = new JsonObject();
        jo.put("module_name", MODULE_NAME);
        jo.put("module_id", MODULE_ID);
        jo.put("status", status_cache.status());
        response.end(jo.toString());
    });


      // general logging of get requests that didn't match proper requests
      router.route(HttpMethod.GET,"/*").handler(ctx -> {
              logger.log(Constants.LOG_DEBUG, "Console GET request not matched for " + ctx.request().absoluteURI());
        });
    
    // connect router to http_server
      http_server.requestHandler(router).listen(HTTP_PORT);

      // send system status message from this module (i.e. to itself) immediately on startup, then periodically
      send_status();
      
    // send periodic "system_status" messages
      vertx.setPeriodic(SYSTEM_STATUS_PERIOD, id -> { send_status();  });

  } // end start()

    // send UP status to the EventBus
    private void send_status()
    {
        eb.publish(EB_SYSTEM_STATUS,
                 "{ \"module_name\": \""+MODULE_NAME+"\"," +
                   "\"module_id\": \""+MODULE_ID+"\"," +
                   "\"status\": \"UP\"," +
                   "\"status_msg\": \"UP\"," +
                   "\"status_amber_seconds\": "+String.valueOf( SYSTEM_STATUS_AMBER_SECONDS ) + "," +
                   "\"status_red_seconds\": "+String.valueOf( SYSTEM_STATUS_RED_SECONDS ) +
                 "}" );
    }
    
    // Load initialization global constants defining this module from config()
    private boolean get_config()
    {
        // config() values needed by all TFC modules are:
        // module.name e.g. "console"
        // module.id e.g. "A"
        // eb.system_status - String eventbus address for system status messages
        // eb.manager - evenbus address to subscribe to for system management messages

        MODULE_NAME = config().getString("module.name"); // "console"
        if (MODULE_NAME==null)
            {
                System.err.println("Console: no module.name in config()");
                return false;
            }
        
        MODULE_ID = config().getString("module.id"); // A, B, ...
        if (MODULE_ID==null)
            {
                System.err.println(MODULE_NAME+": no module.id in config()");
                return false;
            }

        // set logging level
        LOG_LEVEL = config().getInteger(MODULE_NAME+".log_level", 0);
        if (LOG_LEVEL==0)
            {
                LOG_LEVEL = Constants.LOG_INFO;
            }
        
        // common system status reporting address, e.g. for UP messages
        // picked up by Console
        EB_SYSTEM_STATUS = config().getString("eb.system_status");
        if (EB_SYSTEM_STATUS==null)
            {
                System.err.println(MODULE_NAME+"."+MODULE_ID+": no eb.system_status in config()");
                return false;
            }

        // system control address - commands are broadcast on this
        EB_MANAGER = config().getString("eb.manager");
        if (EB_MANAGER==null)
            {
                System.err.println(MODULE_NAME+"."+MODULE_ID+" no eb.manager in config()");
                return false;
            }

        // port for user browser access to this Rita
        HTTP_PORT = config().getInteger(MODULE_NAME+".http.port");
        if (HTTP_PORT==null)
            {
                System.err.println(MODULE_NAME+"."+MODULE_ID+": no "+MODULE_NAME+".http.port in config()");
                return false;
            }

        return true;
    }

    // ****************************************************************
    // Class to hold latest status messages from each reporting module
    // ****************************************************************
    class StatusCache {
        
        JsonArray status_messages;

        StatusCache() {
            status_messages = new JsonArray();
        }

        void add(JsonObject jo)
        {
            // delete the status of current module if that is in cache
            for (int i=0; i<status_messages.size(); i++)
                {
                    JsonObject msg = status_messages.getJsonObject(i);
                    if (msg.getString("module_name").equals(jo.getString("module_name")) &&
                        msg.getString("module_id").equals(jo.getString("module_id")))
                        {
                            status_messages.remove(i);
                        }
                }
            // add string UTC timestamp "acp_ts" to system status message if it is not already in there from source
            if (jo.getString("acp_ts","42")=="42") // check if we have an acp_ts property, if not then set one
                {
                    Instant now = Instant.now();
                    // The object sent i the messagebus will include "ts": utc_seconds
                    long utc_milliseconds = now.toEpochMilli();

                    // Built utc_ts as "<UTC Seconds>.<UTC Milliseconds>" for use in the filename
                    String utc_milli_string = String.valueOf(utc_milliseconds);  // ~UTC time in milliseconds
                    int utc_len = utc_milli_string.length();
                    String utc_ts = utc_milli_string.substring(0,utc_len-3)+"."+utc_milli_string.substring(utc_len-3,utc_len);

                    jo.put("acp_ts", utc_ts);
                }
            // now add latest status as received to the JsonArray
            status_messages.add(jo);
        }

        JsonArray status()
        {
            return status_messages;
        }
    }
} // end class Console
