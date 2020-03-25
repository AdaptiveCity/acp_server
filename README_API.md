# acp_server verticals providing an API

Note the restful API functionality of the platform is provided by `acp_web` so this list is relatively short.

### /api/console/status

This returns the 'latest reported status' of each module running on the Rita platform. Note that if a module
silently dies then the last status will still be reported and it is the reponsibility of the *receiver* of this
data from the API to calculate how much time has passed since the status was reported (by subtracting the 'ts'
value from the current time) and acting upon that data appropriately.

For example, if the API returns a 'ts' value of 1476969294 for a given module status, and the current unix timestamp
is 1476969394, then it is 100 seconds since that status was originally reported.

Each module provides a 'status_red_seconds' (equiv. down)  and 'status_amber_seconds' (equiv warning) which indicate
reasonable thresholds after which an alarm is appropriate (as this may vary by module).

```
{
  "module_name":"console", // name of Rita module providing this API
  "module_id":"A",         // id of Rita module providing this API
  "status":[               // array of latest module status messages from currently running modules
             {
               "module_name":"rita", // name of Rita module with this status
               "module_id":"vix",    // id of Rita module with this status
               "status":"UP",        // current status, currently always "UP"
               "status_msg": "UP",   // (optional) status message which can be any text
               "status_amber_seconds":15, // how many seconds to allow the 'ts' to be aged before flagging as AMBER
               "status_red_seconds":25,   // how many seconds to allow the 'ts' to be aged before flagging as RED
               "ts":1476969294       // unix timestamp (seconds) when this status was received from this module
             },
             //... more individual module status records
           ]
}
```

