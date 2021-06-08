'use strict';

var async = require('async');

var redis = require('redis');
var redisClient = redis.createClient();

const STREAMS_KEY = "weather_sensor:wind";
const APPLICATION_ID = "iot_application:node_1";
var CONSUMER_ID = "consumer:2"
redisClient.xgroup("CREATE", STREAMS_KEY, APPLICATION_ID, '$', function(err) {
    if (err) {
        if (err.code == 'BUSYGROUP' ) {
            console.log(`Group ${APPLICATION_ID} already exists`);
        } else {
            console.log(err);
            process.exit();    
        }
    }
});

async.forever(
    function(next) {
        redisClient.xreadgroup('GROUP', APPLICATION_ID, CONSUMER_ID, 'BLOCK', 500, 'STREAMS',  STREAMS_KEY , '>', function (err, stream) {
            if (err) {
                console.error(err);
                next(err);
            }

            if (stream) {
                var messages = stream[0][1]; 
                // print all messages
                messages.forEach(function(message){
                    // convert the message into a JSON Object
                    console.log(message[1]);
                    var id = message[0];
                    var values = message[1];
                    var msgObject = { id : id};
                    for (var i = 0 ; i < values.length ; i=i+2) {
                        msgObject[values[i]] = values[i+1];
                    }                    
                    console.log( "Consumer2 Message: "+ JSON.stringify(msgObject));
                    redisClient.xack(STREAMS_KEY, APPLICATION_ID,msgObject.id);
                    console.log("message acknowledged");
                    // redisClient.xpending(STREAMS_KEY, APPLICATION_ID, function(err, stream){
                    //     if(err) console.log(err);
                    //     else {
                    //         console.log(stream);
                    //         console.log(stream[3][0][0]);
                    //         var pending = stream[0][1];
                    //         pending.forEach(function(message){
                    //             // convert the message into a JSON Obj
                    //             var id = message[0];
                    //             var values = message[1];
                    //             var msgObject = { id : id};
                    //             for (var i = 0 ; i < values.length ; i=i+2) {
                    //                 msgObject[values[i]] = values[i+1];
                    //             }         
                    //             console.log(msgObject); 
                    //         })
                    //     }
                    // })
                });
                
                
            }

            next();
        });
    },
    function(err) {
        console.log(" ERROR " + err);
        process.exit()
    }
);