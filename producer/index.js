'use strict';
const express = require('express');
var redis = require('redis');
var redisClient = redis.createClient();
const STREAMS_KEY = "weather_sensor:wind";

const app = express();
let i = 0;
app.get("/", (req ,res)=>{
    redisClient.xadd(STREAMS_KEY, "*","message", i);
    console.log("message sent");
})
app.listen(3000, ()=>{
    console.log("listening on port 3000");
})


