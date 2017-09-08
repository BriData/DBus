'use strict';

var Redis = require('ioredis');

var redis = new Redis({
    sentinels: [{host: 'localhost', port: 16000}, {host: 'localhost', port: 16000}],
    name: 'master6000'
});

redis.select(9);

module.exports = redis;
