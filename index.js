
'use strict'

const express = require('express');

const PORT = 8181;
const HOST = '0.0.0.0';

const app = express();

app.get('/', function(req, res) {
    res.send("Hello Docker!!!");
});

app.get('/taskinfo', function(req, res) {
    res.send({
        "repeat": 5,
        "openPice": 5,
        "trackURLTimeInterval": 5.0,
    })
});

app.listen(PORT, HOST);

console.log(`Running on http://${HOST}:${PORT}`);