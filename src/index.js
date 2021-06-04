const http = require('http');
const url = require('url');
const express = require('express');
const app = express();
const fs = require('fs');
const { BigQuery } = require('@google-cloud/bigquery');
const litAPI = require('./api');
const config = require('./config.json');
const port = config.port || 3000;
// routes
app.use('/data', litAPI.router);
app.use('/data', litAPI.apiErrorHandler);

const server = app.listen(port, () => {
  console.log(`listening on ${port}`);
});

module.exports = {
  app,
  server,
};
