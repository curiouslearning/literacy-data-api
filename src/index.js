const http = require('http');
const url = require('url');
const express = require('express');
const fs = require('fs');
const { BigQuery } = require('@google-cloud/bigquery');
const app = express();
const config = require('./config');
const tableMap = require('./tableMap');
const port = config.port || 3000;

/**
* returns the table containing records from the given app id
* @param{string} id the app id
* @returns{string} the table name in SQL table format
*/
const getTable = function (map, id) {
  const regex = /(^[a-z]{2,3})([\.][a-z0-9]*)+$/gmi;
  if (!id.match(regex)) {
    const msg = `cannot parse app id: '${id}'. Check formatting and try again`;
    const err = new Error(msg);
    err.name = 'MalformedArgumentError';
    throw err;
  }
  const obj = map[id];
  if (!obj ) {
    const msg = `could not find a table for app id: '${id}'`;
    const err = new Error(msg);
    throw err;
  }
  return `${obj.project}.${obj.dataset}.${obj.table}`;
}

/**
* handler for GET requests to /fetch_latest
* returns all records for app_id logged after the given cursor
* @param{obj} req the request
* @param{obj} res the response
*/
const fetchLatestHandler = async function (req, res) {
  const searchParams = req.query;

  if (!searchParams.app_id) {
    return res.status(400).send({msg: config.errNoId});
  } else if (!searchParams.from) {
    return res.status(400).send({msg: config.errNoCursor});
  } else if (!searchParams.from.match(/[0-9]+/gmi)) {
    return res.status(400).send({msg: config.errBadCursor});
  }

  const sqlQueryString = config.fetchLatestQuery.string;
  const options = {
    query: sqlQueryString,
    location: config.fetchLatestQuery.loc,
    params: {
      app_id: searchParams.app_id,
      cursor: Number(searchParams.from),
    },
  };

  try{
    const table = getTable(tableMap, options.params.app_id);
    options.query = options.query.replace('@table', `\`${table}\``);
    const rows = await runBigQueryJob(options);
    return res.status(200).json(rows).send();
  } catch (e) {
    return handleError(e, res);
  }
}

// TODO: support for pagination tokens on large result sets
/**
* create a bigquery job with given options, then await its completion
* @param{obj} options the QueryJobOptions object
* @returns{Array<obj>} the query result as an array of row objects
*/
function runBigQueryJob(options) {
  const bq = new BigQuery();
  return bq.createQueryJob(options).then((data) => {
      if (data) {
        return data[0].getQueryResults(data[0]).then((result) => {
          return result[0];
        }).catch((err) => {
          throw err;
        });
      } else {
        throw new Error('error creating the query job')
      }
    }).catch((err) => {throw err;});
}

function handleError(e, res) {
  console.log(e);
  if(e.name === 'MalformedArgumentError') {
    return res.status(400).send({
      msg: 'error in one or more arguments',
      err: e,
    });
  }
  return res.status(500).send({
    msg: 'The data could not be fetched',
    err: e,
  });
}

// routes
app.get('/fetch_latest*', fetchLatestHandler);

const server = app.listen(port, () => {
  console.log(`listening on ${port}`);
});

module.exports = {
  app,
  server,
  fetchLatestHandler,
  getTable,
};
