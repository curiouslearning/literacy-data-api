const http = require('http');
const url = require('url');
const express = require('express');
const fs = require('fs');
const { BigQuery } = require('@google-cloud/bigquery');
const app = express();
const config = require('./config');
const port = config.port || 3000;

async function getTable(id) {
  const data = this.loadTableMap();
  const regex = /(^[a-z]{2,3})([\.][a-z0-9]*)+/gmi;
  if (!id.match(regex)) {
    const msg = `cannot parse app id: '${id}'. Check formatting and try again`;
    const err = new Error(msg);
    err.name = 'MalformedArgumentError';
    throw err;
  }
  const matches = data.filter(elem => elem.id === id);
  const obj = matches[0];
  if (!obj ) {
    const msg = `could not find a table for app id: '${id}'`;
    const err = new Error(msg);
    throw err;
  }
  const project = obj.project;
  const datasetId = obj.dataset;
  const tableId = obj.table;
  return `${project}.${datasetId}.${tableId}`
}

function loadTableMap() {
  const res = fs.readFileSync(`./${config.loadTableMap.mapFile}`, 'utf8');
  return res;
}

async function fetchLatestHandler (req, res) {
  const searchParams = new URL(req.url).searchParams;
  const sqlQueryString = config.fetchLatestQuery.string;
  const options = {
    query: sqlQueryString,
    location: config.fetchLatestQuery.loc,
    params: {
      app_id: searchParams.get('app_id'),
      cursor: searchParams.get('from'),
    },
  };
  if (!searchParams.has('app_id')) {
    return res.status(400).send({msg: 'Please specify the value of app_id'});
  } else if (!searchParams.has('from')) {
    return res.status(400).send({msg: 'Please specify the value of cursor'});
  } else if (!options.params.cursor.match(/[0-9]+/gmi)) {
    return res.status(400).send({msg: `value of 'from' must be a Unix timestamp`});
  }
  try{
    const bq = new BigQuery();
    const table = this.getTable(options.params.app_id);
    options.query = options.query.replace('@table', table);
    const rows = await bq.createQueryJob(options).then((data) => {
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
    return res.status(200).json(rows);
  } catch (e) {
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
}

app.get('fetch_latest*', fetchLatestHandler);

const server = app.listen(port, () => {
  console.log(`listening on ${port}`);
});

module.exports = {
  app,
  server,
  fetchLatestHandler,
  getTable,
  loadTableMap,
};
