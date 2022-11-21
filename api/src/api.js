const express = require('express');
const mustache = require('mustache');
const config = require('./config');
const tableMap = require('./tableMap');
const { BigQuery } = require('@google-cloud/bigquery');
const fs = require('fs');

const getDataset = function (id) {
  const map = tableMap;

  const regex = /(^[a-z]{2,3})([\.][a-z0-9]*)+$/gmi;
  if (!id.match(regex)) {
    const msg = `cannot parse app id: '${id}'. Check formatting and try again`;
    throw new MalformedArgumentError(msg);
  }
  const obj = map[id];
  if (!obj) {
    const msg = `could not find a table for app id: '${id}'`;
    const err = new MissingDataError(msg);
    throw err;
  }
  return `${obj.project}.${obj.dataset}`;
}

function getDayOffset(timestamp) {
  return Math.ceil((new Date() - new Date(Math.round(timestamp / 1000))) / 1000 / 60 / 60 / 24)
}

async function fetchEvents(params) {
  const { appId, source, from, country, limit } = params;
  console.log(`Fetching data for: ${JSON.stringify(params)}`)

  const dataset = getDataset(appId)
  const sql = fs.readFileSync('sql/fetch_latest.sql').toString();

  const query = mustache.render(sql, { dataset })
  const options = {
    query: query,
    params: {
      traffic_source: source,
      range: getDayOffset(from),
      cursor: from,
      country: country,
      limit: limit,
    },
    location: 'US',
  };

  const bigquery = new BigQuery();

  const [job] = await bigquery.createQueryJob(options);
  console.log(`Job ${job.id} started.`);

  const [rows] = await job.getQueryResults();
  return rows
}

async function fetchLatestHandler(req, res, next) {
  const searchParams = req.query;

  const { app_id: appId, from, traffic_source: source, limit, country } = searchParams;


  // TODO: cleanup checks, preferably with a Type or something simple
  if (!appId) {
    return res.status(400).send({ msg: config.errNoId });
  } else if (!from) {
    return res.status(400).send({ msg: config.errNoCursor });
  } else if (!from.match(/[0-9]{1,10}/gmi)) {
    return res.status(400).send({ msg: config.errBadTimestamp });
  } else if (!source) {
    return res.status(400).send({ msg: "No source" })
  }

  try {
    const rows = await fetchEvents({ appId, source, from: Number(from), country, limit: Number(limit) })
    res.status(200).json({ data: rows })
  }
  catch (e) {
    next(e)
  }
}

function apiErrorHandler(err, _, res) {
  return res.status(500).send({
    msg: err.message,
    err: err,
  });
}

module.exports = (app) => {
  const router = express.Router();
  router.get('/fetch_latest*', fetchLatestHandler);

  app.use('/', router);
  app.use('/', apiErrorHandler);
};
