const http = require('http');
const url = require('url');
const express = require('express');
const fs = require('fs');
const { BigQuery } = require('@google-cloud/bigquery');
const router = express.Router();
const config = require('./config');
const tableMap = require('./tableMap');
const {
  MalformedArgumentError,
  BigQueryManager,
  MemcachedManager,
} = require('./helperClasses');
const Memcached = require('memcached');
const cacheManager = new MemcachedManager('127.0.0.1:11211');

/**
* returns the table containing records from the given app id
* @param{string} id the app id
* @returns{string} the table name in SQL table format
*/
const getTable = function (map, id) {
  const regex = /(^[a-z]{2,3})([\.][a-z0-9]*)+$/gmi;
  if (!id.match(regex)) {
    const msg = `cannot parse app id: '${id}'. Check formatting and try again`;
    throw new MalformedArgumentError(msg);
  }
  const obj = map[id];
  if (!obj ) {
    const msg = `could not find a table for app id: '${id}'`;
    const err = new Error(msg);
    throw err;
  }
  return `${obj.project}.${obj.dataset}.${obj.table}`;
}

function sendRows(res, rows, key) {
  let next = true;
  if (!rows || rows.length == 0) {
    cacheManager.removeCache(key);
    next = false;
  }
  const resObj = formatRowsToJson(rows);
  return res.status(200).json({data: resObj, next});
}

function formatRowsToJson (rows) {
  let resObj = [];
  if (!rows || rows.length === 0) return resObj;
  rows.forEach((row) => {
    resObj.push({
      attribution_url: row.referral_source,
      app_id: row.app_package_name,
      ordered_id: row.event_timestamp,
      user: {
        id: row.user_pseudo_id,
        metadata: {
          continent: row.continent,
          country: row.country,
          region: row.region,
          city: row.city,
        },
        ad_attribution: {
          source: getSource(row.referral_source),
          data: {
            advertising_id: row.advertising_id,
          },
        },
        event: {
          name: row.event_name,
          date: row.event_date,
          timestamp: row.event_timestamp,
          label: row.label,
          action: row.action,
          value: row.value,
          value_type: row.type,
        }
      }

    });
  });
  return resObj;
}

function getSource(referralString) {
  if (!referralString) return "no-source";
  const regex = /^([a-z]{1,6})(_[a-z]{1,3})?/gmi;
  const group = referralString.match(regex);
  switch (group.tolower()) {
    case 'fb_web':
      return 'fb_web';
    case 'fb_app':
      return 'fb_app';
    case 'google_web':
      return 'google_web';
    case 'google_app':
      return 'google_app';
    case 'direct':
      return 'direct';
    default:
      return 'no-source';
  }
}

/**
* handler for GET requests to /fetch_latest
* returns all records for app_id logged after the given cursor
* @param{obj} req the request
* @param{obj} res the response
*/
router.get('/fetch_latest*', async function (req, res, next) {
  const searchParams = req.query;

  if (!searchParams.app_id) {
    return res.status(400).send({msg: config.errNoId});
  } else if (!searchParams.from) {
    return res.status(400).send({msg: config.errNoCursor});
  } else if (!searchParams.from.match(/[0-9]+/gmi)) {
    return res.status(400).send({msg: config.errBadCursor});
  }
  const sql = fs.readFileSync(config.fetchLatestQuery.string).toString();
  const options = {
    query: sql,
    location: config.fetchLatestQuery.loc,
    params: {
      pkg_id: searchParams.app_id,
      ref_id: searchParams.attribution_id || '',
      cursor: Number(searchParams.from),
    },
    types: {
      pkg_id: 'STRING',
      ref_id: 'STRING',
      cursor: 'INT64',
    },
  };

  try{
    const table = getTable(tableMap, options.params.pkg_id);
    options.query = options.query.replace('@table', `\`${table}\``);
    const keyParams = {pkg: options.params.pkg_id, cursor: options.params.cursor};
    const key = cacheManager.createKey( 'BigQuery', keyParams);
    const duration = config.fetchLatestQuery.cacheDuration;
    const callback = (rows, id, token, isComplete) => {
      if(!isComplete) {
        console.log(`saving job ${id} with token ${token}`);
        const val =  {jobId: id, token: token};
        cacheManager.cacheResults(key, val, duration);
      } else {
        cacheManager.removeCache(key);
      }
      sendRows(res, rows, key);
    };

    cacheManager.get(key, (err, cache) => {
      if (err) throw err;
      if (cache) {
        const bq = new BigQueryManager(options, config.fetchLatestQuery.MAXROWS, cache.jobId, cache.token);
        bq.resume(callback);
      }
      else {
        const bq = new BigQueryManager(options, config.fetchLatestQuery.MAXROWS);
        bq.start(callback);
      }
    });
  } catch (e) {
    next(e)
  }
});

function apiErrorHandler(err, req, res, next) {
  console.log(err.stack);
  if(err.name === 'MalformedArgumentError') {
    return res.status(400).send({
      msg: 'error in one or more arguments',
      err: err,
    });
  }
  return res.status(500).send({
    msg: 'The data could not be fetched',
    err: err,
  });
}

module.exports = {
  router,
  apiErrorHandler
}
