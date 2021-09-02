const http = require('http');
const url = require('url');
const express = require('express');
const router = express.Router();
const mustache = require('mustache');
const config = require('./config');
const tableMap = require('./tableMap');
const {
  MissingDataError,
  MalformedArgumentError,
  BigQueryManager,
  MemcachedManager,
  SqlLoader
} = require('./helperClasses');
const queryStrings = new SqlLoader(config.sqlFiles, './src/sql');
const dns = process.env.MEMCACHE_DNS;
const port = process.env.MEMCACHE_PORT;
const cacheManager = new MemcachedManager(`${dns}:${port}`);

const DAYINSECONDS = 86400;
/**
* returns the table containing records from the given app id
* @param{string} id the app id
* @returns{string} the table name in SQL table format
*/
const getTable = function (map, id, getHistoric) {
  const regex = /(^[a-z]{2,3})([\.][a-z0-9]*)+$/gmi;
  if (!id.match(regex)) {
    const msg = `cannot parse app id: '${id}'. Check formatting and try again`;
    throw new MalformedArgumentError(msg);
  }
  const obj = map[id];
  if (!obj ) {
    const msg = `could not find a table for app id: '${id}'`;
    const err = new MissingDataError(msg);
    throw err;
  }
  return `${obj.project}.${obj.dataset}.${obj.table}`;
}

function sendRows(res, rows, nextCursor, key) {
  if (!rows || rows.length == 0 || !nextCursor) {
    cacheManager.removeCache(key);
  }
  const resObj = formatRowsToJson(rows);
  return res.status(200).json({nextCursor, data: resObj});
}

function formatRowsToJson (rows) {
  let resObj = rows.map((row) => {
    return {
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
        properties: row.user_properties,
      },
      event: {
        name: row.event_name,
        date: row.event_date,
        timestamp: row.event_timestamp,
        key: `${row.action} - ${row.label}`,
        value: row.value,
        value_type: row.type,
      },
    };
  });
  return resObj;
}

function getSource(referralString) {
  if (!referralString) return "no-source";
  const regex = /^([a-z]{1,6})(_[a-z]{1,3})?/gmi;
  const group = referralString.match(regex);
  let source = group.toString().toLowerCase();
  if(config.sourceMapping[source])
  {
    return config.sourceMapping[source];
  }
  return 'no-source';
}

/**
* handler for GET requests to /fetch_latest
* returns all records for app_id logged after the given cursor
* @param{obj} req the request
* @param{obj} res the response
*/
async function fetchLatestHandler (req, res, next) {
  const searchParams = req.query;

  if (!searchParams.app_id) {
    return res.status(400).send({msg: config.errNoId});
  } else if (!searchParams.from) {
    return res.status(400).send({msg: config.errNoCursor});
  } else if (!searchParams.from.match(/[A-Z0-9]+/gmi)) {
    return res.status(400).send({msg: config.errBadCursor});
  }
  const sql = queryStrings.getQueryString(config.sqlFiles.fetchLatest)
  const options = {
    query: sql,
    location: config.fetchLatestQuery.loc,
    params: {
      pkg_id: searchParams.app_id,
      ref_id: searchParams.attribution_id || '',
      cursor: Number(searchParams.from) * 1000000, //convert to micros
      //only search back as far as we need to
      range:  Math.ceil(((Date.now()/1000) - searchParams.from)/DAYINSECONDS),
    },
    types: {
      pkg_id: 'STRING',
      ref_id: 'STRING',
      cursor: 'INT64',
      range: 'INT64',
    },
  };

  try{
    const table = getTable(tableMap, options.params.pkg_id);
    options.query = mustache.render(options.query, {table: table});
    const keyParams = {pkg: searchParams.app_id, cursor: searchParams.from};
    let key = cacheManager.createKey( 'bigquery', keyParams);
    const duration = config.fetchLatestQuery.cacheDuration;
    const maxRows = config.fetchLatestQuery.MAXROWS;
    const callback = (rows, id, token, isComplete) => {
      if(!isComplete) {
        console.log(`saving job ${id} with token ${token}`);
        const val =  {jobId: id, token: token};
        keyParams.cursor = token;
        key = cacheManager.createKey('bigquery', keyParams);
        cacheManager.cacheResults(key, val, duration);
      } else {
        cacheManager.removeCache(key);
      }
      sendRows(res, rows, token, key);
    };
    console.log('trying key ' + key);
    cacheManager.get(key, (err, cache) => {
      if (err) throw err;
      if (cache) {
        const bq = new BigQueryManager(options, maxRows, cache.jobId, cache.token);
        bq.fetchNext(callback);
      }
      else {
        const bq = new BigQueryManager(options, maxRows);
        bq.start(callback);
      }
    });
  } catch (e) {
    next(e)
  }
}

function apiErrorHandler(err, req, res, next) {
  if(err.name === 'MalformedArgumentError') {
    return res.status(400).send({
      msg: err.message,
      err: err,
    });
  }
  if(err.name === 'MissingDataError') {
    return res.status(404).send({
      msg: err.message,
      err: err,
    });
  }
  console.log(err.stack);
  return res.status(500).send({
    msg: err.message,
    err: err,
  });
}

module.exports = (app) => {
    app.use('/', router);
    router.get('/fetch_latest*', fetchLatestHandler);
    app.use('/', apiErrorHandler);
};
