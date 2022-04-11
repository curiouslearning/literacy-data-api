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
  BigQueryParser,
  SqlLoader
} = require('./helperClasses');
const queryStrings = new SqlLoader(config.sqlFiles, './src/sql');

const DAYINSECONDS = 86400;
/**
* returns the table containing records from the given app id
* @param{string} id the app id
* @returns{string} the table name in SQL table format
*/
const getDataset = function (map, id, getHistoric) {
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
  return `${obj.project}.${obj.dataset}`;
}

function sendRows(res, rows, nextCursor) {
  const parser = new BigQueryParser(config.sourceMapping);
  const dedupe = parser.deduplicateData(rows);
  const resObj = parser.formatRowsToJson(dedupe);
  return res.status(200).json({nextCursor, size: resObj.length, data: resObj});
}

/**
* handler for GET requests to /fetch_latest
* returns all records for app_id logged after the given cursor
* @param{obj} req the request
* @param{obj} res the response
*/
async function fetchLatestHandler (req, res, next) {
  const searchParams = req.query;

  if (!searchParams.app_id) { //TODO: Edit proofreading to allow timestamp OR job hash
    return res.status(400).send({msg: config.errNoId});
  } else if (!searchParams.from && !searchParams.token) {
    return res.status(400).send({msg: config.errNoCursor});
  } else if (!searchParams.from.match(/[0-9]{1,10}/gmi)) {
    return res.status(400).send({msg: config.errBadTimestamp});
  }
  const sql = queryStrings.getQueryString(config.sqlFiles.fetchLatest)
  const options = {
    query: sql,
    location: config.fetchLatestQuery.loc,
    params: {
      pkg_id: searchParams.app_id,
      ref_id: searchParams.attribution_id || '',
      user_id: searchParams.user_id || '',
      event: searchParams.event || '',
      utm_campaign: searchParams.utm_campaign || '',
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
    const dataset = getDataset(tableMap, options.params.pkg_id);
    options.query = mustache.render(options.query, {dataset: dataset});
    const maxRows = config.fetchLatestQuery.MAXROWS;
    const callback = (rows, id, token) => {
      if(id && token) {
        let combinedToken = encodeURIComponent(`${id}/${token}`);
        sendRows(res, rows, combinedToken);
      } else {
        sendRows(res, rows, null);
      }
    };
    if(searchParams.token) {
      let params = decodeURIComponent(searchParams.token).split('/');
      const job = {id: params[0], token: params[1]};
      //TODO decode token into job id and token
      const bq = new BigQueryManager(options, maxRows, job.id, job.token);
      bq.fetchNext(callback);
    }
    else {
      const bq = new BigQueryManager(options, maxRows);
      bq.start(callback);
    }
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
