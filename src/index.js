const http = require('http');
const url = require('url');
const express = require('express');
const fs = require('fs');
const { BigQuery } = require('@google-cloud/bigquery');
const app = express();
const config = require('./config');
const tableMap = require('./tableMap');
const port = config.port || 3000;
const { MalformedArgumentError } = require('./helperClasses');

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

/**
* handler for GET requests to /fetch_latest
* returns all records for app_id logged after the given cursor
* @param{obj} req the request
* @param{obj} res the response
*/
const fetchLatestHandler = async function (req, res, next) {
  const searchParams = req.query;

  if (!searchParams.app_id) {
    return res.status(400).send({msg: config.errNoId});
  } else if (!searchParams.attribution_id){
    return res.status(400).send({msg: config.errNoAttr})
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
      ref_id: searchParams.attribution_id,
      cursor: Number(searchParams.from),
    },
  };

  try{
    const table = getTable(tableMap, options.params.app_id);
    options.query = options.query.replace('@table', `\`${table}\``);
    return runBigQueryJob(options).then((rows) => {
      resObj = formatRowsToJson(rows);
      return res.status(200).json(resObj).send();
    });
  } catch (e) {
    next(e)
  }
}

function formatRowsToJson (rows) {
  let resObj = [];
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
          timestamp: event_timestamp,
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
  const regex = /^([a-z]{1,6})(_[a-z]{1,3})?/gmi;
  const group = referralString.match(regex);
  switch (group.tolower()) {
    case 'fb_web':
      return 'fb_web';
    case 'fb_app':
      return 'fb_app';
    case: 'google_web':
      return 'google_web';
    case: 'google_app':
      return 'google_app';
    case: 'direct':
      return 'direct';
    default:
      return 'no-source';
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

app.get('/fetch_latest*', fetchLatestHandler);

app.use(function(err, req, res, next) {
  console.log(err.stack);
  if(e.name === 'MalformedArgumentError') {
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

// routes

const server = app.listen(port, () => {
  console.log(`listening on ${port}`);
});

module.exports = {
  app,
  server,
  fetchLatestHandler,
  getTable,
};
