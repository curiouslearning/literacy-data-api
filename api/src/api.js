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
  const resObj = formatRowsToJson(rows);
  return res.status(200).json({nextCursor, size: resObj.length, data: resObj});
}

function formatRowsToJson (rows) {
  let resObj = rows.map((row) => {
    return {
      attribution_url: row.attribution_id,
      app_id: row.app_package_name,
      ordered_id: row.event_timestamp,
      user: {
        id: row.user_pseudo_id,
        metadata: {
          continent: row.geo.continent,
          country: row.geo.country,
          region: row.geo.region,
          city: row.geo.city,
        },
        ad_attribution: {
          source: getSource(row.attribution_id),
          data: {
            advertising_id: row.device.advertising_id,
          },
        },
      },
      event: {
        name: parseName(row.action),
        date: row.event_date,
        timestamp: row.event_timestamp,
        value_type: getValueType(row.label),
        value: getValue(row.label) || row.val,
        level: getLevel(row.screen) || row.label.split('_')[1],
        profile: getProfile(row.screen) || 'unknown',
        rawData: {
          action: row.action,
          label: row.label,
          value: row.value,
        }
      },
    };
  });
  return resObj;
}

function getLevel (screen) {
  try {
    return screen.split('-')[0].split(' ')[1];
  } catch(e) {
    return null;
  }
}

function getProfile(screen) {
  try {
    return screen.split(':')[1].trim();
  } catch(e) {
    return null;
  }
}

function parseName(action) {
  if(action.indexOf('Segment') !== -1 || action.indexOf('Level') !== -1 || action.indexOf('Monster') !==-1) {
    return action.split('_')[0].trim();
  } else {
    return action;
  }
}

function getValueType(label) {
  let spacesSplit = label.split(' ');
  if (spacesSplit[0] === 'Puzzle') {
    return label.split(':')[0].replace('Puzzle ', '');
  } else if (spacesSplit[1] === 'puzzles') {
    return spacesSplit[1];
  } else if (spacesSplit[0] === 'days_since_last') {
    return 'days';
  } else if (spacesSplit[0] === 'total_playtime' || spacesSplit[0] === 'average_session') {
    return 'seconds';
  } else if (spacesSplit[0].indexOf('Level') !== -1){
    return 'Monster Level'
  }else{
    return null;
  }
}

function getValue(label) {
  if(label.indexOf('Puzzle') !== -1) {
    return label.split(':')[1].trim();
  } else if (label.indexOf('puzzles') != -1) {
    return label.split(' ')[0].trim();
  } else {
    return null;
  }
}

function getSource(referralString) {
  if (!referralString) return "no-source";
  const regex = /^([a-z]{1,6})?/gmi;
  const group = referralString.match(regex);
  if(group){
    let source = group.toString().toLowerCase();
    if(config.sourceMapping[source])
    {
      return config.sourceMapping[source];
    }
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
