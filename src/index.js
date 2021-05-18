const http = require('http');
const url = require('url');
const express = require('express');
const { BigQuery } = require('@google-cloud/bigquery');
const app = express();
const config = require('./config');
const port = config.port || 3000;

async function fetchLatestHandler (req, res){
  const searchParams = new URL(req.url).searchParams;
  const sqlQueryString = config.fetchLatestQuery.string;
  const options = {
    query: sqlQueryString,
    location: config.fetchLatestQuery.loc,
    params: {
      app_id: searchParams.get('app_id'),
      cursor: searchParams.get('from'),
      table: 'table-goes-here',
    },
  };
  if (!searchParams.has('app_id')) {
    return res.status(400).send({msg: 'Please specify the value of app_id'});
  } else if (!searchParams.has('from')) {
    return res.status(400).send({msg: 'Please specify the value of cursor'})
  }
  const bq = new BigQuery();
  try{
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
};
