const sinon = require('sinon');
const express = require ('express');
const proxyquire = require('proxyquire').noPreserveCache();
const bq =  require('@google-cloud/bigquery');
const http = require('http');
const sandbox = sinon.createSandbox();

function initGetTest() {
  this.request = sandbox.stub(http, 'request');
  let utils = {};
  utils['data'] = [
    {user_pseudo_id: 'user1', event_date: '20210503', event_timestamp: '6285654678', event_timestamp_offest: '65765'},
    {user_pseudo_id: 'user2', event_date: '20210504', event_timestamp: '6295654678', event_timestamp_offest: '95465'},
    {user_pseudo_id: 'user3', event_date: '20210501', event_timestamp: '6235654678', event_timestamp_offest: '65365'},
    {user_pseudo_id: 'user4', event_date: '20210503', event_timestamp: '6285657678', event_timestamp_offest: '65065'},
  ];

  utils['res'] = {};
  utils.res.status = sandbox.stub().returns(utils.res);
  utils.res.send = sandbox.stub().returns(utils.res);
  utils.res.json = sandbox.stub().returns(utils.res);
  utils['URL'] = '';
  utils['buildURL'] = (route, params) => {
    utils.URL = `/${route}?`;
    for (param in params) {
      let queryString = `${param}=${params[param]}`;
      utils.URL += queryString
    }
  }
  utils['event'] = {};
  const job = {
    getQueryResults: sandbox.stub().resolves([utils.data]),
  };
  const bigquery = {
    createQueryJob: sandbox.stub().resolves([job]),
  };
  const BigQueryStub = sandbox.stub().returns(bigquery);
  utils['queryStub'] = bigquery;
  utils['index'] = proxyquire('../src/index', {
    '@google-cloud/bigquery': {BigQuery: BigQueryStub},
  });
  return utils;
}

function initFetchLatest() {
  let run = async (req, res, index) => {
    const result = await index.fetchLatestHandler(req, res);
    index.server.close();
    return result;
  }
  return run;
}

describe('app', () => {
  afterEach(() => {
    sandbox.restore();
  });
  describe('fetch_latest', () => {
    afterEach(() => {
      sandbox.restore();
    });

    it('should return 200 when successful', async () => {
      let utils = initGetTest();
      utils.event = {
        url: `https://followthelearners.curiouslearning.org/fetch_latest?app_id=com.test.app&from=618984568766`,
        body: {
          app_id: 'com.test.app',
          from:   '618984568766',
        },
      };
      let run = initFetchLatest();
      await run(utils.event, utils.res, utils.index);
      utils.res.status.should.have.been.calledWith(200);
    });

    it('should return the data', async() => {
      let utils = initGetTest();
      utils.event = {
        url: `https://followthelearners.curiouslearning.org/fetch_latest?app_id=com.test.app&from=618984568766`,
        body: {
          app_id: 'com.test.app',
          from:   '618984568766',
        },
      };
      let run = initFetchLatest();
      await run(utils.event, utils.res, utils.index);
      utils.res.json.should.have.been.calledWith(utils.data);
    });

    it('should return a 400 response on malformed app id', async () => {
      let utils = initGetTest();
      utils.event = {
        url: `https://followthelearners.curiouslearning.org/fetch_latest?app_id=com.test.@pp&from=618984568766`,
        body: {
          app_id: 'com.test.@pp',
          from: '618984568766',
        },
      };
      let run = initFetchLatest();
      await run(utils.event, utils.res, utils.index);
      utils.res.status.should.have.been.calledWith(400);
    });
    it('should return a 400 response on malformed cursor', async () => {
      let utils = initGetTest();
      utils.event = {
        url: `https://followthelearners.curiouslearning.org/fetch_latest?app_id=com.test.app&from=cursor`,
        body: {
          app_id: 'com.test.app',
          from: 'cursor',
        },
      };
      let run = initFetchLatest();
      await run(utils.event, utils.res, utils.index);
      utils.res.status.should.have.been.calledWith(400);
    });

    it('should return a 500 response if fetch fails', async () => {
      let utils = initGetTest();
      utils.queryStub.createQueryJob.rejects();
      utils.event = {
        url: `https://followthelearners.curiouslearning.org/fetch_latest?app_id=com.test.app&from=618984568766`,
        body: {
          app_id: 'com.test.app',
          from: '618984568766',
        },
      };
      let run = initFetchLatest();
      await run(utils.event, utils.res, utils.index);
      utils.res.status.should.have.been.calledWith(500);
    });
  });
});
