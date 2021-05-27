const sinon = require('sinon');
const express = require ('express');
const proxyquire = require('proxyquire').noPreserveCache();
const fs = require('fs');
const bq =  require('@google-cloud/bigquery');
const http = require('http');
const testData = require('./testTableMap.json');
const sandbox = sinon.createSandbox();

function initGetTest() {
  this.request = sandbox.stub(http, 'request');
  let utils = {};
  utils['data'] = [
    {user_pseudo_id: 'user1', event_date: '20210503', event_timestamp: '6285654678', event_timestamp_offest: '65765', app_info: {id: 'com.test.app.id'}},
    {user_pseudo_id: 'user2', event_date: '20210504', event_timestamp: '6295654678', event_timestamp_offest: '95465', app_info: {id: 'com.test.app.id'}},
    {user_pseudo_id: 'user3', event_date: '20210501', event_timestamp: '6235654678', event_timestamp_offest: '65365', app_info: {id: 'com.test.app.id'}},
    {user_pseudo_id: 'user4', event_date: '20210503', event_timestamp: '6285657678', event_timestamp_offest: '65065', app_info: {id: 'com.test.app.id'}},
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
    './tableMap': require('./testTableMap.json'),
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


function initSetTable() {
  let utils = {};

  utils['data'] = testData;
  const table = {
    exists: sandbox.stub().resolves(),
  };
  const dataset = {
    exists: sandbox.stub().resolves(),
    table: sandbox.stub().returns(table),
  };
  utils['tableStub'] = table;
  utils['datasetStub'] = dataset;
  utils['bigquery'] = {
    dataset: sandbox.stub().returns(dataset),
    createQueryJob: sandbox.stub().resolves(),
  };
  const BigQueryStub = sandbox.stub().returns(utils.bigquery);
  utils['querystub'] = BigQueryStub;
  utils['index'] = proxyquire('../src/index', {
    '@google-cloud/bigquery': {BigQuery: BigQueryStub},
    './tableMap': require('./testTableMap.json'),
  });
  utils['run'] = async (id) => {
    const res = await utils.index.getTable(id);
    utils.index.server.close();
    return res;
  };
  return utils;
}

function initLoadMap () {
  let utils = {};
  utils['data'] = testData;
  const fs = {
    readFileSync: sandbox.stub().returns(utils.data),
  };
  utils['fs'] = fs;
  utils['index'] = proxyquire('../src/index', {
    'fs': fs,
  });
  utils['run'] = () => {
    const res = utils.index.loadTableMap();
    utils.index.server.close();
    return res;
  }
  return utils;
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
        url: `/fetch_latest?app_id=com.test.app&from=618984568766`,
        query: {
          app_id: 'com.test.app.id',
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
        url: `/fetch_latest?app_id=com.test.app&from=618984568766`,
        query: {
          app_id: 'com.test.app.id',
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
        url: `/fetch_latest?app_id=com.test.@pp&from=618984568766`,
        query: {
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
        url: `/fetch_latest?app_id=com.test.app&from=cursor`,
        query: {
          app_id: 'com.test.app.id',
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
        url: `/fetch_latest?app_id=com.test.app&from=618984568766`,
        query: {
          app_id: 'com.test.app.id',
          from: '618984568766',
        },
      };
      let run = initFetchLatest();
      await run(utils.event, utils.res, utils.index);
      utils.res.status.should.have.been.calledWith(500);
    });

    it('should throw an error if auth fails for the table', async () => {
      let utils = initGetTest();
      utils.event = {
        url: `?app_id=com.test.app&from=654982341687`,
        query: {
          app_id: 'com.test.app',
          from: '654982341687',
        },
      };
      const err = new Error('you do not have permission to access this table');
      utils.queryStub.createQueryJob.throws(err);
      const spy = sandbox.spy(utils.index, 'fetchLatestHandler');
      let run = initFetchLatest();
      try {
        await run(utils.event, utils.res, utils.index);
        utils.res.status.should.have.been.calledWith(500);
      } catch (e) {
        utils.index.server.close();
        console.log(e);
        e.message.should.equal(err.message);
      }
    });
  });
  describe('setTable', () => {
    beforeEach(() => {

    });
    afterEach(()=> {
      sandbox.restore();
    });

    it('should return the table that matches the app id', async () => {
      let utils = initSetTable();
      try {
        const expected = utils.data[0];
        const res = await utils.run(expected.id);
        res.indexOf(expected.table).should.not.equal(-1);
      } catch (e) {
        utils.index.server.close();
        throw e;
      }
    });
    it('should find the dataset based on the app id', async () => {
      let utils = initSetTable();
      try {
        const expected = utils.data[0];
        const res = await utils.run(expected.id);
        res.indexOf(expected.dataset).should.not.equal(-1);
      } catch (e) {
        utils.index.server.close();
        throw e;
      }
    });
    it('should throw an error on missing app id', async () => {
      let utils = initSetTable();
      const spy = sandbox.spy(utils.index, 'getTable');
      const expected = {id: 'com.missing.app.id'};
      try{
        await utils.run(expected.id);
        spy.exceptions[0].should.not.equal(undefined);
      } catch (e) {
        utils.index.server.close();
        const res = `could not find a table for app id: '${expected.id}'`;
        e.message.should.equal(res);
      }
    });
    it('should throw an error on malformed app id', async () => {
      let utils = initSetTable();
      const spy = sandbox.spy(utils.index, 'getTable');
      const id = 'malformed-app,id';
      try {
        await utils.run(id);
        spy.exceptions[0].should.not.equal(undefined);
      } catch(e) {
        utils.index.server.close();
        e.message.should.equal(`cannot parse app id: '${id}'. Check formatting and try again`);
      }
    });
  });
});
