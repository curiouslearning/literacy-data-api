const sinon = require('sinon');
const bigquery = require('@google-cloud/bigquery');
const http = require('http');
const PassThrough = require('stream').PassThrough;
const sandbox = sinon.createSandbox();
describe('app', () => {
  afterEach(() => {
    sandbox.restore();
  })
  describe('fetch_latest', () => {
    afterEach(() => {
      sandbox.restore();
    })
    function initGetTest() {
      this.request = sandbox.stub(http, 'request');
      let utils = {};
      utils['data'] = [
        {user_pseudo_id: 'user1', event_date: '20210503', event_timestamp: '6285654678', event_timestamp_offest: '65765'},
        {user_pseudo_id: 'user2', event_date: '20210504', event_timestamp: '6295654678', event_timestamp_offest: '95465'},
        {user_pseudo_id: 'user3', event_date: '20210501', event_timestamp: '6235654678', event_timestamp_offest: '65365'},
        {user_pseudo_id: 'user4', event_date: '20210503', event_timestamp: '6285657678', event_timestamp_offest: '65065'},
      ];

      utils['send'] = sandbox.spy();
      utils['status'] = sandbox.stub().returns(utils.res);
      utils['res'] = {
        status: utils.status,
        send: utils.send,
      };
      utils['event'] = {};
      utils['run'] = async (myFuncPath, myFuncName) => {
        const request = new PassThrough();
        sandbox.stub(request, 'write');
        this.request.returns(request);
        let myFunc = require(myFuncPath);
        return await myFunc[myFuncName](utils.event, utils.res);
      }
      utils['queryStub'] = sandbox.stub(bigquery.prototype, 'Client').returns({
        createQueryJob: sandbox.stub().resolves(),
        fetchQueryResults: sandbox.stub().resolves(utils.data),
      });
      return utils;
    }
    it('should return 200 when successful', async () => {
      let utils = initGetTest();
      utils.event = {
        body: {
          app_id: 'com.test.app',
          from:   '618984568766',
        },
      };
      const res = await utils.run('../src/app.js', 'fetch_latest');
      res.status.should.equal(200);
    });
  });
  it('should return the data', async() => {
    let utils = initGetTest();
    utils.event = {
      body: {
        app_id: 'com.test.app',
        from: '618984568766',
      },
    };
    const res = await utils.run('../src/app.js', 'fetch_latest');
    res.data.should.equal(utils.data);
  });

  it('should return a 400 response on malformed app id', async () => {
    let utils = initGetTest();
    utils.event = {
      body: {
        app_id: 'com.test.@pp',
        from: '618984568766',
      },
    };
    const res = await utils.run('../src/app.js', 'fetch_latest');
    res.status.should.equal(400);
  });
  it('should return a 400 response on malformed cursor', async () => {
    let utils = initGetTest();
    utils.event = {
      body: {
        app_id: 'com.test.app',
        from: 'cursor',
      },
    };
    const res = await utils.run('../src/app.js', 'fetch_latest');
    res.status.should.equal(400);
  });

  it('should return a 500 response if fetch fails', async () => {
    let utils = initGetTest();
    utils.queryStub.rejects();
    utils.event = {
      body: {
        app_id: 'com.test.app',
        from: '618984568766',
      },
    };
    const res = await utils.run('../src/app.js', 'fetch_latest');
    res.status.should.equal(500);
  });
});
