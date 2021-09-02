const sinon = require('sinon');
const proxyquire = require('proxyquire');
const sandbox = sinon.createSandbox();
const queryResults = require('./fixtures/queryResults.json');
describe('BigQueryManager', () => {
  let job;
  let bigQuery;
  let queryOptions;
  let maxRows;
  let jobId;
  let token;
  beforeEach(() => {
    jobId = 'fake-job-id-01';
    token = 'thisisafaket0k3n';
    maxRows = 1000;
    queryOptions = {
      string: 'fake-query-string',
      location: 'fake-loc',
      params: {
        pkg_id: 'fake-pkg',
        ref_id: 'fake-ref-source',
        cursor: 123456789000000,
        range: Math.ceil((Date.now() - 123456789)/86400),
      },
      types: {
        pkg_id: 'STRING',
        ref_id: 'STRING',
        cursor: 'INT64',
      },
    };
    job = {
      id: jobId,
      getQueryResults: sandbox.stub(),
    }
    job.getQueryResults.onCall(0).callsArgWith(1, null, queryResults.set1, {
      maxResults: maxRows,
      autopaginate: false,
      pageToken: token}, {msg: 'fake-api-response'});
    job.getQueryResults.onCall(1).callsArgWith(1, null, queryResults.set2, null, {msg:'fake-api-response'});
    bigQuery = {
      createQueryJob: sandbox.stub().resolves([job, {msg: 'fake-api-response'}]),
      job: sandbox.stub().returns(job),
    }
  });

  afterEach(() => {
    sandbox.restore();
  });

  const { BigQueryManager } = proxyquire('../src/helperClasses', {
    '@google-cloud/bigquery': {
      BigQuery: sinon.stub().callsFake(() => {
        return bigQuery;
      }),
    }
  });

  it('should take a query options object and a max row value in constructor', ()=> {
    try {
      const test = new BigQueryManager(queryOptions, maxRows);
      test.MAXROWS.should.equal(maxRows);
      const res = test.getOptions();
      res.should.deep.equal(queryOptions);
    } catch(e) {
      e.should.equal(null);
    }
  });
  it('should take an optional job id and token in constructor', () => {
    try {
      const test = new BigQueryManager(queryOptions, maxRows, jobId, token);
      test.jobId.should.equal(jobId);
      test.token.should.equal(token);
    } catch (e) { //an error should not happen
      e.should.equal(null);
    }
  });
  it('should start a new bigquery job on start', async () => {
    const test = new BigQueryManager(queryOptions, maxRows);
    await test.start(() => {});
    bigQuery.createQueryJob.should.have.been.calledWith(queryOptions);
  });
  it('should get the results when the new query has finished', async () => {
    const test = new BigQueryManager(queryOptions, maxRows);
    const spy = sandbox.spy(test, 'paginationCallback');
    await test.start(() => {});
    job.getQueryResults.should.have.been.calledWith({
      maxResults: maxRows,
      autopaginate: false,
      timeoutMs: 60000
    }, sinon.match.func);
  });
  it('should call the pagination callback after a new job is complete', async ()=> {
    const test = new BigQueryManager(queryOptions, maxRows);
    const spy = sandbox.spy(test, 'paginationCallback');
    await test.start(() => {});
    spy.should.have.been.calledWith(null, queryResults.set1, {
      maxResults: maxRows,
      autopaginate: false,
      pageToken: token,
    }, {msg: 'fake-api-response'});
  });
  it('should pass the proper arguments to the client callback', async ()=> {
    const test = new BigQueryManager(queryOptions, maxRows);
    const callback= sandbox.stub();
    await test.start(callback);
    callback.should.have.been.calledWith(queryResults.set1, jobId, token, false);
  });
  it('should inform the client when all results fetched', async () => {
    job.getQueryResults.onCall(0).callsArgWith(1, null, queryResults.set2, null, {msg: 'fake-api-response'});
    const test = new BigQueryManager(queryOptions, maxRows);
    const callback = sandbox.stub();
    await test.start(callback);
    callback.should.have.been.calledWith(queryResults.set2, null, null, true);
  });
  it('should create a job object with the id', () => {
    try{
      const test = new BigQueryManager(queryOptions, maxRows, jobId, token);
      test.fetchNext();
      bigQuery.job.should.have.been.calledWith(jobId);
    } catch (e) {
      should.not.exist(e);
    }
  })
  it('should not start a new job if a jobId is present', () => {
    const test = new BigQueryManager(queryOptions, maxRows, jobId);
    test.fetchNext();
    bigQuery.createQueryJob.should.not.have.been.called;
  });
  it('should not fetch results if all results have been fetched', () => {
    const test = new BigQueryManager(queryOptions, maxRows, jobId, null);
    test.allResultsFetched = true;
    const callback = sandbox.stub();
    test.fetchNext(callback);
    callback.should.have.been.calledWith([], null, null, true);
  })
  it('should pass the token when resuming a job', () => {
    const test = new BigQueryManager(queryOptions, maxRows, jobId, token);
    test.fetchNext();
    job.getQueryResults.should.have.been.calledWith({
      maxResults: maxRows,
      autopaginate: false,
      pageToken: token
    }, sinon.match.func);
  });
  it('should not re-query the database when all results fetched', () => {
    const test = new BigQueryManager(queryOptions, maxRows);
    const callback = sandbox.stub();
    test.fetchNext(callback);
    callback.should.have.been.calledWith([], null, null, true);
  });

  it('should fetch the next set of results', (done) => {
    const test = new BigQueryManager(queryOptions, maxRows);
    const callback = (rows, jobId, token, isComplete) => {
      if (isComplete) {
        rows.should.deep.equal(queryResults.set2);
        done();
      } else {
        test.fetchNext();
      }
    };
    test.start(callback)
  });
});
