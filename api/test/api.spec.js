const sinon = require('sinon');
const proxyquire = require('proxyquire').noPreserveCache();
const supertest = require('supertest');
const express = require('express');
const sandbox = sinon.createSandbox();
const queryResults = require('./fixtures/queryResults.json');
const {BigQueryParser} = require('../src/helperClasses.js');
const config = require('../src/config.json');

describe('Literacy API Routes', () => {
  let app, request, cacheManager, bqManager, sqlLoader;
  let pkgId, attrId, from, queryOptions;
  let resultSet, jobId, token;
  let api;
  const bqParser = new BigQueryParser(config.sourceMapping);
  beforeEach(() => {
    pkgId = 'fake-pkg';
    attrId = '';
    from = 123456789000000;
    resultSet = queryResults.set3;
    jobId= 'fake-job';
    token= 'th1s1safak3t0k3n';
    queryOptions= {
      string: 'fake querystring',
      params: {
        pkg_id: pkgId,
        attr_id: attrId,
        cursor: from
      }
    };
    cacheManager = {
      createKey: sandbox.stub().returns(`__test__${token}${attrId}${from}`),
      cacheResults: sandbox.stub(),
      removeCache: sandbox.stub(),
      get: sandbox.stub()
          .onCall(0).callsArgWith(1, null, null)
          .onCall(1).callsArgWith(1, null, {jobId: jobId, token: token}),
      set: sandbox.stub(),
    };
    bqManager = {
      start: sandbox.stub().callsArgWith(0, resultSet, jobId, token),
      fetchNext: sandbox.stub().callsArgWith(0, resultSet, null, null),
      getOptions: sandbox.stub().returns(queryOptions),
      isComplete: sandbox.stub().returns(false),
    };
    sqlLoader = {
      getQueryString: sandbox.stub().returns('fake-query-string'),
    }
    api = proxyquire('../src/api', {
      './helperClasses': {
        MemcachedManager: sinon.stub().callsFake(() => {return cacheManager;}),
        BigQueryManager: sinon.stub().callsFake(()=> {return bqManager;}),
        SqlLoader: sinon.stub().callsFake(()=> {return sqlLoader;}),
      },
    });
    app = express();
    api(app);
    request = supertest(app);
  });

  afterEach(() => {
    sandbox.restore();
    sandbox.reset();
  });
  it('it should successfully stub the dependencies', () => {
    cacheManager.createKey().should.equal(`__test__${token}${attrId}${from}`);
    cacheManager.cacheResults.should.exist;
    cacheManager.removeCache.should.exist;
    cacheManager.get.should.exist;
    cacheManager.set.should.exist;
    bqManager.start.should.exist;
    bqManager.fetchNext.should.exist;
    bqManager.getOptions().should.deep.equal(queryOptions);
    bqManager.isComplete().should.equal(false);
  })
  it('we can make a get request at the api endpoint', (done) => {
    request
        .get('/fetch_latest')
        .query({
          app_id: 'com.eduapp4syria.feedthemonsterENGLISH',
          attribution_id: 'referral_source_8675309',
          from: 0,
        })
        .expect('Content-Type', /json/)
        .expect(200)
        .end(done);
  });
  it('we can successfully omit the referral id if desired', (done) => {
    request
        .get('/fetch_latest')
        .query({
          app_id: 'com.eduapp4syria.feedthemonsterENGLISH',
          from: 0,
        })
        .expect(200)
        .end(done);
      });
  it('we receive a 400 error if we omit the app id', (done) => {
    request
        .get('/fetch_latest')
        .query({
          attribution_id: 'referral_source_8675309',
          from: 0,
        })
        .expect(400)
        .end(done);
  });
  it('we receive a 400 error if we omit the cursor', (done) => {
    request
        .get('/fetch_latest')
        .query({
          app_id: 'com.eduapp4syria.feedthemonsterENGLISH',
          attribution_id: 'referral_source_8675309',
        })
        .expect(400)
        .end(done);
  });
  it('we receive a 404 error if we submit a missing app id', (done) => {
    request
        .get('/fetch_latest')
        .query({
          app_id: 'com.missing.pkg',
          attribution_id: 'referral_source_8675309',
          from: 0,
        })
        .expect(404)
        .end(done);
  });
  it('we receive a 400 error if we submit an improperly formatted app id', (done) => {
    request
        .get('/fetch_latest')
        .query({
          app_id: 'fake-pkg',
          attribution_id: 'referral_source_8675309',
          from: 0,
        })
        .expect(400)
        .end(done);
  });
  it('we receive a 400 error if we submit an improperly formatted cursor', (done) => {
    request
        .get('/fetch_latest')
        .query({
          app_id: 'fake-pkg',
          attribution_id: 'referral_source_8675309',
          from: 'A75A&FD569^&',
        })
        .expect(400)
        .end(done);

  });
  it('we receive a 500 error if BigQuery fails to fetch data', (done) => {
    bqManager.start = sandbox.stub().throws('auth failure');
    request
        .get('/fetch_latest')
        .query({
          app_id: 'com.eduapp4syria.feedthemonsterENGLISH',
          attribution_id: 'referral_source_8675309',
          from: 0,
        })
        .expect(500)
        .end(done);
  });
  it('the data we receive are properly formatted', (done) => {
    let expected = resultSet.map((row)=> {
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
            source: 'no-source',
            data: {
              advertising_id: row.device.advertising_id,
            },
          },
        },
        event: {
          name: bqParser.parseName(row.action),
          date: row.event_date,
          timestamp: row.event_timestamp,
          value_type: bqParser.getValueType(row.label),
          value: bqParser.getValue(row.label) || row.val,
          level: bqParser.getLevel(row.screen) || row.label.split('_')[1],
          profile: bqParser.getProfile(row.screen) || 'unknown',
          rawData: {
            action: row.action,
            label: row.label,
            screen: row.screen,
            value: row.value,
          }
        },
      };
    });
    request
        .get('/fetch_latest')
        .query({
          app_id: 'com.eduapp4syria.feedthemonsterENGLISH',
          attribution_id: 'referral_source_8675309',
          from: 0,
        })
        .expect(200)
        .end((err, res)=> {
          if (err) return done(err);
          res.body.data.should.deep.equal(expected);
          done();
        })
  });
  it('we receive a cursor when there is more data', (done) => {
    request
      .get('/fetch_latest')
      .query({
        app_id: 'com.eduapp4syria.feedthemonsterENGLISH',
        attribution_id: 'referral_source_8675309',
        from: 0,
      })
      .expect(200)
      .end((err, res)=> {
        if (err) return done(err);
        res.body.nextCursor.should.equal(encodeURIComponent(`${jobId}/${token}`));
        done();
      })
  });
  it('we receive no cursor when there is no more data', (done) => {
    bqManager.start.callsArgWith(0, resultSet, null, null);
    request
        .get('/fetch_latest')
        .query({
          app_id: 'com.eduapp4syria.feedthemonsterENGLISH',
          attribution_id: 'referral_source_8675309',
          from: 0,
        })
        .expect(200)
        .end((err, res)=> {
          if (err) return done(err);
          should.equal(res.body.nextCursor, null);
          done();
        })
    });

  it('we receive a second subset of data when passing the returned cursor', (done) => {
    let secondResults = queryResults.set4
    let expected = secondResults.map((row) => {
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
            source: 'no-source',
            data: {
              advertising_id: row.device.advertising_id,
            },
          },
        },
        event: {
          name: bqParser.parseName(row.action),
          date: row.event_date,
          timestamp: row.event_timestamp,
          value_type: bqParser.getValueType(row.label),
          value: bqParser.getValue(row.label) || row.val,
          level: bqParser.getLevel(row.screen) || row.label.split('_')[1],
          profile: bqParser.getProfile(row.screen) || 'unknown',
          rawData: {
            action: row.action,
            label: row.label,
            screen: row.screen,
            value: row.value,
          }
        },
      };
    });
    bqManager.fetchNext.callsArgWith(0, secondResults, null, null);
    request
      .get('/fetch_latest')
      .query({
        app_id: 'com.eduapp4syria.feedthemonsterENGLISH',
        attribution_id: 'referral_source_8675309',
        from: 0,
      })
      .expect(200)
      .end((err, res)=> {
        if (err) return done(err);
        console.log("beep");
        request
          .get('/fetch_latest')
          .query({
            app_id: 'com.eduapp4syria.feedthemonsterENGLISH',
            attribution_id: 'referral_source_8675309',
            from: 0,
            token: res.body.nextCursor,
          })
          .expect(200)
          .end((err, fi) => {
            if (err) return done(err);
            console.log("boop");
            fi.body.data.should.deep.equal(expected);
            done();
          });
      });
  });
  it('we do not receive the same cursor when submitting a returned cursor', (done) => {
    let secondResults = queryResults.set4
    let expected = secondResults.map((row) => {
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
            source: 'no-source',
            data: {
              advertising_id: row.device.advertising_id,
            },
          },
        },
        event: {
          name: bqParser.parseName(row.action),
          date: row.event_date,
          timestamp: row.event_timestamp,
          value_type: bqParser.getValueType(row.label),
          value: bqParser.getValue(row.label) || row.val,
          level: bqParser.getLevel(row.screen) || row.label.split('_')[1],
          profile: bqParser.getProfile(row.screen) || 'unknown',
          rawData: {
            action: row.action,
            label: row.label,
            screen: row.screen,
            value: row.value,
          }
        },
      };
    });
    bqManager.fetchNext.callsArgWith(0, secondResults, null, null);
    request
      .get('/fetch_latest')
      .query({
        app_id: 'com.eduapp4syria.feedthemonsterENGLISH',
        attribution_id: 'referral_source_8675309',
        from: 0,
      })
      .expect(200)
      .end((err, res)=> {
        if (err) return done(err);

        console.log(`cursor: ${res.body.nextCursor}`)
        request
          .get('/fetch_latest')
          .query({
            app_id: 'com.eduapp4syria.feedthemonsterENGLISH',
            attribution_id: 'referral_source_8675309',
            from: 0,
            token: res.body.nextCursor,
          })
          .expect(200)
          .end((err, fi) => {
            if (err) return done(err);

            should.equal(fi.body.nextCursor, null);
            done();
          });
      })
  })
});
