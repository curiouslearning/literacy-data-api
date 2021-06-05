const sinon = require('sinon');
const proxyquire = require('proxyquire');
const sandbox = sinon.createSandbox();

describe('CacheManager', () => {
  let memcached;
  let cache;
  let params;

  beforeEach(() => {
    cache = {
      jobId: 'fake-job',
      token: 'th1s1safak3t0k3n'
    };
   memcached = {
      set: sandbox.stub(),
      get: sandbox.stub().returns(cache),
      del: sandbox.stub()
    };
    params = {
      pkg_id: 'pkg',
      cursor: 123456789000000,
    };
  });

  const { MemcachedManager } = proxyquire('../src/helperClasses', {
    'memcached': {
      Memcached: sinon.stub().callsFake(() => {
        return memcached;
      }),
    },
  });

  afterEach(() => {
    sandbox.restore();
  });

  it('should initialize successfully', () => {
    const test = new MemcachedManager('fakeaddr');
    test.memcached.should.deep.equal(memcached);
  });
  it('should throw an error if no address specified', () => {
    try {
      const test = new MemcachedManager();
      test.should.equal(null);
    } catch (e) {
      e.message.should.equal('Please provide a cache address');
    }
  });
  it('should set a key-value pair in the cache', ()=>{
    const test = new MemcachedManager('fakeaddr');
    test.cacheResults('fake-key', cache, 3600);
    memcached.set.should.have.been.calledWith('fake-key', cache, 3600, sinon.match.func);
  });
  it('should get the corresponding cache from the key', () => {
    const test = new MemcachedManager('fakeaddr');
    test.get('fake-key', (err, data) => {
    });
    memcached.get.should.have.been.calledWith('fake-key', sinon.match.func);
  });
  it('should delete the key-value pair from the cache', ()=>{
    const test = new MemcachedManager('fakeaddr');
    test.removeCache('fake-key');
    memcached.del.should.have.been.calledWith('fake-key', sinon.match.func);
  });
  it('should create a key from the prefix and parameters', () => {
    const test = new MemcachedManager('fakeaddr');
    const newKey = test.createKey('test', params);
    newKey.should.equal(`__test__${params.pkg_id}${params.cursor}`);
  });
  it('should throw an error on bad prefix arguments', () => {
    try {
      const test = new MemcachedManager('fakeaddr');
      const newKey = test.createKey(() => {return 'test'}, params);
    } catch(e) {
      e.message.should.equal('Keys can only be made with strings or numbers!');
    }
  });
  it('should throw an error on bad key params', () => {
    params.pkg_id = true;
    try {
      const test = new MemcachedManager('fakeaddr');
      const newKey = test.createKey('test', params);
    } catch(e) {
      e.message.should.equal('Keys can only be made with strings or numbers!');
    }
  });
});
