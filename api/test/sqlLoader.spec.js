const sinon = require('sinon');
const proxyquire = require('proxyquire');
const sandbox = sinon.createSandbox();

describe('SqlLoader', () => {
  let paths, queryStrings, readStub;

  beforeEach(() => {
    paths = {
      fakeQuery1: 'fakeQuery1.sql',
      fakeQuery2: 'fakeQuery2.sql',
      fakeQuery3: 'fakeQuery3.sql',
      fakeQuery4: 'fakeQuery4.sql',
      fakeQuery5: 'fakeQuery5.sql',
      fakeQuery6: 'fakeQuery6.sql',
    }
    queryStrings = [ //return nodeJS buffers to test string conversion
      Buffer.from('fake-query-string 1'),
      Buffer.from('fake-query-string 2'),
      Buffer.from('fake-query-string 3'),
      Buffer.from('fake-query-string 4'),
      Buffer.from('fake-query-string 5'),
      Buffer.from('fake-query-string 6'),
    ]
    readStub = sandbox.stub();
    readStub.withArgs(sinon.match(paths.fakeQuery1)).returns(queryStrings[0]);
    readStub.withArgs(sinon.match(paths.fakeQuery2)).returns(queryStrings[1]);
    readStub.withArgs(sinon.match(paths.fakeQuery3)).returns(queryStrings[2]);
    readStub.withArgs(sinon.match(paths.fakeQuery4)).returns(queryStrings[3]);
    readStub.withArgs(sinon.match(paths.fakeQuery5)).returns(queryStrings[4]);
    readStub.withArgs(sinon.match(paths.fakeQuery6)).returns(queryStrings[5]);
  });
  afterEach(() => {
    sandbox.restore();
    sandbox.reset();
  });


  const {SqlLoader} = proxyquire('../src/helperClasses', {
    'fs': {
      readFileSync: (path, encoding) => {
        return readStub(path, encoding);
      },
    },
  });

  it('we successfully stub the dependency', () => {
    readStub.should.exist;
    readStub(`./${paths.fakeQuery1}`, '').should.equal(queryStrings[0]);
    readStub(`./${paths.fakeQuery2}`, '').should.equal(queryStrings[1]);
    readStub(`./${paths.fakeQuery3}`, '').should.equal(queryStrings[2]);
    readStub(`./${paths.fakeQuery4}`, '').should.equal(queryStrings[3]);
    readStub(`./${paths.fakeQuery5}`, '').should.equal(queryStrings[4]);
    readStub(`./${paths.fakeQuery6}`, '').should.equal(queryStrings[5]);
  });
  it('we can initialize the class', () => {
    const test = new SqlLoader(paths);
    test.fileStrings.should.exist;
  });
  it('we can provide optional basePath and encoding arguments', ()=> {
    const test = new SqlLoader(paths, './testPath', 'utf-9');
    test.basePath.should.equal('./testPath');
    test.encoding.should.equal('utf-9');
  });
  it('we get an error if we omit the paths object', () => {
    try {
      const test = new SqlLoader();
      throw new Error('test failed');
    } catch (e) {
      e.message.should.equal('you must provide an array or map of paths to sql files');
    }
  });
  it('the class successfully reads in the files in the map', () => {
    let expected ={};
    for (let path in paths) {
      expected[paths[path]] = readStub(paths[path]).toString();
    }
    const test = new SqlLoader(paths);
    test.fileStrings.should.deep.equal(expected);
  });
  it('we can pass an array instead of a map', () => {
    const testArray = Object.values(paths);
    let expected = {};
    for (let path in paths) {
      expected[paths[path]] = readStub(paths[path]).toString();
    }
    const test = new SqlLoader(testArray);
    test.fileStrings.should.deep.equal(expected);
  });
  it('we receive the the first query string', () => {
    const test = new SqlLoader(paths);
    const expected = queryStrings[0].toString();
    test.getQueryString(paths.fakeQuery1).should.equal(expected);
  });
  it('we receive the the second query string', () => {
    const test = new SqlLoader(paths);
    const expected = queryStrings[1].toString();
    test.getQueryString(paths.fakeQuery2).should.equal(expected);
  });
  it('we receive the the third query string', () => {
    const test = new SqlLoader(paths);
    const expected = queryStrings[2].toString();
    test.getQueryString(paths.fakeQuery3).should.equal(expected);
  });
  it('we receive the the fourth query string', () => {
    const test = new SqlLoader(paths);
    const expected = queryStrings[3].toString();
    test.getQueryString(paths.fakeQuery4).should.equal(expected);
  });
  it('we receive the the fifth query string', () => {
    const test = new SqlLoader(paths);
    const expected = queryStrings[4].toString();
    test.getQueryString(paths.fakeQuery5).should.equal(expected);
  });
  it('we receive the the sixth query string', () => {
    const test = new SqlLoader(paths);
    const expected = queryStrings[5].toString();
    test.getQueryString(paths.fakeQuery6).should.equal(expected);
  });
  it('we receive an error if there is no matching path', () => {
    const test = new SqlLoader(paths);
    sandbox.spy (test, 'getQueryString');
    try{
      test.getQueryString('missing-query');
      throw new Error ('test failed');
    } catch (e) {
      test.getQueryString.should.have.thrown;
    }
  });
});
