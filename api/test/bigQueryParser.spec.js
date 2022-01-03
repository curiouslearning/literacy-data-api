
const sinon = require('sinon');
const proxyquire = require('proxyquire').noPreserveCache();
const supertest = require('supertest');
const express = require('express');
const sandbox = sinon.createSandbox();
const fixtures = require('./fixtures/parserFixtures.json');
const {BigQueryParser} = require('../src/helperClasses.js');
const config = require('../src/config');

beforeEach(()=> {});

afterEach(()=> {
  sandbox.restore();
});

describe('BigQueryHelper', () => {
  let bqParser = new BigQueryParser(config.sourceMapping);
  beforeEach(() => {

  });

  afterEach(() => {
    sandbox.restore();
  });

  describe('parseName', () => {

  });

  describe('getLevel', () => {
    it('should successfully parse the string', () => {
      const row = fixtures[0].row;
      const expected = fixtures[0].expected;
      let actual = bqParser.getLevel(row.screen);
      actual.should.equal(expected.event.level);
    });

    it('should return null on an improperly formatted string', () => {
      const row = fixtures[6].row;
      const actual = bqParser.getLevel(row.screen);
      expect(actual).to.be.null;
    });

    it('should return null on an improperly formatted string', () => {
      const row = fixtures[2].row;
      const actual = bqParser.getLevel(row.screen);
      expect(actual).to.be.null;
    });

    it('should return null on an improperly formatted string', () => {
      const screen = "Monster Select";
      const actual = bqParser.getLevel(screen);
      expect(actual).to.be.null
    });

    it('should return the level number', () => {
      const row = fixtures[2].row;
      const expected = fixtures[2].expected;
      const actual = bqParser.getLevel(row.label);
      actual.should.equal(expected.event.level);
    });

    it('should return the level number', () => {
      const row = fixtures[6].row;
      const expected = fixtures[6].expected;
      const actual = bqParser.getLevel(row.action);
      actual.should.equal(expected.event.level);
    });
    it('should return the level number', () => {
      const row = fixtures[7].row;
      const expected = fixtures[7].expected;
      const actual = bqParser.getLevel(row.action);
      actual.should.equal(expected.event.level);
    });

    it('should return null on improperly formatted input', () => {
      const row = fixtures[6].row;
      const expected = fixtures[6].expected;
      const actual = bqParser.getLevel(row.label);
      expect(actual).to.be.null;
    });

    it('should return null on improperly formatted input', () => {
      const label = "fjhwkdcagds";
      const actual = bqParser.getLevel(label);
      expect(actual).to.be.null;
    });
  });

  describe('getProfile', () => {
    it('should successfully parse the string', () => {
      const row = fixtures[0].row;
      const expected = fixtures[0].expected;
      let actual = bqParser.getProfile(row.screen);
      actual.should.equal(expected.event.profile);
    });

    it('should return "unknown" on an improperly formatted string', () => {
      const row = fixtures[6].row;
      const actual = bqParser.getProfile(row.screen);
      expect(actual).to.equal("unknown");
    });

    it('should return "unknown" on an improperly formatted string', () => {
      const row = fixtures[2].row;
      const actual = bqParser.getProfile(row.screen);
      expect(actual).to.equal("unknown");
    });
  });

  describe('getValueType', ()=>{
    it('should return the stimulus type', () => {
      const row = fixtures[5].row;
      const expected = fixtures[5].expected;
      const actual = bqParser.getValueType(row.label);
      actual.should.equal(expected.event.value_type);
    });
    it('should return "puzzles"', () => {
      const row = fixtures[4].row;
      const actual = bqParser.getValueType(row.label);
      actual.should.equal("puzzles");
    });
    it('should return "seconds"', () => {
      const row = fixtures[0].row;
      const actual = bqParser.getValueType(row.label);
      actual.should.equal("seconds");
    });
    it('should return "seconds"', () => {
      const row = fixtures[1].row;
      const actual = bqParser.getValueType(row.label);
      actual.should.equal("seconds");
    });
    it('should return "days"', () => {
      const row = fixtures[8].row;
      const actual = bqParser.getValueType(row.label);
      actual.should.equal('days');
    });
    it('should return "Monster Level"', () => {
      const row = fixtures[2].row;
      const actual = bqParser.getValueType(row.label);
      actual.should.equal('Monster Level');
    });
    it('should return "null"', () => {
      const label = 'fhwkqgads';
      const actual = bqParser.getValueType(label);
      expect(actual).to.be.null;
    });
  });

  describe('getValue', () => {

    it('should return the stimulus', () => {
      const row = fixtures[5].row;
      const expected = fixtures[5].expected;
      const actual = bqParser.getValue(row.label);
      actual.should.equal(expected.event.value);
    });

    it('should return the number of puzzles', () => {
      const row = fixtures[4].row;
      const expected = fixtures[4].expected;
      const actual = bqParser.getValue(row.label);
      actual.should.equal(expected.event.value);
    });

    it('should return null on a non-value label', () => {
      const row = fixtures[2].row;
      const expected = fixtures[2].expected;
      const actual = bqParser.getValue(row.label);
      expect(actual).to.be.null;
    });
  });

  describe('getSource', () => {
    it('should return "no-source"', () => {
      const row = fixtures[0].row;
      const actual = bqParser.getSource(row.attribution_id);
      actual.should.equal('no-source');
    });
    it('should return "Facebook"', () => {
      const attr = "FB_Language_App_6_2022";
      const actual = bqParser.getSource(attr);
      actual.should.equal('Facebook');
    });
    it('should return "Google"', () => {
      const attr = "Google_Bangla_App_Test";
      const actual = bqParser.getSource(attr);
      actual.should.equal('Google');
    });
    it('should return "direct"', () => {
      const attr = "(direct)";
      const actual = bqParser.getSource(attr);
      actual.should.equal('direct');
    });
  });

  describe('deduplicateData', () => {
    it('should remove duplicate rows', () => {
      const rows = fixtures.map((row) => {return row.row});
      const data = rows.concat(rows);
      const result = bqParser.deduplicateData(data);
      result.should.deep.equal(rows);
    });
    it('should not change an array of unique objects', () => {
      const rows = fixtures.map((row) => {return row.row});
      const result = bqParser.deduplicateData(rows);
      result.should.deep.equal(rows);
    });
  });

  describe('formatRowsToJson', () => {
    it('should parse data correctly', () => {
      const rows = fixtures.map((row) => {return row.row});
      const expected = fixtures.map((row) => {return row.expected});
      const actual = bqParser.formatRowsToJson(rows);
      actual.should.deep.equal(expected);
    });

  });
});
