const sinon = require('sinon');
const express = require ('express');
const proxyquire = require('proxyquire').noPreserveCache();
const fs = require('fs');
const bq =  require('@google-cloud/bigquery');
const http = require('http');
const testData = require('./testTableMap.json');
const sandbox = sinon.createSandbox();
