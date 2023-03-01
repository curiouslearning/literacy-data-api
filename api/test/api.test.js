const { expect } = require('chai')
const express = require('express')
const routes = require('../src/api')
const request = require('supertest');


describe('/fetch_latest', () => {

  it('Returns 400 if missing query params', async () => {
    const app = express()
    routes(app)

    const response = await request(app)
      .get('/fetch_latest')
      .set('Accept', 'application/json')
      .expect('Content-Type', /json/)
      .expect(400);

    expect(response.body.msg).to.contain('app id')
  })
})
