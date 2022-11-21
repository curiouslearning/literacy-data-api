const express = require('express');
const app = express();
const litApiRoutes = require('./api');
const config = require('./config.json');
const port = config.port || 3000;
// routes
litApiRoutes(app);

const server = app.listen(port, () => {
  console.log(`listening on ${port}`);
});

module.exports = {
  app,
  server,
};
