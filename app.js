const express = require('express')
const cors = require('cors')

const routes = require('./routes/index.js')

//  Express API Definition
module.exports = (os_client, mongo_client) => {
  const app = express()

  // Middleware
  app.use(cors())

  //  Add More Routes
  for(const [route, handler] of routes)
    app.use(route, handler(os_client, mongo_client))

  //  Index Route
  app.get('/', async (req, res) => {
    const status_obj = {
      status : "ok",
      info : "Service is running",
    };

    res.send(status_obj).end()
  });

  app.get('/memo-key', async (req, res) => {
    const status_obj = {
      status : "ok",
      pubKey : process.env.ActionChain_PUB_MEMO,
    };
    res.send(status_obj).end()
  })

  return app;
}
