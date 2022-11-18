require('dotenv').config();
const express = require('express');
const Bee = require('bee-queue');
const Arena = require('bull-arena');
const app = express();
app.use(express.json({ limit: '1000kb' }));
app.use(express.urlencoded({ extended: true }));

async function init () {

  const queue = new Bee('primary', {
    activateDelayedJobs: true,
    redis: { url: process.env.REDIS_URL }
  });
  
  queue.process(2, async (job) => {
    console.log('Processing Job', job.id);
    await new Promise((res, rej) => {
      setTimeout(() => {
        console.log('Finishing Job', job.id);
        res(job.id);
      }, job.data.delay);
    });
  });
  
  queue.checkStalledJobs(5000, (err, numStalled) => {
    // prints the number of stalled jobs detected every 5000 ms
    console.log('Checked stalled jobs', numStalled);
  });
  
  const arena = new Arena({
    Bee,
    queues: [
      {
        type: 'bee',
        hostId: 'Bee Queue',
        delayedDebounce: 1000,
        redis: { url: process.env.REDIS_URL || 'redis://localhost:6379' },
        name: 'primary'
      }
    ]
  }, {
    disableListen: true
  });
  
  app.use('/arena', arena);
  
  app.post('/enqueue', async (req, res) => {
    const body = req.body;
    await queue.createJob(body).save();
    res.json(body);
  });
  
  app.listen(parseInt(process.env.PORT || 8000), async () => {
    console.log(`Server Listening at http://localhost:${process.env.PORT || 8000}`)
    const counts = await queue.checkHealth();
    console.log('Counts:', counts);
  });
}

init();
  