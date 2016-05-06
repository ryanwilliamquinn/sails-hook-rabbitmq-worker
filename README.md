# sails-hook-rabbitmq-worker

sails hook for easy rabbitmq worker integration

### This is in beta, still working out connection error handling

Jobs are defined in api/jobs.  They should look like this:
```
// api/jobs/testJob.js
module.exports = {
    name: 'testJob', // required
    worker(msg) { // required
        console.log('worker args', msg.content.toString())
        console.log('msg.ack', msg.ack)
        return Promise.resolve()
    },
    durable: true, // optional, defaults to true
    prefetch: 3 // optional, defaults to 1
}
```
A config file is also required, it should look something like this (given values are defaults unless otherwise noted):
```
var path = require('path')

module.exports.rabbitworker = {
  options: {
    runJobs: true, // optional flag for whether this particular instance should consume jobs from the queue
    exchangeName: 'some_exchange_name',  // optional, defaults to 'sails_jobs'
    retryFailedJobs: true, // optional flag for if jobs should be retried (one additional time) if they fail
    heartbeat: 5, // optional number of seconds, defaults to no heartbeat
    retryConnectionAttempts: 3, // optional number of retry attempts for initial connection
    retryConnectionTimeout: 10000, // optional number of milliseconds before attempting to retry after a connection error
    retryDroppedConnectionAttempts: 100, // optional number of retry attempts after a dropped connection 
    host: 'localhost', // all of these connection settings are optional.  if omitted, these are the defaults.
    port: 5672,
    username: 'guest',
    password: 'guest',
    ssl: false
  },
  jobs: require('require-all')(path.join(__dirname, '../api/jobs'))  // required
}
```
Note the peer dependency on require-all.

Once this is set up, you can create a new job like this:
```
sails.createJob('nameOfTheJob', 'string message')
```


