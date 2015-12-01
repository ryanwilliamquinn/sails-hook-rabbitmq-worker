const amqp = require('amqplib')

module.exports = sails => {
  return {
    identity: 'worker',
    configure() {
      if (!_.isObject(sails.config.worker)) sails.config.worker = {};
    },
    initialize(next) {
      // load up rabbit mq jobs
      const jobs = sails.config.jobs

      // connect to rabbit
      return amqp.connect('amqp://localhost').then(function(conn) {
        process.once('SIGINT', function() {
          conn.close();
        });

        // set up the channel and the delayed job exchange
        return conn.createChannel().then(function(ch) {
          const exchangeName = 'delayed_jobs'

          return ch.assertExchange(exchangeName, 'direct', {
            durable: false
          }).then(() => {

            // create a method that accepts the job name and the message data and publishes it to the appropriate queue
            sails.sendToQueue = (queueName, payload, options) => {
              ch.publish(exchangeName, queueName, new Buffer(payload), options)
            }

            // configure the workers defined in the parent project's api/jobs to consume the appropriate queues
            return Promise.all(Object.keys(jobs).map(jobName => {
              const jobData = jobs[jobName]

              // default to a durable queue, can be overridden in job definition
              const durable = jobData.durable === false ? false : true

              var ok = ch.assertQueue(jobData.name, {
                durable: durable
              });

              ok = ok.then(() => {
                return ch.bindQueue(jobData.name, exchangeName, jobData.name)
              })

              ok = ok.then(() => {
                // default to prefetch = 1, can be overridden in job definition
                const prefetchCount = jobData.prefetchCount === undefined ? 1 : jobData.prefetchCount
                ch.prefetch(prefetchCount);
              });
              ok = ok.then(() => {
                const ackWorker = message => {
                  jobData.worker(message).then(() => {
                    ch.ack(message)
                  })
                }
                ch.consume(jobData.name, ackWorker, {
                  noAck: false
                });
              });
              return ok;
            }))
          })
        })
      }).then(() => {
        next()
      }).catch(next)

    }
  }
}
