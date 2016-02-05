'use strict'

const amqp = require('amqplib')
const _ = require('lodash')

function getConnectionUrl(options) {
  // create connection string with options:
  const protocol = options.ssl ? 'amqps' : 'amqp'
  const host = options.host || 'localhost'
  const port = options.port || 5672
  const username = options.username || 'guest'
  const password = options.password || 'guest'
  let url = `${protocol}://${username}:${password}@${host}:${port}`
  if (options.vhost) {
    url += `/${options.vhost}`
  }
  return url
}

module.exports = sails => {
  return {
    identity: 'worker',
    configure() {
      if (!_.isObject(sails.config.rabbitworker)) {
        sails.config.rabbitworker = {
          options: {}
        }
      }
    },
    initialize(next) {
      const options = sails.config.rabbitworker.options

      // default retry connection attempts to 3
      if (!_.has(options.retryConnectionAttempts)) {
        options.retryConnectionAttempts = 3
      }

      if (!_.has(options.retryDroppedConnectionAttempts)) {
        options.retryDroppedConnectionAttempts = 100
      }

      // default retry connection timeout to 10 seconds
      if (!_.has(options.retryConnectionTimeout)) {
        options.retryConnectionTimeout = 10000
      }
      connect(options)
        .then(() => next())
        .catch(next)
    }
  }
}

function connect(options) {
  const jobs = sails.config.rabbitworker.jobs
  const connectionUrl = getConnectionUrl(options)

  return amqp.connect(connectionUrl)
    .then(function(conn) {
      sails.rabbitworker = {
        connection: conn
      }

      // log connection errors, try to reconnect
      conn.on('error', err => {
        handleDroppedConnection(err, options)
      })

      // set up the channel and the delayed job exchange
      return conn.createChannel().then(function(ch) {
        const exchangeName = options.exchangeName || 'sails_jobs'
        sails.rabbitworker.channel = ch

        // log channel level errors
        ch.on('error', sails.log.error.bind(sails.log))

        // create a durable 'direct' exchange, to pass jobs to queues based on exact name match
        return ch.assertExchange(exchangeName, 'direct', {
          durable: true
        }).then(() => {
          // create a method that accepts the job name and the message data and publishes it to the appropriate queue
          sails.createJob = (queueName, payload, options) => {
            ch.publish(exchangeName, queueName, new Buffer(payload), options)
          }

          // only worry about registering workers on sails instances that are running jobs
          if (!options.runJobs) return

          return new Promise((resolve, reject) => {
            sails.after('hook:orm:loaded', function() {
              registerWorkers(ch, exchangeName, jobs).then(resolve).catch(reject)
            })
          })
        })
      })
    })
    .catch(err => {
      if (err.code === 'ECONNREFUSED') {
        return handleConnectionError(err, options)
          .then(() => {
            sails.log.info('Reconnected to rabbitmq')
          })
      } else {
        throw err
      }
    })

}


/**
 * configure the workers defined in the parent project's api/jobs directory to consume the appropriate queues
 *
 */
function registerWorkers(channel, exchangeName, jobs) {
  return Promise.all(Object.keys(jobs).map(jobName => {
    const jobData = jobs[jobName]

    // default to a durable queue, can be overridden in job definition
    const durable = jobData.durable === false ? false : true

    // create a queue based on job name (if it already exists nothing happens)
    return channel.assertQueue(jobData.name, {
      durable: durable
    }).then(() => {
      // bind the queue to the proper exchange
      return channel.bindQueue(jobData.name, exchangeName, jobData.name)
    }).then(() => {
      // default to prefetch = 1, can be overridden in job definition
      const prefetchCount = jobData.prefetchCount === undefined ? 1 : jobData.prefetchCount
      channel.prefetch(prefetchCount)
    }).then(() => {
      // get wrapped worker function that automatically handles ack/nack
      const ackWorker = createWrappedWorker(channel, jobData)

      channel.consume(jobData.name, ackWorker, {
        noAck: false
      })
    })
  }))
}


/**
 * wrap worker function to include ack/nack automatically (with a single retry)
 */
function createWrappedWorker(channel, jobData) {
  return function(message) {
    jobData.worker(message).then(() => {
      channel.ack(message)
    }).catch(err => {
      //TODO do we have to json.parse the message here?
      if (message.fields.redelivered) {
        // nack and don't requeue
        channel.nack(message, false, false)
      } else {
        // nack and requeue
        channel.nack(message, false, true)
      }
    })
  }
}

function handleDroppedConnection(err, options) {
  sails.log.error('Connection to rabbitmq dropped')
  options.retryConnectionAttempts = options.retryDroppedConnectionAttempts
  handleConnectionError(err, options)
}

function handleConnectionError(err, options) {
  const retryAttempts = options.retryConnectionAttempts
  sails.log.error('Problem connecting to rabbitmq server', err)
  if (retryAttempts > 0) {
    sails.log.info('Attempting to reconnect to rabbitmq server, retry attempts remaining: ', retryAttempts)
    options.retryConnectionAttempts = retryAttempts - 1
    return new Promise(resolve => {
      setTimeout(() => {
        resolve(connect(options))
      }, options.retryConnectionTimeout)
    })
  } else {
    sails.log.info('Unable to connect to rabbitmq server')
    throw err
  }
}
