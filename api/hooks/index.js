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
  const heartbeat = options.heartbeat
  let url = `${protocol}://${username}:${password}@${host}:${port}`

  if (options.vhost) {
    url += `/${options.vhost}`
  }

  if (heartbeat) {
    url += `?heartbeat=${heartbeat}`
  }

  sails.log.verbose(`Rabbitmq connection url: ${url}`)

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
      if (!_.has(options, 'retryConnectionAttempts')) {
        options.retryConnectionAttempts = 3
      }

      if (!_.has(options, 'retryDroppedConnectionAttempts')) {
        options.retryDroppedConnectionAttempts = 100
      }

      // default retry connection timeout to 10 seconds
      if (!_.has(options, 'retryConnectionTimeout')) {
        options.retryConnectionTimeout = 10000
      }

      // default retry failed jobs
      if (!_.has(options, 'retryFailedJobs')) {
        options.retryFailedJobs = true
      }



      connect(options)
        .catch(err => {
          return handleDroppedConnection(err, options)
        })
        .then(() => next())
        .catch(next)
    }
  }
}

function setupChannel(conn, options, jobs) {
  // set up the channel and the delayed job exchange
  return conn.createChannel().then(function(ch) {
    const exchangeName = options.exchangeName || 'sails_jobs'
    sails.rabbitworker.channel = ch

    // log channel level errors
    ch.on('error', err => {
      sails.log.error(err)
    })

    ch.on('close', () => {
      sails.log.verbose('Rabbitmq channel closed')
    })

    // create a durable 'direct' exchange, to pass jobs to queues based on exact name match
    return ch.assertExchange(exchangeName, 'direct', {
      durable: true
    }).then(() => {
      // create a method that accepts the job name and the message data and publishes it to the appropriate queue
      sails.createJob = (queueName, payload, options) => {
        options = Object.assign({
          persistent: true
        }, options)
        sails.rabbitworker.channel.publish(exchangeName, queueName, new Buffer(payload), options)
      }

      // only worry about registering workers on sails instances that are running jobs
      if (!options.runJobs) return

      return new Promise((resolve, reject) => {
        sails.after('hook:orm:loaded', function() {
          registerWorkers(ch, exchangeName, jobs, options).then(resolve).catch(reject)
        })
      })
    })
  })
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

      return setupChannel(conn, options, jobs)

    })

}


/**
 * configure the workers defined in the parent project's api/jobs directory to consume the appropriate queues
 *
 */
function registerWorkers(channel, exchangeName, jobs, options) {
  return Promise.all(Object.keys(jobs).map(jobName => {
    const jobData = jobs[jobName]

    // default to a durable queue, can be overridden in job definition
    const durable = jobData.durable === false ? false : true

    // create a queue based on job name (if it already exists nothing happens)
    return channel.assertQueue(jobData.name, {
      durable
    }).then(() => {
      // bind the queue to the proper exchange
      return channel.bindQueue(jobData.name, exchangeName, jobData.name)
    }).then(() => {
      // default to prefetch = 1, can be overridden in job definition
      const prefetchCount = jobData.prefetchCount === undefined ? 1 : jobData.prefetchCount
      // TODO prefetch looks like it is at the channel level, but we are redifining it per job
      channel.prefetch(prefetchCount)
    }).then(() => {
      // get wrapped worker function that automatically handles ack/nack
      const ackWorker = createWrappedWorker(channel, jobData, options)

      channel.consume(jobData.name, ackWorker, {
        noAck: false
      })
    })
  }))
}


/**
 * wrap worker function to include ack/nack automatically (with a single retry)
 */
function createWrappedWorker(channel, jobData, options) {
  return function(message) {
    jobData.worker(message).then(() => {
      // channel.ack does not return a promise
      channel.ack(message)
    }).catch(err => {
      if (err.message === 'Channel closed') {
        sails.log.error(`Rabbitmq channel closed during job processing for ${message.fields.routingKey}, job will be requeued`)
        return
      }

      if (message.fields.redelivered || !options.retryFailedJobs) {
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
  return handleConnectionError(err, options)
    .then(() => {
      sails.log.info('Reconnected to rabbitmq')
    })
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
    .catch(() => {
      return handleConnectionError(err, options)
    })
  } else {
    sails.log.info('Unable to connect to rabbitmq server')
    return Promise.reject(err)
  }
}
