/**
 * Before running any API tests, get Sails running.
 *
 * NOTICE:
 * This exposes the `sails` global.
 *
 * @framework mocha
 */

const queueName = 'testMessagePassing'

before(function(done) {
  require('sails').lift({
    host: 'localhost',
    port: 1440,
    log: {
      level: 'silent'
    },
    hooks: {
      i18n: false,
      grunt: false
    },
    rabbitworker: {
      options: {
        runJobs: true,
        retryConnectionTimeout: 500,
        retryFailedJobs: false
      },
      jobs: {
        testMessagePassing: {
          name: 'testMessagePassing',
          worker(msg) {
            const content = JSON.parse(msg.content)

            // check that the message content is coming through as expected
            if (content.value === 'test content 1') {
              sails.testMessagePassingCountComplete++
              return Promise.resolve()
            }

            // ensure that rejected messages eventually get removed from the queue
            return Promise.reject(new Error('Bad request'))
          }
        }
      }
    }
  }, done);
});

after(done => {
  sails.rabbitworker.channel.purgeQueue(queueName).then(() => {
    return sails.rabbitworker.connection.close().then(() => {
      sails.log.info('Purged queues and closed rabbit connection after tests complete')
      done()
    })
  })
})
