const assert = require('assert')
const queueName = 'testMessagePassing'

after(done => {
  sails.rabbitworker.channel.purgeQueue(queueName).then(() => {
    return sails.rabbitworker.connection.close().then(() => {
      done()
    })
  })
})

function assertEmptyQueue(done) {
  sails.rabbitworker.channel.checkQueue(queueName)
    .then(results => {
      assert.equal(results.consumerCount, 1)
      assert.equal(results.messageCount, 0)
      done()
    })
    .catch(done)
}

describe('worker exchange tests', () => {
  it('should have a queue with one consumer and be empty', done => {
    assertEmptyQueue(done)
  })

  it('should create and run a job successfully', done => {
    sails.testMessagePassingCountComplete = 0
    sails.createJob('testMessagePassing', '{"value":"test content 1"}')
    setTimeout(() => {
      assert.equal(sails.testMessagePassingCountComplete, 1)
      assertEmptyQueue(done)
    }, 100)
  })

  it('should create a job that is nacked, and still cleared from the queue', done => {
    sails.testMessagePassingCountComplete = 0
    sails.createJob('testMessagePassing', '{"value":"incorrect value that makes the worker reject"}')
    setTimeout(() => {
      assert.equal(sails.testMessagePassingCountComplete, 0)
      assertEmptyQueue(done)
    }, 1000)
  })


})
