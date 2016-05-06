const assert = require('assert')
const queueName = 'testMessagePassing'

function assertEmptyQueue(done) {
  return sails.rabbitworker.channel.checkQueue(queueName)
    .then(results => {
      assert.equal(results.consumerCount, 1)
      assert.equal(results.messageCount, 0)
      done()
    })
    .catch(done)
}

describe('connection error handling tests', function() {
  it('should reconnect if the connection emits an error', done => {
    // assert that the queue exists
    assertEmptyQueue(() => {})
      .then(() => {
        // close the connection
        sails.rabbitworker.connection.close()
        // emit an error to let the client know something went wrong, this will initiate a reconnect
        sails.rabbitworker.connection.emit('error', new Error('Fake Connection Error'))
      })
      .then(() => {
        // give the app some time to reconnect
        return new Promise(resolve => {
          setTimeout(resolve, 1000)
        })
      })
      .then(() => {
        // assert that the connection is restored
        return assertEmptyQueue(done)
      })

      .catch(done)
  })
})
