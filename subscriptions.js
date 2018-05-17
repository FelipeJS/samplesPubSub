/**
 * Copyright 2017, Google, Inc.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * This application demonstrates how to perform basic operations on
 * subscriptions with the Google Cloud Pub/Sub API.
 *
 * For more information, see the README.md under /pubsub and the documentation
 * at https://cloud.google.com/pubsub/docs.
 */

'use strict';

function listSubscriptions() {
  // [START pubsub_list_subscriptions]
  // Imports the Google Cloud client library
  const PubSub = require(`@google-cloud/pubsub`);

  // Creates a client
  const pubsub = new PubSub({
    projectId: 'pubsubbroker-204320',
    keyFilename: 'PubSubBroker-b89b64437c1e.json'
  })

  // Lists all subscriptions in the current project
  pubsub
    .getSubscriptions()
    .then(results => {
      const subscriptions = results[0];

      console.log('Subscriptions:');
      subscriptions.forEach(subscription => console.log(subscription.name));
    })
    .catch(err => {
      console.error('ERROR:', err);
    });
  // [END pubsub_list_subscriptions]
}

function listTopicSubscriptions(topicName) {
  // [START pubsub_list_topic_subscriptions]
  // Imports the Google Cloud client library
  const PubSub = require(`@google-cloud/pubsub`);

  // Creates a client
  const pubsub = new PubSub({
    projectId: 'pubsubbroker-204320',
    keyFilename: 'PubSubBroker-b89b64437c1e.json'
  })

  /**
   * TODO(developer): Uncomment the following line to run the sample.
   */
  // const topicName = 'your-topic';

  // Lists all subscriptions for the topic
  pubsub
    .topic(topicName)
    .getSubscriptions()
    .then(results => {
      const subscriptions = results[0];

      console.log(`Subscriptions for ${topicName}:`);
      subscriptions.forEach(subscription => console.log(subscription.name));
    })
    .catch(err => {
      console.error('ERROR:', err);
    });
  // [END pubsub_list_topic_subscriptions]
}

function createSubscription(topicName, subscriptionName) {
  // [START pubsub_create_subscription]
  // Imports the Google Cloud client library
  const PubSub = require(`@google-cloud/pubsub`);

  const pubsub = new PubSub({
    projectId: 'pubsubbroker-204320',
    keyFilename: 'PubSubBroker-b89b64437c1e.json'
  })

  /**
   * TODO(developer): Uncomment the following lines to run the sample.
   */
  // const topicName = 'your-topic';
  // const subscriptionName = 'your-subscription';

  // Creates a new subscription
  pubsub
    .topic(topicName)
    .createSubscription(subscriptionName)
    .then(results => {
      const subscription = results[0];
      console.log(`Subscription ${subscription.name} created.`);
    })
    .catch(err => {
      console.error('ERROR:', err);
    });
  // [END pubsub_create_subscription]
}

function deleteSubscription(subscriptionName) {
  // [START pubsub_delete_subscription]
  // Imports the Google Cloud client library
  const PubSub = require(`@google-cloud/pubsub`);

    // Creates a client
    const pubsub = new PubSub({
      projectId: 'pubsubbroker-204320',
      keyFilename: 'PubSubBroker-b89b64437c1e.json'
    })

  /**
   * TODO(developer): Uncomment the following line to run the sample.
   */
  // const subscriptionName = 'your-subscription';

  // Deletes the subscription
  pubsub
    .subscription(subscriptionName)
    .delete()
    .then(() => {
      console.log(`Subscription ${subscriptionName} deleted.`);
    })
    .catch(err => {
      console.error('ERROR:', err);
    });
  // [END pubsub_delete_subscription]
}

function listenForMessages(subscriptionName, timeout) {
  // [START pubsub_listen_messages]
  // Imports the Google Cloud client library
  const PubSub = require(`@google-cloud/pubsub`);

    // Creates a client
    const pubsub = new PubSub({
      projectId: 'pubsubbroker-204320',
      keyFilename: 'PubSubBroker-b89b64437c1e.json'
    })

  /**
   * TODO(developer): Uncomment the following lines to run the sample.
   */
  // const subscriptionName = 'your-subscription';
  // const timeout = 60;

  // References an existing subscription
  const subscription = pubsub.subscription(subscriptionName);

  // Create an event handler to handle messages
  let messageCount = 0;
  const messageHandler = message => {
    console.log(`Received message ${message.id}:`);
    console.log(`\tData: ${message.data}`);
    messageCount += 1;

    // "Ack" (acknowledge receipt of) the message
    message.ack();
  };

  // Listen for new messages until timeout is hit
  subscription.on(`message`, messageHandler);
  setTimeout(() => {
    subscription.removeListener('message', messageHandler);
    console.log(`${messageCount} message(s) received.`);
  }, timeout * 1000);
  // [END pubsub_listen_messages]
}

let subscribeCounterValue = 1;

function getSubscribeCounterValue() {
  return subscribeCounterValue;
}

function setSubscribeCounterValue(value) {
  subscribeCounterValue = value;
}

// [START pubsub_listen_ordered_messages]
const outstandingMessages = {};

function listenForOrderedMessages(subscriptionName, timeout) {
  // Imports the Google Cloud client library
  const PubSub = require(`@google-cloud/pubsub`);

  // Creates a client
  const pubsub = new PubSub();

  // References an existing subscription, e.g. "my-subscription"
  const subscription = pubsub.subscription(subscriptionName);

  // Create an event handler to handle messages
  const messageHandler = function(message) {
    // Buffer the message in an object (for later ordering)
    outstandingMessages[message.attributes.counterId] = message;

    // "Ack" (acknowledge receipt of) the message
    message.ack();
  };

  // Listen for new messages until timeout is hit
  return new Promise(resolve => {
    subscription.on(`message`, messageHandler);
    setTimeout(() => {
      subscription.removeListener(`message`, messageHandler);
      resolve();
    }, timeout * 1000);
  }).then(() => {
    // Pub/Sub messages are unordered, so here we manually order messages by
    // their "counterId" attribute which was set when they were published.
    const outstandingIds = Object.keys(outstandingMessages).map(counterId =>
      parseInt(counterId, 10)
    );
    outstandingIds.sort();

    outstandingIds.forEach(counterId => {
      const counter = getSubscribeCounterValue();
      const message = outstandingMessages[counterId];

      if (counterId < counter) {
        // The message has already been processed
        message.ack();
        delete outstandingMessages[counterId];
      } else if (counterId === counter) {
        // Process the message
        console.log(
          `* %d %j %j`,
          message.id,
          message.data.toString(),
          message.attributes
        );
        setSubscribeCounterValue(counterId + 1);
        message.ack();
        delete outstandingMessages[counterId];
      } else {
        // Have not yet processed the message on which this message is dependent
        return false;
      }
    });
  });
}
// [END pubsub_listen_ordered_messages]

module.exports = {listenForOrderedMessages};

const cli = require(`yargs`)
  .demand(1)
  .command(
    `list [topicName]`,
    `Lists all subscriptions in the current project, optionally filtering by a topic.`,
    {},
    opts => {
      if (opts.topicName) {
        listTopicSubscriptions(opts.topicName);
      } else {
        listSubscriptions();
      }
    }
  )
  .command(
    `create <topicName> <subscriptionName>`,
    `Creates a new subscription.`,
    {},
    opts => createSubscription(opts.topicName, opts.subscriptionName)
  )
  .command(`delete <subscriptionName>`, `Deletes a subscription.`, {}, opts =>
    deleteSubscription(opts.subscriptionName)
  )
  .command(
    `listen-messages <subscriptionName>`,
    `Listens to messages for a subscription.`,
    {
      timeout: {
        alias: 't',
        type: 'number',
        default: 10,
      },
    },
    opts => listenForMessages(opts.subscriptionName, opts.timeout)
  )
  
  .example(`node $0 list`)
  .example(`node $0 list my-topic`)
  .example(`node $0 create my-topic worker-1`)
  .example(`node $0 listen-messages my-subscription`)
  .example(`node $0 delete worker-1`)
  .wrap(120)
  .recommendCommands()
  .epilogue(`For more information, see https://cloud.google.com/pubsub/docs`);

if (module === require.main) {
  cli.help().strict().argv; // eslint-disable-line
}
