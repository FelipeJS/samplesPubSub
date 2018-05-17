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
 * This application demonstrates how to perform basic operations on topics with
 * the Google Cloud Pub/Sub API.
 *
 * For more information, see the README.md under /pubsub and the documentation
 * at https://cloud.google.com/pubsub/docs.
 */

'use strict';

function listAllTopics() {
 // [START pubsub_list_subscriptions]
  // Imports the Google Cloud client library
  const PubSub = require(`@google-cloud/pubsub`);

  // Creates a client
  const pubsub = new PubSub({
    projectId: 'pubsubbroker-204320',
    keyFilename: 'PubSubBroker-b89b64437c1e.json'
  })

  // Lists all topics in the current project
  pubsub
    .getTopics()
    .then(results => {
      const topics = results[0];

      console.log('Topics:');
      topics.forEach(topic => console.log(topic.name));
    })
    .catch(err => {
      console.error('ERROR:', err);
    });
  // [END pubsub_list_topics]
}

function createTopic(topicName) {
  // [START pubsub_create_topic]
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

  // Creates a new topic
  pubsub
    .createTopic(topicName)
    .then(results => {
      const topic = results[0];
      console.log(`Topic ${topic.name} created.`);
    })
    .catch(err => {
      console.error('ERROR:', err);
    });
  // [END pubsub_create_topic]
}

function deleteTopic(topicName) {
  // [START pubsub_delete_topic]
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

  // Deletes the topic
  pubsub
    .topic(topicName)
    .delete()
    .then(() => {
      console.log(`Topic ${topicName} deleted.`);
    })
    .catch(err => {
      console.error('ERROR:', err);
    });
  // [END pubsub_delete_topic]
}

function publishMessage(topicName, data) {
  // [START pubsub_publish_message]
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
  // const topicName = 'your-topic';
  // const data = JSON.stringify({ foo: 'bar' });

  // Publishes the message as a string, e.g. "Hello, world!" or JSON.stringify(someObject)
  const dataBuffer = Buffer.from(data);

  pubsub
    .topic(topicName)
    .publisher()
    .publish(dataBuffer)
    .then(messageId => {
      console.log(`Message ${messageId} published.`);
    })
    .catch(err => {
      console.error('ERROR:', err);
    });
  // [END pubsub_publish_message]
}


let publishCounterValue = 1;

function getPublishCounterValue() {
  return publishCounterValue;
}

function setPublishCounterValue(value) {
  publishCounterValue = value;
}

function publishOrderedMessage(topicName, data) {
  // [START pubsub_publish_ordered_message]
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
  // const topicName = 'your-topic';
  // const data = JSON.stringify({ foo: 'bar' });

  // Publishes the message as a string, e.g. "Hello, world!" or JSON.stringify(someObject)
  const dataBuffer = Buffer.from(data);

  const attributes = {
    // Pub/Sub messages are unordered, so assign an order ID and manually order messages
    counterId: `${getPublishCounterValue()}`,
  };

  // Publishes the message
  return pubsub
    .topic(topicName)
    .publisher()
    .publish(dataBuffer, attributes)
    .then(results => {
      const messageId = results;

      // Update the counter value
      setPublishCounterValue(parseInt(attributes.counterId, 10) + 1);

      console.log(`Message ${messageId} published.`);

      return messageId;
    })
    .catch(err => {
      console.error('ERROR:', err);
    });
  // [END pubsub_publish_ordered_message]
}


module.exports = {publishOrderedMessage};

const cli = require(`yargs`)
  .demand(1)
  .command(
    `list`,
    `Lists all topics in the current project.`,
    {},
    listAllTopics
  )
  .command(`create <topicName>`, `Creates a new topic.`, {}, opts =>
    createTopic(opts.topicName)
  )
  .command(`delete <topicName>`, `Deletes a topic.`, {}, opts =>
    deleteTopic(opts.topicName)
  )
  .command(
    `publish <topicName> <message>`,
    `Publishes a message to a topic.`,
    {},
    opts => {
      publishMessage(opts.topicName, opts.message);
    }
  )
  
  .example(`node $0 list`)
  .example(`node $0 create my-topic`)
  .example(`node $0 delete my-topic`)
  .example(`node $0 publish my-topic "Hello, world!"`)
  .wrap(120)
  .recommendCommands()
  .epilogue(`For more information, see https://cloud.google.com/pubsub/docs`);

if (module === require.main) {
  cli.help().strict().argv; // eslint-disable-line
}
