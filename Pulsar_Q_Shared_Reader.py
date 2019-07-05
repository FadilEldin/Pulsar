import random
import time
import pulsar

client = pulsar.Client('pulsar://localhost:6650')

"""
Note that the default queue size is 1000
If you'd like to have tight control over message dispatching across consumers, 
set the consumers' receiver queue size very low (potentially even to 0 if necessary). 
`Each Pulsar consumer has a receiver queue that determines how many messages the consumer will attempt to fetch at a time. 
A receiver queue of 1000 (the default), for example, means that the consumer will attempt 
to process 1000 messages from the topic's 
backlog upon connection. Setting the receiver queue to zero essentially means ensuring that each consumer is only doing 
one thing at a time.


https://pulsar.apache.org/api/python/
def create_reader(	self, topic, start_message_id, reader_listener=None, 
receiver_queue_size=1000, reader_name=None, subscription_role_prefix=None)
Create a reader on a particular topic

Args

topic: The name of the topic.
start_message_id: The initial reader positioning is done by specifying a message id. The options are:
MessageId.earliest: Start reading from the earliest message available in the topic
MessageId.latest: Start reading from the end topic, only getting messages published after the reader was created
MessageId: When passing a particular message id, the reader will position itself on that specific position. 
The first message to be read will be the message next to the specified messageId. 
Message id can be serialized into a string and deserialized back into a MessageId object:

# Serialize to string s = msg.message_id().serialize()

# Deserialize from string msg_id = MessageId.deserialize(s)

Options

reader_listener: Sets a message listener for the reader. When the listener is set, 
the application will receive messages through it. Calls to reader.read_next() will not be allowed. 
The listener function needs to accept (reader, message), for example:

def my_listener(reader, message):
    # process message
    pass
receiver_queue_size: Sets the size of the reader receive queue. The reader receive queue controls how 
many messages can be accumulated by the reader before the application calls read_next(). 
Using a higher value could potentially increase the reader throughput at the expense of higher memory utilization.

reader_name: Sets the reader name.
subscription_role_prefix: Sets the subscription role prefix.
"""

consumer = client.subscribe('topic_FadilEldin',
                            'subscription_1',
                            pulsar.ConsumerType.Shared )

msg_c = consumer.receive()
msg_id = msg_c.message_id()
reader= client.create_reader(
                 'topic_FadilEldin', 
                 start_message_id=msg_id,
                 receiver_queue_size=0)

while True:
    # Fadil Eldin
    # March/21/2019
    # Ran it, returned error, AttributeError: 'Reader' object has no attribute 'receive'
    msg = consumer.receive()
    #msg = reader.next()
    #print("Shared: %s id=%s" % (msg.data().decode('utf-8'), msg.message_id()))
    print("Shared: %s " % (msg.data().decode('utf-8')))

    RndSleep=random.randint(3000,5000)
    time.sleep(RndSleep/1000)

    #https://pulsar.apache.org/docs/latest/clients/Python/
    #  No acknowledgment

client.close()
