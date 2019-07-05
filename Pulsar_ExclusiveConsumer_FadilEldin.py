
import pulsar

client = pulsar.Client('pulsar://localhost:6650')

"""
def subscribe(self, topic, subscription_name,
              consumer_type=ConsumerType.Exclusive,
              message_listener=None,
              receiver_queue_size=1000,
              
topic = {persistent|non-persistent}://tenant/namespace/topic
ConsumerType={Shared|Exclusive|Failover}
              Exclusive is the default, may be removed from below
"""

consumer = client.subscribe('topic_FadilEldin', 
                            'subscription_3',
                            pulsar.ConsumerType.Exclusive)


while True:
    msg = consumer.receive()
    #print("EX %s id=%s" % (msg.data().decode('utf-8'), msg.message_id()))
    print("EX %s " % (msg.data().decode('utf-8')))
    consumer.acknowledge(msg)

client.close()
