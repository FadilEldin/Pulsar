
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
                            'subscription_2',
                            pulsar.ConsumerType.Failover)


while True:
    msg = consumer.receive()
    #print("FO %s id=%s" % (msg.data().decode('utf-8'), msg.message_id()), flush=True)
    print("FO %s " % (msg.data().decode('utf-8')),  flush=True)
    consumer.acknowledge(msg)

client.close()
