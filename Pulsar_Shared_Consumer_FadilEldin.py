import random
import time
import pulsar

client = pulsar.Client('pulsar://localhost:6650')

"""
def subscribe(self, topic, subscription_name,
              consumer_type=ConsumerType.Exclusive,
              message_listener=None,
              receiver_queue_size=1000,
              
topic = {persistent|non-persistent}://tenant/namespace/topic
ConsumerType={Shared|Exclusive|Failover}
"""

consumer = client.subscribe('topic_FadilEldin', 
                            'subscription_1',
                            pulsar.ConsumerType.Shared )

while True:
    msg = consumer.receive()
    #print("Shared: %s id=%s" % (msg.data().decode('utf-8'), msg.message_id()))
    print("Shared: %s " % (msg.data().decode('utf-8')))

    RndSleep=random.randint(100,1000)
    time.sleep(RndSleep/1000)

    consumer.acknowledge(msg)

client.close()
