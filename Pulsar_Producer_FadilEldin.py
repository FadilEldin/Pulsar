import pulsar
import random 
import time 
import sys 

client = pulsar.Client('pulsar://localhost:6650')
#{persistent|non-persistent}://tenant/namespace/topic
producer = client.create_producer('topic_FadilEldin',
                                   block_if_queue_full=True,
                                   batching_enabled=True)

#for i in range(10):
i=1
while True:
    RndBurst=random.randint(1,25)
    for j in range(RndBurst):
        print ("Producer %s  msg:%d total %d" % (sys.argv[1],j,i))
        producer.send(('Hello Fadil %s:%d' % (sys.argv[1],i)).encode('utf-8'))
        i=i+1

    print("------------------------")
    RndSleep=random.randint(1,10)
    time.sleep(RndSleep)

client.close()

