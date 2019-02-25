import pulsar
import random
import time

client = pulsar.Client('pulsar://localhost:6650')
#{persistent|non-persistent}://tenant/namespace/topic
producer = client.create_producer('persistent://tenant/namespace/NS1/topic_FadilEldin')

#for i in range(10):
i=1
while True:
    RndBurst=random.randint(1,25)
    for j in range(RndBurst):
        print ("Sending  %d msgs msg : %d total %d" % (RndBurst,j,i))
        producer.send(('Hello Fadil Eldin       %d' % i).encode('utf-8'))
        i=i+1

    print("------------------------")
    RndSleep=random.randint(1,10)
    time.sleep(RndSleep)

client.close()
