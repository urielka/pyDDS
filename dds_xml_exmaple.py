import sys
import time
import random

import dds

PUB_ROLE = 0
SUB_ROLE = 1

role = ['pub', 'sub'].index(sys.argv[1])

if role == PUB_ROLE:
    participant = dds.DDS('MyParticipantLibrary::PublicationParticipant')
    writers = []
    writers.append ((participant.lookup_datawriter_by_name('MyPublisher::HelloWorldWriter'),
     lambda: dict(sender=str(random.randrange(2**10)), message=str(random.randrange(2**10)), count=(random.randrange(1,100000)))))
else:
    participant = dds.DDS('MyParticipantLibrary::SubscriptionParticipant')
    readers = []
    readers.append ((participant.lookup_datareader_by_name('MySubscriber::HelloWorldReader')))


if role == PUB_ROLE:
    while True:
        writer, data = random.choice(writers)
        msg = data()
        print("Sending %r on %s" % (msg, writer.name))
        writer.write(msg)
        time.sleep(1)
        print("Unregister %r on %s" % (msg, writer.name))
        writer.unregister(msg)
        time.sleep(1)

else:
    while True:
        for reader in readers:
            msgList = reader.read()
            print (len(msgList))
            for msg in msgList:
                print("Received %r on %s" % (msg, reader.name))
        print('sleeping for 1 sec...')
        time.sleep(1)
