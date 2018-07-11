import sys
import time
import random

import dds

PUB_ROLE = 0
SUB_ROLE = 1

role = ['pub', 'sub'].index(sys.argv[1])

if role == PUB_ROLE:
    participant = dds.DDS('MyParticipantLibrary::BigPublicationParticipant')
    HelloBigWorldWriter = participant.lookup_datawriter_by_name('MyBigPublisher::HelloBigWorldWriter')
    seq = 0
    data = "x" * 10000000 #10MB
    file = open("huge_pic.png", "rb")
    buf = file.read()
    while True:
        seq+=1

        print ('writing sequence#', seq)
        msg = {'seq':seq , 'data':"x" , 'rawBytes':buf}
        a = time.time()
        HelloBigWorldWriter.write(msg)
        print("Write speed: %f" %( time.time() -a))
        time.sleep(3)

else:
    participant = dds.DDS('MyParticipantLibrary::BigSubscriptionParticipant')
    HelloBigWorldReader = participant.lookup_datareader_by_name('MyBigSubscriber::HelloBigWorldReader')
    while True:
        t = time.time()
        msgList = HelloBigWorldReader.take()
        e = time.time() - t
        print (len(msgList))
        for msg in msgList:
            #print("Received %r on %s" % (msg, HelloBigWorldReader.name))
            print("Received %d bytes of raw data in %f seconds" % (len(msg["sampleData"]["rawBytes"]),e))
        print('sleeping for 1 sec...')
        time.sleep(1)
