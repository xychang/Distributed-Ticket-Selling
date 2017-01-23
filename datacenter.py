# this script handles the logic of datacenter
import json
import bisect
# import COMM
# import socket
# import time
from threading import Lock

CONFIG = json.load(open('config.json'))

class ticket_request(object):
    '''
    the ticket request is the request from the client, and
    the client is not in the logical clock system
    therefore, when we first construct the request, it does not have a time
    but when the datacenter who handles the request broad cast this request,
    it now have a time
    '''
    def __init__(self, client, ticket_count):
        self.client = client
        self.ticket_count = ticket_count
        self.collected_reply = []
        self.clock = self.datacenter_id = None
        self.known_ready = False

    def set_datacenter_id(self, datacenter_id):
        ''' set which datacenter is handling this request '''
        self.datacenter_id = datacenter_id

    def set_clock(self, clock):
        ''' set the time the request is broadcasted '''
        self.clock = clock

    def record_response(self, sender):
        ''' update the request object to reflect a received response '''
        self.collected_reply.append(sender.datacenter_id)

    def is_ready(self):
        ''' check whether the request has all response received '''
        # if we already know it's ready, respond so
        if self.known_ready: return True
        # test whether all the response are gethered
        is_ready = set(self.collected_reply + [self.datacenter_id]).\
            issuperset([int(center_id) for center_id in CONFIG['datacenters']])
        if is_ready: self.known_ready = True
        return is_ready

    # implement clock comparison for messages
    def __lt__(self, other):
        return (self.clock, self.datacenter_id) < (other.clock, other.datacenter_id)




class coordinate_reply(object):
    '''
    This class stores the information needed for a reply message
    '''
    def __init__(self, clock, datacenter_id, target_request_clock):
        self.clock = clock
        self.datacenter_id = datacenter_id
        self.target_request_clock = target_request_clock




class coordinate_release(coordinate_reply):
    '''
    This class stores the information needed for a release message
    '''
    def __init__(self, clock, datacenter_id, target_request_clock, ticket_change):
        super(coordinate_release, self).__init__(clock, datacenter_id, target_request_clock)
        self.ticket_change = ticket_change




class datacenter(object):
    '''
    This class handles the protocol logic of datacenters
    '''
    def __init__(self, datacenter_id, server):
        '''
        we use datacenter_id instead of pid because it is potentially
        deployed on multiple servers, and many have the same pid
        '''
        self.datacenter_id = datacenter_id
        self.datacenters = CONFIG['datacenters']
        # update the datacenters, so that the id and port are all int
        self.datacenters = dict([(int(x), y) for x, y in self.datacenters.items()])
        self.total_ticket = CONFIG['total_ticket']
        self.clock = 1 # the clock is to be updated on (1) receive request, (2) send request
        # keep queue of all requests
        self.request_queue = []
        # keep a pool of my requests, so as to esially update the response status
        self.request_pool = {}
        # store the server object to be used for making requests
        self.server = server
        # a lock to resure that the clock is not updated simultanously
        self.clock_lock = Lock()


    def handle_request(self, target_center_id, data):
        ''' This function is a general function for handling received message
        target_center_id: the id of the datacenter sending the message
        data: the message content '''
        # Problem data can only be string
        # if data type == ticket request
        # self.dc.handle_coordinate_request()
        # if data type == reply
        # self.dc.handle_coordinate_reply()
        # if data.type == release
        # self.dc.handle_coordinate_release()

        message_type, content = data.split(':')
        # data = 'REQUEST:{datacenter_id},{clock},{ticket_count}\n'.format(
        if message_type == 'REQUEST':
            datacenter_id, clock, ticket_count = content.split(',')
            message = ticket_request(None, int(ticket_count))
            message.set_clock(int(clock))
            message.set_datacenter_id(int(datacenter_id))
            self.handle_coordinate_request(message)
        # data = 'REPLY:{datacenter_id},{clock},{target_request_clock}\n'.format(
        elif message_type == 'REPLY':
            datacenter_id, clock, target_request_clock = content.split(',')
            message = coordinate_reply(int(clock), int(datacenter_id), int(target_request_clock))
            self.handle_coordinate_reply(message)
        # data = 'RELEASE:{datacenter_id},{clock},{request_clock},{ticket_change}\n'.format(
        elif message_type == 'RELEASE':
            datacenter_id, clock, request_clock, ticket_change = content.split(',')
            message = coordinate_release(int(clock), int(datacenter_id),
                                         int(request_clock), int(ticket_change))
            self.handle_coordinate_release(message)


    def handle_coordinate_request(self, message):
        ''' from this, we expect a ticket_request message, with the datacenter_id and clock set'''
        # update the clock upon revcieing message
        with self.clock_lock:
            clock = self.clock = max(self.clock, message.clock) + 1
        # put the message into queue
        bisect.insort(self.request_queue, message)
        # and then reply the message, containing my clock, my datacenter_id
        # and the message I am replying to (identified by message clock)
        data = 'REPLY:{datacenter_id},{clock},{target_request_clock}\n'.format(
            datacenter_id=self.datacenter_id,
            clock=clock,
            target_request_clock=message.clock
        )
        self.server.send_message(message.datacenter_id, data)
        # COMM.send_reply(self.datacenters[message.datacenter_id],
        #                 (self.clock, self.datacenter_id), message)


    def handle_ticket_request(self, message):
        '''from this, we expect a ticket_request message
        because the client is not in the logical time system, we just send out
        ticket request immediately after receiving the client message
        '''
        # update the clock upon reveing message
        with self.clock_lock:
            self.clock = self.clock + 1
            message.set_clock(self.clock)
        message.set_datacenter_id(self.datacenter_id)
        # the request is generated after all other requests in the queue
        # put it at the end
        self.request_queue.append(message)
        print self.request_queue
        self.request_pool[message.clock] = message

        # format the request message into string and then broadcast it
        # data = str(self.datacenter_id)+","+str(message.clock)+","+str(message.ticket_count)
        data = 'REQUEST:{datacenter_id},{clock},{ticket_count}\n'.format(
            datacenter_id=self.datacenter_id,
            clock=self.clock,
            ticket_count=message.ticket_count)

        self.server.broadcast_message(data)
        #COMM.send_request(message, self.clock, self.datacenter_id, conn)

    def handle_coordinate_reply(self, message):
        '''from this, we expect a coordinate_reply message '''
        # update the clock upon revcieing message
        with self.clock_lock:
            clock = self.clock = max(self.clock, message.clock) + 1
        # update the stored request
        my_request = self.request_pool[message.target_request_clock]
        my_request.record_response(message)
        # check if the request is ready, and if the request is at the top
        # of the queue
        if (my_request.is_ready()) and (my_request == self.request_queue[0]):
            self.sell_ticket(my_request)

    def handle_coordinate_release(self, message):
        '''from this, we expect a coordinate_release message '''
        # update the clock upon revcieing message
        with self.clock_lock:
            self.clock = max(self.clock, message.clock) + 1
        # update the total number of tickets
        self.total_ticket += message.ticket_change
        # update the queue to remove the request from queue
        # ideally it will be the first in queue
        # but bad things may happen
        for request in self.request_queue:
            if message.datacenter_id == request.datacenter_id and \
                message.target_request_clock == request.clock:
                self.request_queue.remove(request)
                break

        # check if the first request is mine, and if it's ready
        # if I have no request, quit
        if len(self.request_pool) == 0: return
        if self.request_queue[0].datacenter_id == self.datacenter_id and \
           self.request_queue[0].is_ready():
            self.sell_ticket(self.request_queue[0])

    def sell_ticket(self, my_request):
        ''' now we are going to sell our ticket, and then release the hold'''
        if my_request.ticket_count <= self.total_ticket:
            change = - my_request.ticket_count
        else:
            change = 0
        # if the change is zero, then the transaction failed
        self.total_ticket += change
        my_request.ticket_change = change

        #COMM.reply_client(my_request.client, change != 0)
        if change == 0:
            my_request.client.send(
                "Cannot sell ticket, total_ticket left {}\n".format(self.total_ticket))
        else:
            my_request.client.send(
                "Selling successful, total_ticket left {}\n".format(self.total_ticket))
        my_request.client.close()

        # after handling the current request, remove it from the top of the queue
        self.request_queue.remove(my_request)
        del self.request_pool[my_request.clock]

        # send the release message to all peers
        # format a release message
        # request clock here is used to identify the request being released
        # before we sell the ticket, we need to make sure that our local clock
        # is updated because of this operation
        with self.clock_lock:
            clock = self.clock = self.clock + 1

        data = 'RELEASE:{datacenter_id},{clock},{request_clock},{ticket_change}\n'.format(
            datacenter_id=self.datacenter_id,
            clock=clock,
            request_clock=my_request.clock,
            ticket_change=my_request.ticket_change)
        self.server.broadcast_message(data)
        # COMM.send_release(message, self.clock, self.datacenter_id, conn)

        # and then check whether we need to sell another ticket
        # if I have no request, quit
        if len(self.request_pool) == 0: return
        if self.request_queue[0].datacenter_id == self.datacenter_id and \
           self.request_queue[0].is_ready():
            self.sell_ticket(self.request_queue[0])
