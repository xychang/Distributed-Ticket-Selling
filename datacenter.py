# this script handles the logic of datacenter
import json
import bisect
import COMM

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

    def is_ready(self, datacenter_id):
        ''' check whether the request has all response received '''
        # if we already know it's ready, respond so
        if self.known_ready: return True
        # test whether all the response are gethered
        is_ready = set([reply.sender.datacenter_id for reply in self.collected_reply] \
            + [datacenter_id]).issuperset([center['id'] for center in CONFIG])
        if is_ready: self.known_ready = True
        return is_ready

    # implement clock comparison for messages
    def __lt__(self, other):
        return (self.clock, self.datacenter_id) < (other.score, other.datacenter_id)

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
    def __init__(self, datacenter_id):
        '''
        we use datacenter_id instead of pid because it is potentially
        deployed on multiple servers, and many have the same pid
        '''
        self.datacenter_id = datacenter_id
        self.datacenters = CONFIG['datacenters']
        self.total_ticket = CONFIG['total_ticket']
        self.clock = 1 # the clock is to be updated on (1) receive request, (2) send request
        # keep queue of all requests
        self.request_queue = []
        # keep a pool of my requests, so as to esially update the response status
        self.request_pool = {}


    def handle_coordinate_request(self, message):
        ''' from this, we expect a ticket_request message, with the datacenter_id and clock set'''
        # update the clock upon revcieing message
        self.clock = max(self.clock, message.clock) + 1
        # put the message into queue
        bisect.insort(self.request_queue, message)
        # and then reply the message, containing my clock, my datacenter_id
        # and the message I am replying to (identified by message clock)
        COMM.send_reply(self.datacenters[message.datacenter_id],
                        (self.clock, self.datacenter_id), message)

    def broadcast_message(self, message):
        ''' from this, we expect a freshly initialized ticket_request message'''
        for center_id in self.datacenters:
            if not center_id == self.datacenter_id:
                if isinstance(message, ticket_request):
                    # the count information is nenver used, but the example case send it
                    # so I am going to send it
                    COMM.send_request(self.datacenters[center_id],
                                      (self.clock, self.datacenter_id), message)
                else:
                    # broadcast release message
                    COMM.send_release(self.datacenters[center_id],
                                      (self.clock, self.datacenter_id), message)


    def handle_ticket_request(self, message):
        '''
        because the client is not in the logical time syste, we just send out
        ticket request immediately after receiving the client message
        '''
        # update the clock upon reveing message
        self.clock = self.clock + 1
        message.set_clock(self.clock)
        message.set_datacenter_id(self.datacenter_id)
        # the request is generated after all other requests in the queue
        # put it at the end
        self.request_queue.append(message)
        self.request_pool[message.clock] = message

        self.broadcast_message(message)

    def sell_ticket(self, my_request):
        ''' now we are going to sell our ticket, and then release the hold'''
        if my_request.ticket_count <= self.total_ticket:
            change = - my_request.ticket_count
        else:
            change = 0
        # if the change is zero, then the transaction failed
        self.total_ticket += change
        my_request.ticket_change = change
        COMM.reply_client(my_request.client, change != 0)
        # after handling the current request, remove it from the top of the queue
        self.request_queue.remove(my_request)
        del self.request_pool[my_request.clock]
        # send the release message to all peers
        self.broadcast_message(my_request)
        # and then check whether we need to sell another ticket
        if self.request_queue[0].datacenter_id == self.datacenter_id and \
           self.request_queue[0].is_ready():
            self.sell_ticket(self.request_queue[0])


    def handle_coordinate_reply(self, message):
        '''from this, we expect a coordinate_reply message '''
        # update the clock upon revcieing message
        self.clock = max(self.clock, message.clock) + 1
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
        if self.request_queue[0].datacenter_id == self.datacenter_id and \
           self.request_queue[0].is_ready():
            self.sell_ticket(self.request_queue[0])
