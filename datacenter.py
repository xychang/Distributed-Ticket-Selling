# this script handles the logic of datacenter
import json
import bisect
import COMM

CONFIG = json.load(open('config.json'))


# the ticket request is the request from the client, and 
# the client is not in the logical clock system
# therefore, when we first construct the request, it does not have a time
# but when the datacenter who handles the request broad cast this request,
# it now have a time
class ticket_request():
    def __init__(self, client, ticket_count):
        self.client = client
        self.ticket_count = ticket_count
        self.collected_reply = []
        self.clock = self.datacenter_id = None
        self.known_ready = False

    # set which datacenter is handling this request
    def set_datacenter_id(self, datacenter_id):
        self.datacenter_id = datacenter_id

    # set the time the request is broadcasted
    def set_clock(self, clock):
        self.clock = clock

    def record_response(self, sender):
        self.collected_reply.append(sender.datacenter_id)

    def is_ready(self, datacenter_id):
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

class coordinate_reply():
    def __init__(self, clock, datacenter_id, target_request_clock):
        self.clock = clock
        self.datacenter_id = datacenter_id
        self.target_request_clock = target_request_clock

class datacenter():
    # we should datacenter_id instead of pid because it is potentially
    # deployed on multiple servers, and many have the same pid
    def __init__(self, datacenter_id):
        self.datacenter_id = datacenter_id
        self.datacenters = CONFIG['datacenters']
        self.total_ticket = CONFIG['total_ticket']
        self.clock = 1 # the clock is to be updated on (1) receive request, (2) send request
        # keep queue of all requests
        self.request_queue = []
        # keep a pool of my requests, so as to esially update the response status
        self.request_pool = {}

    # from this, we expect a ticket_request message, with the datacenter_id and clock set
    def handle_coordinate_request(self, message):
        # update the clock upon revcieing message
        self.clock = max(self.clock, message.clock) + 1
        # put the message into queue
        bisect.insort(self.request_queue, message)
        # and then reply the message, containing my clock, my datacenter_id 
        # and the message I am replying to (identified by message clock)
        COMM.send_reply(self.datacenters[message.datacenter_id], \
            (self.clock, self.datacenter_id), message.clock)

    # from this, we expect a freshly initialized ticket_request message
    def broadcast_message(self, message):
        for center_id in self.datacenters:
            if not center_id == self.datacenter_id:
                if type(message) is ticket_request:
                    # the count information is nenver used, but the example case send it
                    # so I am going to send it
                    COMM.send_request(datacenters[center_id], (self.clock, self.datacenter_id), message.ticket_count)
                else:
                    # broadcast release message
                    COMM.send_message()

    # because the client is not in the logical time syste, we just send out
    # ticket request immediately after receiving the client message
    def handle_ticket_request(self, message):
        # update the clock upon reveing message
        self.clock = self.clock + 1
        message.set_clock(self.clock)
        message.set_datacenter_id(self.datacenter_id)
        # the request is generated after all other requests in the queue
        # put it at the end
        self.request_queue.append(message)
        self.request_pool[message.clock] = message

        self.broadcast_message(message)

    # from this, we expect a coordinate_reply message 
    def handle_coordinate_reply(self, message):
        # update the clock upon revcieing message
        self.clock = max(self.clock, message.clock) + 1
        # update the stored request
        self.request_pool[message.target_request_clock].record_response(message)
        # check if the request is
