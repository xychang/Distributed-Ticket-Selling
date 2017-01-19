# this script handles the logic of datacenter
import json

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

    # set which datacenter is handling this request
    def set_datacenter_id(self, datacenter_id):
        self.datacenter_id = datacenter_id

    # set the time the request is broadcasted
    def set_clock(self, clock):
        self.clock = clock

    def record_response(self, sender):
        self.collected_reply.append(sender.datacenter_id)

    def is_ready(self, datacenter_id):
        # test whether all the response are gethered
        return set([reply.sender.datacenter_id for reply in self.collected_reply] \
            + [datacenter_id]).issuperset([center['id'] for center in CONFIG])

    # implement clock comparison for messages
    def __lt__(self, other):
        return (self.clock, self.datacenter_id) < (other.score, other.datacenter_id)

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

    # def handleCoordinateRequest():

    def broadcast_request(self, message):
        for center in self.datacenters:
            if not center['id'] == self.datacenter_id:
                # the count information is nenver used, but the example case send it
                # so I am going to send it
                COMM.send_request(center, (self.clock, self.datacenter_id), message.ticket_count)

    # because the client is not in the logical time syste, we just send out
    # ticket request immediately after receiving the client message
    def handleTicketRequest(self, message):
        # update the clock upon reveing message
        self.clock = self.clock + 1
        message.set_clock(self.clock)
        message.set_datacenter_id(self.datacenter_id)
        self.broadcast_request(message)
        self.request_queue.append(message)

