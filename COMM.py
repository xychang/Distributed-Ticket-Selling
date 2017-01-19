# to be implemented by zhijing


def send_request(center, clock, message):
    '''
    center is the same as a datacenter entry in config file
    clock is (clock_counter, datacenter_id)
    ticket_count is the number of tickets to buy
    this function is better implemented async
    '''
    ticket_count = message.ticket_count
    pass



def send_reply(center, clock, message):
    '''
    center is the same as a datacenter entry in config file
    clock is (clock_counter, datacenter_id)
    request_clock is the clock in the datacenter who sent the request
    '''
    request_clock = message.clock
    pass


def send_release(center, clock, message):
    '''
    center is the same as a datacenter entry in config file
    clock is (clock_counter, datacenter_id)
    request_clock is the clock in the datacenter who sent the request
    ticket_change is the change to total ticket
    '''
    request_clock = message.clock
    ticket_change = message.ticket_change
    pass


def reply_client(client, succeed):
    '''
    client it the nesessary information about the client so that
    we can send the reply message
    succeed is whether the transaction succeeded
    '''
    pass