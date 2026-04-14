import threading
import time
import queue

#Rever talvez n precise
# class event:
#  def __init__(self, id, operation):
#   self.id = id
#   self.operation = operation
#   self.acknowledged_list = []
#   def get_operation(self):
#    return self.operation
#   def set_operation(self, operation):
#    self.operation = operation

# Algorithm steps
# Sending an update message:

# When a process wants to send an update message:

# Timestamp the message with the sender’s logical time.

# Multicast the message to all processes, including itself.

# Receiving a message:

# Upon receiving a message:

# Place the message into the local_queue.

# Sort the local_queue based on message timestamps.

# Delivering messages:

# Periodically or upon receiving a new message:

# Check the local_queue for the message with the lowest timestamp.

# Deliver this message to the application layer.

# Increment the acknowledgment_counter.

# Multicast acknowledgment to all nodes.

# Handling acknowledgments:

# When receiving an acknowledgment:

# Increment the acknowledgment counter.

# If the acknowledgment counter reaches the total number of nodes, remove the delivered message from the local_queue.

#GLOBAL VARIABLES
__TRANSACTIONS__ = 2

class transaction:
 """
    Class referida a abtração principal da Réplica -> slide glauco

 """
 def __init__(self, 
                id:str,
                transacoes:list
        ):
  #porem precisaria dispor a maneira de comparação (?) para por naquela etapa do relogio de lamport (?)
  #acho que podemos usar uma lista mesmo so chamar de fila/queue(tantofaz) como nome e fazer o sort depois nessa parte de comparacao
  self.id = id 
  self.clock = 0
  self.queue = [] # usar queue ou list mesmo ?
  self.acknowledged_counter = {}
  self.transacoes = transacoes
  
# ELE POEM +! NO TIMESTAMP PQ NAO USA TEMPO ALGUM É UM CONTADOR QUE AUMENTA
 def multicast(self, message):
        self.clock += 1
        timestamp = self.clock
        for p in self.processes:
            p.on_receive_msg(timestamp, self.pid, message)

def on_receive_msg(self, timestamp, sender_id, message):
    self.clock = max(self.clock, timestamp) + 1
    msg_id = (timestamp, sender_id)

    self.queue.append((timestamp, sender_id, message))
    self.queue.sort()

    self.acks[msg_id] = set([self.pid])

    # send ACK to all
    for p in self.processes:
        p.on_receive_ack(msg_id, self.pid)

def on_receive_ack(self, msg_id, sender):
    if msg_id not in self.acks:
        self.acks[msg_id] = set()
    self.acks[msg_id].add(sender)
    self.try_deliver()

def try_deliver(self):
    while self.queue:
        timestamp, sender_id, message = self.queue[0]
        msg_id = (timestamp, sender_id)

        if len(self.acks[msg_id]) == len(self.processes):
            print(f"P{self.pid} delivers: {message}")
            self.queue.pop(0)
        else:
            break


 