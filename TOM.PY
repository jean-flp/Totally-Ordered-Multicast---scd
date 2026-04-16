import threading
import time
import queue
import random

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
__TRANSACTIONS__ = 10

class transaction:
    def __init__(self, 
                    id:str,
                    transacoes:list
            ):
        #porem precisaria dispor a maneira de comparação (?) para por naquela etapa do relogio de lamport (?)
        #acho que podemos usar uma lista mesmo so chamar de fila/queue(tantofaz) como nome e fazer o sort depois nessa parte de comparacao
        self.id = id 
        self.clock = 0
        self.queue = [] # usar queue ou list mesmo ?
        self.acks = {}
        self.transacoes = transacoes
        self.lock = threading.Lock()
  
    # ELE POEM +! NO TIMESTAMP PQ NAO USA TEMPO ALGUM É UM CONTADOR QUE AUMENTA
    def multicast(self, message):
            with self.lock:
                self.clock += 1
                timestamp = self.clock
                print(f"P{self.id} deseja realizar a operação de: {message}")
            for p in self.transacoes:
                print(f"P{self.id} is casting to : P{p.id}, on time: {timestamp}")
                p.on_receive_msg(timestamp, self.id, message)
    def on_receive_msg(self, timestamp, sender_id, message):
        time.sleep(random.randint(a=1,b=3))
        with self.lock:
                self.clock = max(self.clock, timestamp) + 1
                msg_id = (timestamp, sender_id)

                # adiciona na fila e ordena
                print(f"P{self.id} received {message} from P{sender_id} | msg_ts={timestamp} | local_clock={self.clock}")
                self.queue.append((timestamp, sender_id, message))
                self.queue.sort()

                if msg_id not in self.acks:
                    self.acks[msg_id] = set()
                self.acks[msg_id].add(self.id)

        # send ACK to all
        for p in self.transacoes:
            p.on_receive_ack(msg_id, self.id)

    def on_receive_ack(self, msg_id, sender):
        with self.lock:
                if msg_id not in self.acks:
                    self.acks[msg_id] = set()

                self.acks[msg_id].add(sender)
        self.try_deliver()

    def try_deliver(self):
        with self.lock:
                while self.queue:
                    timestamp, sender_id, message = self.queue[0]
                    msg_id = (timestamp, sender_id)

                    if len(self.acks.get(msg_id, [])) == len(self.transacoes):
                        print(f"P{self.id} delivers: {message}")
                        self.queue.pop(0)
                    else:
                        break


# Create processes
processes = []
for i in range(__TRANSACTIONS__):
    processes.append(transaction(i, None))

# Assign process list
for p in processes:
    p.transacoes = processes

# Simulate concurrent sends
for i in range(len(processes)):
    threading.Thread(target=processes[i].multicast, args=(f"m_{i}",)).start()
