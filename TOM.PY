import threading
import time

class event:
 def __init__(self, id, operation):
  self.id = id
  self.operation = operation
  self.acknowledged_list = []
  def get_operation(self):
   return self.operation
  def set_operation(self, operation):
   self.operation = operation

class transaction:
 def __init__(self, id,):
  self.id = id
  self.acknowledged = {}
 def acknowledge(self, node, ):
  self.acknowledged[node] = []


class ProcessoLamport:
    def __init__(self, id):
        self.id = id
        self.relogio = 0
        self.lock = threading.Lock()

    def evento_local(self):
        with self.lock:
            self.relogio += 1
            print(f"Processo {self.id} - Evento local. Relógio: {self.relogio}")

    def enviar_mensagem(self):
        with self.lock:
            self.relogio += 1
            print(f"Processo {self.id} - Enviando msg. Relógio: {self.relogio}")
            return self.relogio

    def receber_mensagem(self, relogio_recebido):
        with self.lock:
            self.relogio = max(self.relogio, relogio_recebido) + 1
            print(f"Processo {self.id} - Msg recebida. Relógio atualizado: {self.relogio}")

# Exemplo de uso
p1 = ProcessoLamport(1)
p2 = ProcessoLamport(2)

p1.evento_local()
relogio_msg = p1.enviar_mensagem()
p2.receber_mensagem(relogio_msg)
p2.evento_local()
 