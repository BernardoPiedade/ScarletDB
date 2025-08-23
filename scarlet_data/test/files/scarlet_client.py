import socket
import json
from scarlet_parser import parse_input

HOST = "127.0.0.1"
PORT = 65432

HELP_TEXT = """
# ---------------- BASES DE DADOS ----------------
wd->TestDB
sd->TestDB
dd->TestDB
show

# ---------------- TABELAS ----------------
wt->Users->id,name,age
wt->Products->id,name,price
st->Users
st->Products
dt->Users
dt->Products

# ---------------- INSERÇÃO ----------------
i->1,'Alice',23
i->2,'Bernardo',25
i->3,'Carla',25
i->101,'Laptop',1200.50
i->102,'Mouse',25.99

# ---------------- ATUALIZAÇÃO (UPDATE) ----------------
u->id:2->age:26
u->id:3->name:'Carla Silva'
u->id:101->price:1100
u->id:102->name:'Wireless Mouse',price:30.50

# ---------------- DELETE ----------------
d->id:1
d->id:2
d->id:101

# ---------------- SELEÇÃO (SELECT) ----------------
select->*
select->id,name
select->name,age->age:25
select->*->id:2
select->id,name->id:1
select->name,age->age:25,id:3

# ---------------- EDIÇÃO (EDIT) ----------------
e->ac->email
e->ac->telefone
e->ac->data_nascimento
e->id:2->set:age=27
e->id:3->set:name='Carla M.'
e->id:101->set:price=1150
e->id:102->set:name='Gaming Mouse'

# ---------------- OUTROS ----------------
show
"""

def send_command(command, args=None):
    msg = {"cmd": command, "args": args or []}
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((HOST, PORT))
        s.sendall(json.dumps(msg).encode("utf-8"))
        data = s.recv(4096)
        return json.loads(data.decode("utf-8"))

def main():
    print("Ligado à ScarletDB CLI (formato: cmd->args)")
    print("Exemplo: wd->TestDB | wt->Users->id,nome | i->1,'Alice',23 | d->id:2 | -h for help")
    while True:
        try:
            user_input = input("\033[93mScarletDB>\033[0m ").strip()

            if user_input in ("-h", "help"):
                print(HELP_TEXT)
                continue

            if user_input.lower() in ("exit", "quit"):
                print("A desligar cliente...")
                break

            cmd, args = parse_input(user_input)
            response = send_command(cmd, args)
            print(f"\033[93m[{response['status']}]\033[0m {response['msg']}")

        except KeyboardInterrupt:
            print("\nCliente interrompido.")
            break

if __name__ == "__main__":
    main()
