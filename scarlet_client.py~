import socket
import json
from scarlet_parser import parse_input

PORT = 65432

HELP_TEXT = """
# ---------------- BASES DE DADOS ----------------
wd->TestDB (create database)
sd->TestDB (select database)
dd->TestDB (delete database)

# ---------------- TABELAS ----------------
wt->Users->id:int,name:str,cv:file (create table)
wt->Products->id:int,name:str,manual:file,price:float
st->Users (select table)
st->Products
dt->Users (delete table)
dt->Products

# ---------------- INSERÇÃO ----------------
i->1,'Alice','/home/username/docs/alice_cv.pdf'
i->2,'Bernardo','/home/username/docs/bernardo_cv.pdf'
i->3,'Carla','/home/username/docs/carla_cv.pdf'
i->101,'Laptop','/home/username/docs/laptop_manual.pdf',1200.50
i->102,'Mouse','/home/username/docs/mouse_manual.pdf',25.99

# ---------------- ATUALIZAÇÃO (UPDATE) ----------------
# ---------------- Command 'u' is no longer maintained, use command 'e' -----------
u->id:2->name:'Bernardo Silva'
u->id:3->cv:'/home/username/docs/carla_new_cv.pdf'
u->id:101->price:1100
u->id:102->manual:'/home/username/docs/mouse_new_manual.pdf',price:30.50

# ---------------- DELETE ----------------
d->id=1

# ---------------- SELEÇÃO (SELECT) ----------------
select->*
select->id,name
select->name,cv
select->*->id:2
select->id,name->id:1
select->name,price->price>25.99
select->*->id>5&age=19
select->*->age>18||name="John"

# ---------------- EDIÇÃO (EDIT) ----------------
e->ac->email (add column)
e->ac->telefone
e->ac->data_nascimento
e->id:2->set:name='Bernardo S.' (edit row)
e->id:3->set:cv='/home/username/docs/carla_updated_cv.pdf'
e->id:101->set:price=1150
e->id:102->set:name='Gaming Mouse'

# ---------------- OUTROS ----------------
show
"""

def send_command(command, args=None, host=None):
    host = host or "127.0.0.1"  # default se não for passado
    msg = {"cmd": command, "args": args or []}
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((host, PORT))
        s.sendall(json.dumps(msg).encode("utf-8"))
        data = s.recv(4096)
        return json.loads(data.decode("utf-8"))

def main():
    default_host = "127.0.0.1"  # ou outro IP que queiras como default
    host_input = input("\033[93mConnect to (IP do servidor, vazio=default):\033[0m ").strip()
    HOST_TO_USE = host_input or default_host  # se vazio, usa default

    print(f"Ligado a {HOST_TO_USE}")
    print("Ligado à ScarletDB CLI (formato: cmd->args)")
    print("'-h' or 'help' for help")
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
            response = send_command(cmd, args, host=HOST_TO_USE)
            print(f"\033[93m[{response['status']}]\033[0m {response['msg']}")

        except KeyboardInterrupt:
            print("\nCliente interrompido.")
            break

if __name__ == "__main__":
    main()
