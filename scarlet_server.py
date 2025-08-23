import socket
import json
from scarletdb import ScarletDB  # importa a classe

HOST = "0.0.0.0"
PORT = 65432

def _match_condition(value, cond):
    op = cond["op"]
    target = cond["val"]

    if op == "=":  return value == target
    if op == "!=": return value != target
    if op == "<":  return value < target
    if op == ">":  return value > target
    if op == "<=": return value <= target
    if op == ">=": return value >= target
    return False

def handle_command(db, command):
    try:
        cmd = command.get("cmd")
        args = command.get("args", [])

        if cmd == "select":
            if not db.current_db or not db.current_table:
                return {"status": "error", "msg": "Nenhuma base de dados ou tabela selecionada"}
            
            cols, conds = args
            table = db.databases[db.current_db][db.current_table]
            rows = []
            for row in table["rows"]:
                if all(_match_condition(row.get(k), conds[k]) for k in conds):
                    if cols == ["*"]:
                        rows.append(row)
                    else:
                        rows.append({c: row.get(c) for c in cols})
            return {"status": "ok", "msg": rows}

        if not hasattr(db, cmd):
            return {"status": "error", "msg": f"Comando desconhecido: {cmd}"}

        method = getattr(db, cmd)
        # Descompactar apenas se for lista ou tupla
        if isinstance(args, (list, tuple)):
            # Caso args seja [[...], [...], [...]] (ou seja, nested), descompacta 1 nível
            if len(args) == 1 and isinstance(args[0], (list, tuple)):
                args = args[0]
            result = method(*args)
        else:
            result = method(args)
        return {"status": "ok", "msg": str(result)}

    except Exception as e:
        return {"status": "error", "msg": str(e)}

def main():
    scarlet = ScarletDB()
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((HOST, PORT))
        s.listen()
        print(f"ScarletDB a correr em {HOST}:{PORT}...")
        while True:
            conn, addr = s.accept()
            print(f"\033[92m[+]\033[0m Cliente ligado: {addr}")
            with conn:
                while True:
                    data = conn.recv(4096)
                    if not data:
                        break
                    try:
                        command = json.loads(data.decode("utf-8"))
                        print(f"\033[93m[{addr}] comando recebido:\033[0m {command}")
                        response = handle_command(scarlet, command)
                    except Exception as e:
                        response = {"status": "error", "msg": str(e)}

                    conn.sendall(json.dumps(response).encode("utf-8"))
            print(f"\033[91m[-]\033[0m Cliente desligado: {addr}")

if __name__ == "__main__":
    main()
