import socket
import json
import re
from scarletdb import ScarletDB  # importa a classe

HOST = "0.0.0.0"
PORT = 65432

def _to_number_if_possible(s):
    txt = str(s).strip()
    if len(txt) >= 2 and ((txt[0] == txt[-1] == "'") or (txt[0] == txt[-1] == '"')):
        txt = txt[1:-1]
    try:
        return int(txt)
    except ValueError:
        try:
            return float(txt)
        except ValueError:
            return txt

def _eval_simple_condition(row, cond):
    # aceita: coluna (==|=|!=|<=|>=|<|>) valor
    m = re.match(r'\s*([A-Za-z_]\w*)\s*(==|=|!=|<=|>=|<|>)\s*(.+)\s*$', cond)
    if not m:
        return False
    col, op, raw = m.groups()
    if col not in row:
        return False
    val = row.get(col)
    target = _to_number_if_possible(raw)
    if val is None:
        return False
    if op == '==':
        op = '='
    try:
        if op == '=':  return val == target
        if op == '!=': return val != target
        if op == '<':  return val <  target
        if op == '>':  return val >  target
        if op == '<=': return val <= target
        if op == '>=': return val >= target
    except Exception:
        return False
    return False

def _eval_condition(row, cond_str):
    # prioridade: || por fora (OR), & por dentro (AND)
    if '||' in cond_str:
        return any(_eval_condition(row, part) for part in cond_str.split('||'))
    if '&' in cond_str:
        return all(_eval_condition(row, part) for part in cond_str.split('&'))
    return _eval_simple_condition(row, cond_str)

def _match_condition(value, cond):
    """
    Avalia uma condição do parser no formato dict: {"op": "<", "val": 10}
    - value: valor actual da row (int/float/str/...)
    - cond:  dict com keys "op" e "val"
    Retorna True/False sem lançar exceções.
    """
    if not isinstance(cond, dict):
        return False
    op = cond.get("op", "=")
    target = cond.get("val")

    # Se value está ausente, falha
    if value is None:
        return False

    def _coerce(v):
        # tenta preservar números; remove aspas de strings
        if isinstance(v, (int, float)):
            return v
        if isinstance(v, str):
            s = v.strip()
            # remover aspas se existirem
            if len(s) >= 2 and ((s[0] == s[-1] == "'") or (s[0] == s[-1] == '"')):
                s = s[1:-1]
            # tentar converter para int/float
            try:
                return int(s)
            except Exception:
                try:
                    return float(s)
                except Exception:
                    return s
        return v

    v1 = _coerce(value)
    v2 = _coerce(target)

    try:
        if op in ("=", "=="):
            return v1 == v2
        if op == "!=":
            return v1 != v2
        if op == "<":
            return v1 < v2
        if op == ">":
            return v1 > v2
        if op == "<=":
            return v1 <= v2
        if op == ">=":
            return v1 >= v2
    except Exception:
        # comparações com tipos incompatíveis (ex: 'a' < 3) devolvem False
        return False

    return False

def handle_command(db, command):
    try:
        cmd = command.get("cmd")
        args = command.get("args", [])

        if cmd == "select":
            if not db.current_db or not db.current_table:
                return {"status": "error", "msg": "Nenhuma base de dados ou tabela selecionada"}

            cols, conds = args  # conds pode ser dict (antigo) OU string (novo) OU {}
            table = db.databases[db.current_db][db.current_table]
            rows = []
            for row in table["rows"]:
                ok = True
                if isinstance(conds, dict) and conds:
                    ok = all(_match_condition(row.get(k), conds[k]) for k in conds)
                elif isinstance(conds, str) and conds.strip():
                    ok = _eval_condition(row, conds)
                if ok:
                    rows.append(row if cols == ["*"] else {c: row.get(c) for c in cols})
            return {"status": "ok", "msg": rows}

        if not hasattr(db, cmd):
            return {"status": "error", "msg": f"Comando desconhecido: {cmd}"}

        method = getattr(db, cmd)
        result = method(*args) if args else method()
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
