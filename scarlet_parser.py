import re

def parse_value(val):
    """Converte strings em tipos Python sempre que possível"""
    val = val.strip()
    
    # substituir vírgula por ponto para floats
    val_num = val.replace(",", ".")
    
    try:
        # tentar converter para int ou float
        if '.' in val_num:
            return float(val_num)
        else:
            return int(val_num)
    except ValueError:
        pass
    
    # remover aspas de strings
    if (val.startswith('"') and val.endswith('"')) or (val.startswith("'") and val.endswith("'")):
        return val[1:-1]
    
    return val

def parse_dict(s):
    conds = {}
    for pair in s.split(","):
        if ":" in pair:
            k, v = pair.split(":", 1)
        elif "=" in pair:
            k, v = pair.split("=", 1)
        else:
            continue

        k = k.strip()
        v = v.strip()

        # Verificar operador
        match = re.match(r"(<=|>=|!=|<|>|=)?\s*(.*)", v)
        if match:
            op = match.group(1) or "="  # default é igualdade
            val = parse_value(match.group(2).strip())
            conds[k] = {"op": op, "val": val}
    return conds

def parse_list(s):
    return [col.strip() for col in s.split(",")]

def parse_values(s):
    return [parse_value(v) for v in s.split(",")]

def parse_columns_with_type(s):
    """Converte 'id:int,name:string,file:file' em (colunas, tipos)"""
    cols = []
    types = []
    for item in s.split(","):
        if ":" in item:
            col, typ = item.split(":", 1)
            cols.append(col.strip())
            types.append(typ.strip())
        else:
            cols.append(item.strip())
            types.append("string")  # default
    return cols, types

# Definição da gramática
COMMANDS = {
    "wd":   {"args": ["string"]},             # wd->DBNAME
    "sd":   {"args": ["string"]},             # sd->DBNAME
    "wt":   {"args": ["string", "list"]},     # wt->TABLE->col1:type1,col2:type2
    "st":   {"args": ["string"]},             # st->TABLE
    "i":    {"args": ["values"]},             # i->1,'Alice',23
    "u":    {"args": ["dict", "dict"]},       # u->id:2->idade:26
    "d":    {"args": ["dict"]},               # d->id:2
    "dt":   {"args": ["string"]},             # dt->TABLE
    "dd":   {"args": ["string"]},             # dd->DB
    "show": {"args": []},                     # show
    "select": {"args": ["list", "dict?"]},    # select->col1,col2->condições (opcional)
    "e": {"args": ["custom"]}
}

def parse_input(user_input: str):
    parts = user_input.split("->")
    cmd = parts[0].strip()
    spec = COMMANDS.get(cmd)

    if not spec:
        return cmd, []

    args = []
    expected = spec["args"]

    if cmd == "e":
        if len(parts) < 2:
            return cmd, []

        # adicionar coluna
        if parts[1] == "ac":
            if len(parts) < 3:
                return cmd, []
            return cmd, ["ac", parts[2].strip()]

        # editar valor de row
        if parts[1].startswith("id:") and len(parts) >= 3 and parts[2].startswith("set:"):
            try:
                row_id = int(parts[1].split(":")[1])
            except ValueError:
                return cmd, []

            assignment_str = parts[2][len("set:"):]  # ex: "nome='Bernardo',idade=25,preco=24,56"
    
            assignments = {}
            # separar por vírgula, respeitando valores com vírgulas dentro de aspas
            for pair in re.split(r',(?=(?:[^"\']|"[^"]*"|\'[^\']*\')*$)', assignment_str):
                if "=" not in pair:
                    continue
                col, val = pair.split("=", 1)
                assignments[col.strip()] = parse_value(val.strip())
    
            return cmd, ["row_edit", row_id, assignments]

        return cmd, []

    for i, typ in enumerate(expected):
        if cmd == "wt":
            # colunas com tipo
            if len(parts) < 3:
                return cmd, []
            table_name = parts[1].strip()
            cols, types = parse_columns_with_type(parts[2])
            return cmd, [table_name, cols, types]
        elif typ == "string":
            args.append(parts[i+1].strip())
        elif typ == "list":
            args.append(parse_list(parts[i+1]))
        elif typ == "dict":
            args.append(parse_dict(parts[i+1]))
        elif typ == "values":
            args.extend(parse_values(parts[i+1]))
        elif typ == "dict?":
            if len(parts) > i+1:
                args.append(parse_dict(parts[i+1]))
            else:
                args.append({})

    # --- SELECT: suporta modo antigo (dict) e novo (string com & / ||) ---
    if cmd == "select":
        # cols: obrigatório (ex: "*", "id,nome")
        cols = parse_list(parts[1]) if len(parts) > 1 and parts[1].strip() else ["*"]

        # terceiro argumento pode ser:
        # - vazio  → sem condições
        # - "id:2,idade:>18" (modo antigo: dict)
        # - "idade>18&nome='Alice' || preco>=10" (modo novo: string)
        if len(parts) > 2 and parts[2].strip():
            cond_token = parts[2].strip()
            if ":" in cond_token:
                return cmd, [cols, parse_dict(cond_token)]   # modo antigo (dict)
            else:
                return cmd, [cols, cond_token]               # modo novo (string)
        else:
            return cmd, [cols, {}]                           # sem condições

    # mapear comando del → del_ (interno)
    if cmd == "d":
        return "d", args

    return cmd, args
