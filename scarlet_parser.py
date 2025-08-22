def parse_value(val):
    """Converte strings em tipos Python sempre que possível"""
    val = val.strip()
    try:
        return eval(val)  # tenta converter para int, str, etc.
    except Exception:
        return val  # devolve string se não der

def parse_dict(s):
    conds = {}
    for pair in s.split(","):
        if ":" not in pair:
            continue
        k, v = pair.split(":")
        conds[k.strip()] = parse_value(v.strip())
    return conds

def parse_list(s):
    return [col.strip() for col in s.split(",")]

def parse_values(s):
    return [parse_value(v) for v in s.split(",")]

# Definição da gramática
COMMANDS = {
    "wd":   {"args": ["string"]},             # wd->DBNAME
    "sd":   {"args": ["string"]},             # sd->DBNAME
    "wt":   {"args": ["string", "list"]},     # wt->TABLE->col1,col2
    "st":   {"args": ["string"]},             # st->TABLE
    "i":    {"args": ["values"]},             # i->1,'Alice',23
    "u":    {"args": ["dict", "dict"]},       # u->id:2->idade:26
    "d":  {"args": ["dict"]},               # d->id:2
    "dt":   {"args": ["string"]},             # dt->TABLE
    "dd":   {"args": ["string"]},             # dd->DB
    "show": {"args": []},                      # show
    "select": {"args": ["list", "dict?"]},     # select->col1,col2->condições (opcional)
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
            row_id = parts[1].split(":")[1]
            assignment = parts[2][len("set:"):]  # ex: age=25
            if "=" not in assignment:
                return cmd, []
            col, val = assignment.split("=", 1)
            return cmd, ["row_edit", row_id.strip(), col.strip(), parse_value(val.strip())]

        return cmd, []

    for i, typ in enumerate(expected):
        if typ == "string":
            args.append(parts[i+1].strip())
        elif typ == "list":
            args.append(parse_list(parts[i+1]))
        elif typ == "dict":
            args.append(parse_dict(parts[i+1]))
        elif typ == "values":
            args.append(parse_values(parts[i+1]))
        elif typ == "dict?":
            if len(parts) > i+1:
                args.append(parse_dict(parts[i+1]))
            else:
                args.append({})

    # mapear comando del → del_ (interno)
    if cmd == "d":
        return "del_", args

    return cmd, args
