import os
import json
import shutil

DATA_DIR = "scarlet_data"  # pasta onde guardamos as bases de dados

class ScarletDB:
    def __init__(self):
        self.databases = {}
        self.current_db = None
        self.current_table = None
        os.makedirs(DATA_DIR, exist_ok=True)
        self._load_databases()

    # ---- Funcoes de persistencia ----
    def _db_path(self, db_name):
        return os.path.join(DATA_DIR, db_name, f"{db_name}.json")

    def _files_path(self, db_name):
        path = os.path.join(DATA_DIR, db_name, "files")
        os.makedirs(path, exist_ok=True)
        return path

    def _save_db(self, db_name):
        path = self._db_path(db_name)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(self.databases[db_name], f, indent=2, ensure_ascii=False)

    def _delete_db_file(self, db_name):
        """Apaga toda a pasta da base de dados"""
        path = os.path.join(DATA_DIR, db_name)
        if os.path.exists(path):
            def remover_erro(func, path, exc_info):
                os.chmod(path, 0o777)
                func(path)
            shutil.rmtree(path, onerror=remover_erro)

    def _load_databases(self):
        for db_folder in os.listdir(DATA_DIR):
            db_path = os.path.join(DATA_DIR, db_folder)
            if os.path.isdir(db_path):
                json_file = os.path.join(db_path, f"{db_folder}.json")
                if os.path.isfile(json_file):
                    with open(json_file, "r", encoding="utf-8") as f:
                        self.databases[db_folder] = json.load(f)

    # ---- Helpers ----
    def _handle_file_value(self, val):
        if isinstance(val, str):
            try:
                val_path = os.path.abspath(val)
                if os.path.isfile(val_path):
                    # Criar pasta files dentro da pasta da base de dados
                    files_dir = os.path.join(DATA_DIR, self.current_db, "files")
                    os.makedirs(files_dir, exist_ok=True)

                    # Determinar destino e copiar ficheiro
                    filename = os.path.basename(val_path)
                    dest_path = os.path.join(files_dir, filename)
                    shutil.copy(val_path, dest_path)

                    # Devolver caminho relativo dentro da pasta da base de dados
                    return os.path.join("files", filename)
                else:
                    print(f"Atenção: ficheiro '{val}' não existe.")
            except PermissionError:
                print(f"Atenção: não foi possível copiar '{val}' por falta de permissões.")
                return val
        return val

    # ---- Comandos ----
    def wd(self, db_name):
        if db_name in self.databases:
            return f"Base de dados '{db_name}' já existe."
    
        # Criar pasta da base de dados
        db_dir = os.path.join(DATA_DIR, db_name)
        os.makedirs(db_dir, exist_ok=True)

        # Criar subpasta 'files'
        os.makedirs(os.path.join(db_dir, "files"), exist_ok=True)
    
    	# Inicializar base de dados vazia
        self.databases[db_name] = {}
    
        # Guardar ficheiro JSON dentro da pasta da base de dados
        path = os.path.join(db_dir, f"{db_name}.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump(self.databases[db_name], f, indent=2, ensure_ascii=False)
    
        return f"Base de dados '{db_name}' criada com pasta e ficheiro JSON."

    def wt(self, table_name, columns, types):
        if self.current_db is None:
            return "Nenhuma base de dados selecionada."
        if table_name in self.databases[self.current_db]:
            return f"Tabela '{table_name}' já existe em '{self.current_db}'."
        self.databases[self.current_db][table_name] = {
            "columns": columns,
            "types": types,
            "rows": []
        }
        self._save_db(self.current_db)
        return f"Tabela '{table_name}' criada em '{self.current_db}' com tipos de coluna."

    def i(self, *values):
        if self.current_table is None:
            return "Nenhuma tabela selecionada."
        table = self.databases[self.current_db][self.current_table]
        if len(values) != len(table["columns"]):
            return "Número incorreto de valores."
    
        row = {}
        for col, typ, val in zip(table["columns"], table["types"], values):
            if typ == "file":
                row[col] = self._handle_file_value(val)
            elif typ == "int":
                row[col] = int(val)
            elif typ == "float":
                # permite vírgula ou ponto
                row[col] = float(str(val).replace(",", "."))
            else:
                row[col] = str(val)
    
        table["rows"].append(row)
        self._save_db(self.current_db)
        return f"Valores inseridos em '{self.current_table}'."

    def u(self, condition, updates):
        if self.current_table is None:
            return "Nenhuma tabela selecionada."
        table = self.databases[self.current_db][self.current_table]
        count = 0
        for row in table["rows"]:
            if all(row.get(k) == v for k, v in condition.items()):
                row.update(updates)
                count += 1
        if count > 0:
            self._save_db(self.current_db)
        return f"{count} linha(s) atualizada(s)."

    def d(self, condition):

        if self.current_table is None:
            return "Nenhuma tabela selecionada."
        table = self.databases[self.current_db][self.current_table]

        # se vier como string tipo "id=2"
        if isinstance(condition, str):
            import re
            m = re.match(r"(\w+)\s*(=|!=|<|>|<=|>=)\s*(.+)", condition)
            if not m:
                return "Condição inválida."
            k, op, val = m.groups()
            # tenta converter o valor para int se for número
            try:
                val = int(val)
            except ValueError:
                val = val.strip("'\"")
            condition = {k: {"op": op, "val": val}}

        def match(row, condition):
            for k, cond in condition.items():
                v = row.get(k)
                op = cond["op"]
                val = cond["val"]

                # tenta uniformizar: converte para string se um deles for string
                if isinstance(v, str) or isinstance(val, str):
                    v = str(v)
                    val = str(val)

                if op == "=" and v != val:
                    return False
                elif op == "!=" and v == val:
                    return False
                elif op == ">" and not (v > val):
                    return False
                elif op == "<" and not (v < val):
                    return False
                elif op == ">=" and not (v >= val):
                    return False
                elif op == "<=" and not (v <= val):
                    return False
            return True

        before = len(table["rows"])

        print("DEBUG rows:", table["rows"])
        print("DEBUG condition:", condition)

        table["rows"] = [row for row in table["rows"] if not match(row, condition)]
        deleted = before - len(table["rows"])
        if deleted > 0:
            self._save_db(self.current_db)
        return f"{deleted} linha(s) apagada(s)."

    def dt(self, table_name):
        if self.current_db is None:
            return "Nenhuma base de dados selecionada."
        if table_name not in self.databases[self.current_db]:
            return f"Tabela '{table_name}' não existe em '{self.current_db}'."
        del self.databases[self.current_db][table_name]
        self._save_db(self.current_db)
        if self.current_table == table_name:
            self.current_table = None
        return f"Tabela '{table_name}' eliminada de '{self.current_db}'."

    def dd(self, db_name):
        if db_name not in self.databases:
            return f"Base de dados '{db_name}' não existe."
        del self.databases[db_name]
        self._delete_db_file(db_name)
        if self.current_db == db_name:
            self.current_db = None
            self.current_table = None
        return f"Base de dados '{db_name}' eliminada."

    def sd(self, db_name):
        if db_name not in self.databases:
            return f"Base de dados '{db_name}' não existe."
        self.current_db = db_name
        return f"Base de dados atual: '{db_name}'."

    def st(self, table_name):
        if self.current_db is None:
            return "Nenhuma base de dados selecionada."
        if table_name not in self.databases[self.current_db]:
            return f"Tabela '{table_name}' não existe em '{self.current_db}'."
        self.current_table = table_name
        return f"Tabela atual: '{table_name}'."

    def show(self):
        if self.current_table is None:
            return "Nenhuma tabela selecionada."
        table = self.databases[self.current_db][self.current_table]
        output = {"columns": table["columns"], "rows": table["rows"]}
        return json.dumps(output, indent=2, ensure_ascii=False)

    def e(self, *args):
        if not self.current_db or not self.current_table:
            return "Nenhuma base de dados/tabela selecionada."

        table = self.databases[self.current_db][self.current_table]

        # Adicionar coluna → e->ac->ColumnName
        if args[0] == "ac":
            col_type = "string"  # default
            col_name = args[1]
            if ":" in col_name:
                col_name, col_type = col_name.split(":", 1)
                col_name, col_type = col_name.strip(), col_type.strip()

            if col_name in table["columns"]:
                return f"Coluna '{col_name}' já existe."

            table["columns"].append(col_name)
            table["types"].append(col_type)
            for row in table["rows"]:
                row[col_name] = None
            self._save_db(self.current_db)
            return f"Coluna '{col_name}' ({col_type}) adicionada com sucesso."

        # Editar valor → e->id:2->set:age=25
        if args[0] == "row_edit":
            row_id = args[1]
            assignments = args[2]  # agora é dict {col: val, ...}

            for row in table["rows"]:
                if str(row.get("id")) == str(row_id):
                    for col, val in assignments.items():
                        if col not in table["columns"]:
                            continue  # ignora colunas inexistentes

                        col_type = table["types"][table["columns"].index(col)]
                        if col_type == "file":
                            row[col] = self._handle_file_value(val)
                        elif col_type == "int":
                            row[col] = int(val)
                        elif col_type == "float":
                            row[col] = float(val)
                        else:
                            row[col] = str(val)

                    self._save_db(self.current_db)
                    updated_cols = ", ".join(f"{c}={v}" for c, v in assignments.items())
                    return f"Linha com id={row_id} atualizada ({updated_cols})."

            return f"Nenhuma linha encontrada com id={row_id}."

        return "Comando inválido para 'e'."
