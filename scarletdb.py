import os
import json

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
        return os.path.join(DATA_DIR, f"{db_name}.json")

    def _save_db(self, db_name):
        path = self._db_path(db_name)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(self.databases[db_name], f, indent=2, ensure_ascii=False)

    def _delete_db_file(self, db_name):
        path = self._db_path(db_name)
        if os.path.exists(path):
            os.remove(path)

    def _load_databases(self):
        for file in os.listdir(DATA_DIR):
            if file.endswith(".json"):
                db_name = file[:-5]
                with open(self._db_path(db_name), "r", encoding="utf-8") as f:
                    self.databases[db_name] = json.load(f)

    # ---- Comandos ----
    def wd(self, db_name):
        if db_name in self.databases:
            return f"Base de dados '{db_name}' já existe."
        self.databases[db_name] = {}
        self._save_db(db_name)
        return f"Base de dados '{db_name}' criada."

    def wt(self, table_name, columns):
        if self.current_db is None:
            return "Nenhuma base de dados selecionada."
        if table_name in self.databases[self.current_db]:
            return f"Tabela '{table_name}' já existe em '{self.current_db}'."
        self.databases[self.current_db][table_name] = {"columns": columns, "rows": []}
        self._save_db(self.current_db)
        return f"Tabela '{table_name}' criada em '{self.current_db}'."

    def i(self, values):
        if self.current_table is None:
            return "Nenhuma tabela selecionada."
        table = self.databases[self.current_db][self.current_table]
        if len(values) != len(table["columns"]):
            return "Número incorreto de valores."
        row = dict(zip(table["columns"], values))
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
        """Apagar linhas da tabela atual que correspondam à condição"""
        if self.current_table is None:
            return "Nenhuma tabela selecionada."
        table = self.databases[self.current_db][self.current_table]
        before = len(table["rows"])
        table["rows"] = [row for row in table["rows"] if not all(row.get(k) == v for k, v in condition.items())]
        deleted = before - len(table["rows"])
        if deleted > 0:
            self._save_db(self.current_db)
        return f"{deleted} linha(s) apagada(s)."

    def dt(self, table_name):
        """Apagar (drop) tabela"""
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
        """Apagar (drop) base de dados"""
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
            col_name = args[1]
            if col_name in table["columns"]:
                return f"Coluna '{col_name}' já existe."
            table["columns"].append(col_name)
            for row in table["rows"]:
                row[col_name] = None
            self._save_db(self.current_db)
            return f"Coluna '{col_name}' adicionada com sucesso."

        # Editar valor → e->id:2->set:age=25
        if args[0] == "row_edit":
            row_id, col, val = args[1], args[2], args[3]
            for row in table["rows"]:
                if str(row.get("id")) == str(row_id):
                    row[col] = val
                    self._save_db(self.current_db)
                    return f"Linha com id={row_id} atualizada ({col}={val})."

            return f"Nenhuma linha encontrada com id={row_id}."

        return "Comando inválido para 'e'."
