from sqlalchemy import create_engine, text
import pandas as pd
from pypika import Table, Case, Query
import pandas as pd

class PostgresSQL:
    def __init__(self, db_connection):
        self._engine = create_engine(db_connection)
        self._connection = None

    def __connect(self):
        if self._connection is None:
            self._connection = self._engine.connect()

    def __disconnect(self):
        if self._connection is not None:
            self._connection.close()
            self._connection = None

    def read(self, tabela: str,) -> pd.DataFrame:
        df = None
        try:
            self.__connect()
            df = pd.read_sql_table(tabela, self._connection, schema='public')
        
        finally:
            self.__disconnect()
        return df

    def query(self, query: str):
        df = None
        try:
            self.__connect()
            df = pd.read_sql_query(text(query), self._connection)
        finally:
            self.__disconnect()
        return df

    def comparar_dados(self, tabela: str, pk_cols: list, new_df: pd.DataFrame):
        df_old = self.read(tabela)
      
        new_df = new_df[df_old.columns]

        for col in pk_cols:
            if col in df_old.columns and col in new_df.columns:
                # Se for coluna de data, converte ambas para datetime
                if "data" in col.lower():
                    df_old[col] = pd.to_datetime(df_old[col], errors="coerce", dayfirst=False)
                    new_df[col] = pd.to_datetime(new_df[col], errors="coerce", dayfirst=False)
                else:
                    # Força o tipo object para comparar com segurança
                    df_old[col] = df_old[col].astype(str)
                    new_df[col] = new_df[col].astype(str)

        old_keys = set(map(tuple,df_old[pk_cols].values))

        new_df_keys = new_df[pk_cols].apply(tuple, axis=1)
        add_df = new_df[~new_df_keys.isin(old_keys)]

        merged = df_old.merge(new_df, on=pk_cols, how="inner", suffixes=("_old", "_new"))
        cols_to_check = [c for c in df_old.columns if c not in pk_cols]

        if cols_to_check:
            mask = (merged[[f"{c}_old" for c in cols_to_check]].values != merged[[f"{c}_new" for c in cols_to_check]].values).any(axis=1)
            to_update = merged.loc[mask, pk_cols]
        else:
            to_update = pd.DataFrame(columns=pk_cols)

        if not to_update.empty:
            update_keys = set(map(tuple, to_update.values))
            update_df = new_df[new_df[pk_cols].apply(tuple, axis=1).isin(update_keys)]
        else:
            update_df = pd.DataFrame(columns=df_old.columns)

        return add_df, update_df

    def sincronizar(self, tabela: str, new_df: pd.DataFrame, pk_cols: list):
        self.__connect()
        add_df, update_df = self.comparar_dados(tabela, pk_cols, new_df)
        table = Table(tabela)
        # --- Inserir ---
        for _, row in add_df.iterrows():
            query = Query.into(table).columns(*row.index).insert(*row.values)
            sql_statement = str(query)
            self._connection.execute(text(sql_statement))

        # --- Atualizar ---
        for _, row in update_df.iterrows():
            q = Query.update(table)
            for col in row.index:
                if col not in pk_cols:
                    q = q.set(table[col], row[col])
            # Condições da chave primária
            for col in pk_cols:
                q = q.where(table[col] == row[col])

            sql_statement = str(q)
            self._connection.execute(text(sql_statement))

        self._connection.commit()

        self.__disconnect()
       
