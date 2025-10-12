from sqlalchemy import create_engine
import pandas as pd
from pypika import Table, Query

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

    def read(self, tabela: str) -> pd.DataFrame:
        self.__connect()
        df = pd.read_sql_table(tabela, self._connection, schema='public')
        return df

    def comparar_dados(self, tabela: str, pk_cols: list, new_df: pd.DataFrame):
        df_old = self.read(tabela)
      
        new_df = new_df[df_old.columns]

        old_keys = set([tuple(x) for x in df_old[pk_cols].values])
        add_df = new_df[~new_df.apply(lambda row: tuple(row[pk_cols]), axis=1).isin(old_keys)]

        merged = df_old.merge(new_df, on=pk_cols, how="inner", suffixes=("_old", "_new"))
        cols_to_check = [c for c in df_old.columns if c not in pk_cols]

        if cols_to_check:
            mask = (merged[[f"{c}_old" for c in cols_to_check]] != merged[[f"{c}_new" for c in cols_to_check]]).any(axis=1)
            to_update = merged[mask]
        else:
            to_update = pd.DataFrame(columns=df_old.columns)

        if not to_update.empty:
            update_df = new_df[new_df.apply(lambda row: tuple(row[pk_cols]), axis=1).isin(
                to_update.apply(lambda row: tuple(row[pk_cols]), axis=1)
            )]
        else:
            update_df = pd.DataFrame(columns=df_old.columns)

        return add_df, update_df

    def sincronizar(self, tabela: str, new_df: pd.DataFrame, pk_cols: list):
        self.__connect()
        add_df, update_df = self.comparar_dados(tabela, pk_cols, new_df)
        table = Table(tabela)

        with self._connection.begin() as conn:
            # --- Inserir ---
            for _, row in add_df.iterrows():
                query = Query.into(table).columns(*row.index).insert(*row.values)
                conn.execute(str(query))

            # --- Atualizar ---
            for _, row in update_df.iterrows():
                q = Query.update(table)
                for col in row.index:
                    if col not in pk_cols:
                        q = q.set(table[col], row[col])
                # Condições da chave primária
                for col in pk_cols:
                    q = q.where(table[col] == row[col])
                conn.execute(str(q))

        self.__disconnect()
        return add_df, update_df
