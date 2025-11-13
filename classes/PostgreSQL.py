from sqlalchemy import create_engine, text
import pandas as pd
from pypika import Table, Query
import pandas as pd

class PostgresSQL:
    def __init__(self, db_connection):
        self._engine = create_engine(db_connection)
        self._connection = None

    def __connect(self):
        if self._connection is None:
            self._connection = self._engine.connect()

            try:
                self._connection.rollback()
            except Exception:
                ...

    def __disconnect(self):
        if self._connection is not None:
            self._connection.close()
            self._connection = None

    def read(self, tabela: str) -> pd.DataFrame:
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
        new_df = new_df[df_old.columns].copy()

        for col in pk_cols:
            if pd.api.types.is_datetime64_any_dtype(df_old[col]):
                df_old[col] = pd.to_datetime(df_old[col]).dt.strftime('%Y-%m-%d')
            else:
                df_old[col] = df_old[col].astype(str)
            
            new_df[col] = new_df[col].astype(str)

        df_old["_key"] = df_old[pk_cols].agg("|".join, axis=1)
        new_df["_key"] = new_df[pk_cols].agg("|".join, axis=1)

        old_keys = set(df_old["_key"])
        new_keys = set(new_df["_key"])

        # 1️⃣ Linhas novas (presentes em new_df mas não em df_old)
        insert = new_df[new_df["_key"].isin(new_keys - old_keys)].copy()

        # 2️⃣ Linhas alteradas (mesmas chaves, mas valores diferentes)
        common_keys = old_keys & new_keys
        old_common = df_old[df_old["_key"].isin(common_keys)].set_index("_key")
        new_common = new_df[new_df["_key"].isin(common_keys)].set_index("_key")

        if not old_common.empty and not new_common.empty:
            old_common = old_common.sort_index()
            new_common = new_common.sort_index()

        non_pk_cols = [col for col in df_old.columns if col not in pk_cols + ["_key"]]
        
        
        diff_mask = (old_common[non_pk_cols] != new_common[non_pk_cols]).any(axis=1)
        update = new_common[diff_mask].reset_index()

       
        for df in [insert, update]:
            df.drop(columns="_key", inplace=True, errors="ignore")

        return insert, update


    def __insert(self,tabela: Table, df: pd.DataFrame):
        self.__connect()
        for _, row in df.iterrows():
            query = Query.into(tabela).columns(*row.index).insert(*row.values)
            sql_statement = str(query)
            self._connection.execute(text(sql_statement))

        self._connection.commit()
        self.__disconnect()

    def __update(self, tabela: Table, df: pd.DataFrame, pk_cols:list):
        self.__connect()

        for _, row in df.iterrows():
            q = Query.update(tabela)
            for col in row.index:
                if col not in pk_cols:
                    q = q.set(tabela[col], row[col])
            
            for col in pk_cols:
                q = q.where(tabela[col] == row[col])

            sql_statement = str(q)
            self._connection.execute(text(sql_statement))

        self._connection.commit()
        self.__disconnect()

    def sincronizar(self, tabela: str, new_df: pd.DataFrame, pk_cols: list):
        insert_df, update_df = self.comparar_dados(tabela, pk_cols, new_df)
        tabela = Table(tabela)

        self.__insert(tabela, insert_df)
        self.__update(tabela, update_df, pk_cols)
              