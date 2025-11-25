# etl/db_writer.py
import os
import pandas as pd
from sqlalchemy import (create_engine, MetaData, Table, Column, Integer, String,
                        Boolean, DateTime, Float, ForeignKey, text)
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import create_async_engine
from dotenv import load_dotenv



HISTORY_DIR = "storage/history"

#  DB Writer Class
class DBWriter:
    def __init__(self):
        load_dotenv()
        url = os.getenv('DATABASE_URL')
        
        if not url.startswith("postgresql+psycopg2"):
            url = url.replace("postgresql://", "postgresql+psycopg2://", 1)
        self.engine = create_engine(url, echo=False, future=True)

        self.meta = MetaData()

        # TABLE DEFINITIONS 
        self.banks = Table(
            "banks", self.meta,
            Column("bank_id", Integer, primary_key=True),
            Column("bank_uid", String),
            Column("bank_name", String),
            Column("bank_url", String),
            Column("bank_phone", String),
            Column("bank_address", String),
            Column("bank_code", String),
            Column("idx_license", String)
        )

        self.regions = Table(
            "regions", self.meta,
            
            Column("region_id", String, primary_key=True),
            Column("region_name", String),
            Column("area_name", String),
            Column("is_city", Boolean),
            Column("is_regional_center", Boolean),
            Column("parent_id", String),
            Column("kladr_code", String),
            Column("region_url", String),
            Column("count", Integer),

            # SCD2 + технические поля
            Column("product_hash", String, index=True),
            Column("is_actual", Boolean),
            Column("start_time", DateTime),
            Column("end_time", DateTime),
            
            # schema=self.schema,
        )



        self.credit_products = Table(
            "credit_products", self.meta,
            Column("offer_uid", String, primary_key=True),
            Column("offer_id", String),
            Column("offer_type", String),
            Column("offer_pledge", String),
            Column("offer_url", String),
            Column("img_logo_url", String),
            Column("bank_id", Integer),
            Column("bank_uid", String),
            Column("bank_name", String),
            Column("currency", String),
            Column("rateMin", Float),
            Column("rateMax", Float),
            Column("amountMin", Float),
            Column("amountMax", Float),
            Column("termMin", Integer),
            Column("termMax", Integer),
            Column("periodFromNotation", String),
            Column("periodToNotation", String),
            # SCD2
            Column("product_hash", String),
            Column("is_actual", Boolean),
            Column("start_time", DateTime),
            Column("end_time", DateTime),
        )

        self.credit_regions = Table(
            "credit_regions", self.meta,
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("offer_uid", ForeignKey("credit_products.offer_uid")),
            Column("region_id", ForeignKey("regions.region_id")),
            Column("is_actual", Boolean),
            Column("start_time", DateTime),
            Column("end_time", DateTime),
            Column("product_hash", String),
        )

        self.deposit_products = Table(
            "deposit_products", self.meta,
            Column("offer_uid", String, primary_key=True),
            Column("offer_pledge", String),
            Column("offer_url", String),
            Column("img_logo_url", String),
            Column("bank_id", Integer),
            Column("bank_name", String),
            Column("currency", String),
            Column("rateMin", Float),
            Column("rateMax", Float),
            Column("amountMin", Float),
            Column("amountMax", Float),
            Column("termMin", Float),
            Column("termMax", Float),
            Column("rateEfficient", Float),
            # SCD2
            Column("product_hash", String),
            Column("is_actual", Boolean),
            Column("start_time", DateTime),
            Column("end_time", DateTime),
        )

        self.deposit_regions = Table(
            "deposit_regions", self.meta,
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("offer_uid", ForeignKey("deposit_products.offer_uid")),
            Column("region_id", ForeignKey("regions.region_id")),
            Column("is_actual", Boolean),
            Column("start_time", DateTime),
            Column("end_time", DateTime),
            Column("product_hash", String),
        )

    # Create schema
    def create_schema(self):
        print("[DB] Creating schema if not exists...")
        self.meta.create_all(self.engine)
        print("[DB] Schema created")

    # Generic loader
    def load_csv(self, filename: str):
        path = os.path.join(HISTORY_DIR, filename)
        if not os.path.exists(path):
            print(f"[DB] No file: {path}")
            return None
        df = pd.read_csv(path)
        if df.empty:
            print(f"[DB] Empty: {path}")
            return None
        return df


    # Load functions
    def _upsert_table(self, table, df: pd.DataFrame, pk: str):
        """Generic UPSERT by primary key.

        - Обрезаем колонки df до тех, что есть в table.
        - Если нечего обновлять (upd == {}), делаем on_conflict_do_nothing.
        """
        if df is None:
            return

        # keep only columns that exist in the target table
        table_cols = {c.name for c in table.columns}
        df = df.copy()
        # drop columns that are completely missing in table
        cols_to_use = [c for c in df.columns if c in table_cols]
        df = df[cols_to_use]

        if df.empty:
            print(f"[DB] Nothing to insert for table {table.name} (no matching columns).")
            return

        # fill NaN -> None to produce None in inserts
        df = df.where(pd.notnull(df), None)

        with self.engine.begin() as conn:
            for _, row in df.iterrows():
                data = row.to_dict()
                stmt = insert(table).values(**data)

                # exclude PK from update set
                upd = {k: data[k] for k in data.keys() if k != pk}

                if not upd:
                    # nothing to update => ignore duplicates
                    stmt = stmt.on_conflict_do_nothing(index_elements=[pk])
                else:
                    stmt = stmt.on_conflict_do_update(index_elements=[pk], set_=upd)

                conn.execute(stmt)

    def load_credit_products(self):
        df = self.load_csv("credits_products_history.csv")
        df['end_time'] = df['end_time'].apply(lambda x: None if pd.isna(x) else x)
        df['start_time'] = df['start_time'].apply(lambda x: None if pd.isna(x) else x)
        self._upsert_table(self.credit_products, df, "offer_uid")

    def load_deposit_products(self):
        df = self.load_csv("deposits_products_history.csv")
        df['end_time'] = df['end_time'].apply(lambda x: None if pd.isna(x) else x)
        df['start_time'] = df['start_time'].apply(lambda x: None if pd.isna(x) else x)
        self._upsert_table(self.deposit_products, df, "offer_uid")

    def load_regions(self):
        # read regions history file created by transform
        path = os.path.join(HISTORY_DIR, "regions.csv")
        if not os.path.exists(path):
            print(f"[DB] No regions history file at {path}")
            return

        df = pd.read_csv(path, dtype=object)  # read as object, we will cast below
        if df.empty:
            print(f"[DB] regions.csv is empty")
            return

        for bcol in ("is_city", "is_regional_center", "is_actual"):
            if bcol in df.columns:
                df[bcol] = df[bcol].astype(str).str.strip().replace({"None": None, "nan": None, "": None})
                df[bcol] = df[bcol].map(lambda v: True if str(v).lower() in ("true", "1", "yes") else (False if str(v).lower() in ("false", "0", "no") else None))

        # numeric conversions
        if "count" in df.columns:
            df["count"] = pd.to_numeric(df["count"], errors="coerce").astype('Int64')

        # datetimes: start_time/end_time (keep as strings if you prefer)
        for dtcol in ("start_time", "end_time"):
            if dtcol in df.columns:
                # try parse; leave as string on failure
                try:
                    df[dtcol] = pd.to_datetime(df[dtcol], errors="coerce")
                except Exception:
                    pass

        # sqlalchemy не работает с пандосовскими natами, поэтому нужно врнучную вбивать питон-наны
        df['end_time'] = df['end_time'].apply(lambda x: None if pd.isna(x) else x)
        df['start_time'] = df['start_time'].apply(lambda x: None if pd.isna(x) else x)

        self._upsert_table(self.regions, df, "region_id")
        print(f"[DB] Regions upsert attempted, rows: {len(df)}")


    def load_credit_regions(self):
        df = self.load_csv("credits_regions_history.csv")
        if df is None:
            return
        df['end_time'] = df['end_time'].apply(lambda x: None if pd.isna(x) else x)
        df['start_time'] = df['start_time'].apply(lambda x: None if pd.isna(x) else x)
        self._upsert_table(self.credit_regions, df, "id")

        existing_regions = set(
            row[0] for row in self.connection.execute(
                self.regions.select().with_only_columns([self.regions.c.id])
            ).fetchall()
        )
        
        new_region_ids = set(df['region_id'].unique()) - existing_regions
        if new_region_ids:
            new_regions_df = pd.DataFrame([
                {'id': rid, 'name': f'Unknown {rid}'} for rid in new_region_ids
            ])
            self._upsert_table(self.regions, new_regions_df, 'id')
            print(f"[INFO] Добавлено {len(new_region_ids)} новых регионов в таблицу regions")


        self._upsert_table(self.credit_regions, df, "id")
        print(f"[INFO] Загружено {len(df)} записей в таблицу credit_regions")

    def load_deposit_regions(self):
        df = self.load_csv("deposits_regions_history.csv")
        if df is None:
            return
        
        df['end_time'] = df['end_time'].apply(lambda x: None if pd.isna(x) else x)
        df['start_time'] = df['start_time'].apply(lambda x: None if pd.isna(x) else x)

        # Получаем все существующие region_id из таблицы regions
        existing_regions = set(
            row[0] for row in self.connection.execute(
                self.regions.select().with_only_columns([self.regions.c.id])
            ).fetchall()
        )


        new_region_ids = set(df['region_id'].unique()) - existing_regions


        if new_region_ids:
            new_regions_df = pd.DataFrame([
                {'id': rid, 'name': f'Unknown {rid}'} for rid in new_region_ids
            ])
            self._upsert_table(self.regions, new_regions_df, 'id')
            print(f"[INFO] Добавлено {len(new_region_ids)} новых регионов в таблицу regions")


        self._upsert_table(self.deposit_regions, df, "id")
        print(f"[INFO] Загружено {len(df)} записей в таблицу deposit_regions")

    # orchestrator
    def run_all(self):
        # коммент - дроп, если чет переделать надо
        # self.meta.drop_all(self.engine)
        self.create_schema()
        self.load_regions()
        self.load_credit_products()
        self.load_deposit_products()
        self.load_credit_regions()
        self.load_deposit_regions()
        print("[DB] All data loaded into PostgreSQL")

