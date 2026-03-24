import os
from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from dotenv import load_dotenv

load_dotenv()


def get_db_engine():
    host = os.getenv("SUPABASE_DB_HOST")
    port = os.getenv("SUPABASE_DB_PORT", "5432")
    dbname = os.getenv("SUPABASE_DB_NAME", "postgres")
    user = os.getenv("SUPABASE_DB_USER")
    password = os.getenv("SUPABASE_DB_PASSWORD")

    if not all([host, dbname, user, password]):
        raise ValueError("Missing one or more database environment variables.")

    db_url = URL.create(
        drivername="postgresql+psycopg2",
        username=user,
        password=password,
        host=host,
        port=int(port),
        database=dbname,
    )

    engine = create_engine(
        db_url,
        connect_args={"sslmode": "require"}
    )
    return engine


def clean_column_names(df: pd.DataFrame) -> pd.DataFrame:
    rename_map = {
        "Date": "visit_date",
        "Personal number": "personal_number",
        "Gender": "gender",
        "Age": "age",
    }
    return df.rename(columns=rename_map)


def load_csv(csv_path: str) -> pd.DataFrame:
    df = pd.read_csv(csv_path)
    df = clean_column_names(df)

    required_cols = {"visit_date", "personal_number", "gender", "age"}
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    df["visit_date"] = pd.to_datetime(df["visit_date"], errors="coerce").dt.date
    df["personal_number"] = df["personal_number"].astype(str).str.strip()
    df["gender"] = df["gender"].astype(str).str.strip()
    df["age"] = pd.to_numeric(df["age"], errors="coerce").astype("Int64")

    df = df.replace(r"^\s*$", pd.NA, regex=True)

    return df


def create_tables(engine):
    sql = """
    create table if not exists public.clinic_visits (
        visit_id bigint generated always as identity primary key,
        visit_date date not null,
        personal_number text not null,
        gender text,
        age integer,
        raw_id bigint unique,
        created_at timestamptz default now()
    );

    create table if not exists public.dispensed_medications_long (
        med_id bigint generated always as identity primary key,
        visit_id bigint not null,
        raw_id bigint,
        visit_date date not null,
        personal_number text not null,
        drug text not null,
        quantity numeric not null,
        created_at timestamptz default now()
    );

    create index if not exists idx_clinic_visits_personal_number
    on public.clinic_visits(personal_number);

    create index if not exists idx_clinic_visits_visit_date
    on public.clinic_visits(visit_date);

    create index if not exists idx_dispensed_drug
    on public.dispensed_medications_long(drug);

    create index if not exists idx_dispensed_visit_date
    on public.dispensed_medications_long(visit_date);

    create index if not exists idx_dispensed_personal_number
    on public.dispensed_medications_long(personal_number);
    """
    with engine.begin() as conn:
        conn.execute(text(sql))


def upload_raw_data(df_raw: pd.DataFrame, engine, source_file_name: str):
    raw_upload = df_raw.copy()
    raw_upload["source_file_name"] = source_file_name

    # Let pandas create/replace raw table structure from the CSV
    raw_upload.to_sql(
        "clinic_visits_raw",
        engine,
        schema="public",
        if_exists="replace",
        index=True,
        index_label="raw_id",
        method="multi",
        chunksize=1000,
    )

    # Make raw_id a primary key and add uploaded_at if needed
    alter_sql = """
    do $$
    begin
        begin
            alter table public.clinic_visits_raw
            add primary key (raw_id);
        exception when others then
            null;
        end;

        begin
            alter table public.clinic_visits_raw
            add column uploaded_at timestamptz default now();
        exception when duplicate_column then
            null;
        end;
    end $$;
    """
    with engine.begin() as conn:
        conn.execute(text(alter_sql))


def rebuild_clean_tables(engine):
    sql = """
    truncate table public.dispensed_medications_long restart identity;
    truncate table public.clinic_visits restart identity;

    insert into public.clinic_visits (
        visit_date,
        personal_number,
        gender,
        age,
        raw_id
    )
    select
        visit_date,
        personal_number,
        gender,
        age,
        raw_id
    from public.clinic_visits_raw
    order by visit_date, personal_number, raw_id;
    """
    with engine.begin() as conn:
        conn.execute(text(sql))


def build_long_from_raw(engine):
    raw_df = pd.read_sql("select * from public.clinic_visits_raw", engine)
    visits_df = pd.read_sql(
        "select visit_id, raw_id, visit_date, personal_number from public.clinic_visits",
        engine
    )

    non_drug_cols = {
        "raw_id",
        "visit_date",
        "personal_number",
        "gender",
        "age",
        "source_file_name",
        "uploaded_at",
    }

    drug_cols = [c for c in raw_df.columns if c not in non_drug_cols]

    long_df = raw_df.melt(
        id_vars=["raw_id", "visit_date", "personal_number"],
        value_vars=drug_cols,
        var_name="drug",
        value_name="quantity"
    )

    long_df["quantity"] = pd.to_numeric(long_df["quantity"], errors="coerce")
    long_df = long_df.dropna(subset=["quantity"])
    long_df = long_df[long_df["quantity"] > 0]

    long_df = long_df.merge(
        visits_df,
        on=["raw_id", "visit_date", "personal_number"],
        how="left"
    )

    if long_df["visit_id"].isna().any():
        missing_rows = long_df[long_df["visit_id"].isna()]
        raise ValueError(
            f"Some medication rows could not be matched to visit_id. Sample:\\n{missing_rows.head()}"
        )

    final_long = long_df[[
        "visit_id",
        "raw_id",
        "visit_date",
        "personal_number",
        "drug",
        "quantity"
    ]].copy()

    final_long.to_sql(
        "dispensed_medications_long",
        engine,
        schema="public",
        if_exists="append",
        index=False,
        method="multi",
        chunksize=1000,
    )


def main():
    csv_path = os.getenv("CSV_FILE_PATH")
    source_file_name = os.getenv("SOURCE_FILE_NAME") or Path(csv_path).name

    if not csv_path:
        raise ValueError("CSV_FILE_PATH is not set in .env")

    engine = get_db_engine()

    print("Creating clean tables...")
    create_tables(engine)

    print("Loading CSV...")
    df = load_csv(csv_path)

    print("Uploading raw data...")
    upload_raw_data(df, engine, source_file_name)

    print("Rebuilding clinic_visits...")
    rebuild_clean_tables(engine)

    print("Building long medication table...")
    build_long_from_raw(engine)

    print("ETL completed successfully.")


if __name__ == "__main__":
    main()