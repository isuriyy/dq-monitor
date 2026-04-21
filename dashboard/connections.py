"""
Connection Manager — dashboard/connections.py
"""
import streamlit as st
import yaml, os, subprocess, sys
from sqlalchemy import create_engine, inspect, text

SOURCES_PATH = "./config/sources.yaml"

def load_sources():
    if not os.path.exists(SOURCES_PATH):
        return []
    with open(SOURCES_PATH) as f:
        data = yaml.safe_load(f) or {}
    return data.get("sources", [])

def save_sources(sources):
    os.makedirs(os.path.dirname(SOURCES_PATH), exist_ok=True)
    with open(SOURCES_PATH, "w") as f:
        yaml.dump({"sources": sources}, f, default_flow_style=False)

def build_url(src):
    d = src.get("dialect","")
    if d == "sqlite":
        return f"sqlite:///{src['path']}"
    elif d == "postgresql":
        return f"postgresql+psycopg2://{src['user']}:{src['password']}@{src['host']}:{src.get('port',5432)}/{src['database']}"
    elif d == "mysql":
        return f"mysql+pymysql://{src['user']}:{src['password']}@{src['host']}:{src.get('port',3306)}/{src['database']}"
    elif d == "cloud":
        return src.get("connection_string","")
    return ""

def test_conn(src):
    try:
        url = build_url(src)
        if not url:
            return False, "Could not build URL — check all fields."
        kwargs = {"connect_args": {"connect_timeout": 5}} if src.get("dialect") != "sqlite" else {}
        engine = create_engine(url, **kwargs)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        tables = inspect(engine).get_table_names()
        return True, f"Connected. Found {len(tables)} table(s): {', '.join(tables[:5])}"
    except Exception as e:
        return False, str(e)

def get_tables(src):
    try:
        engine = create_engine(build_url(src))
        return inspect(engine).get_table_names()
    except Exception:
        return []

def render_connections_page():
    st.markdown("# ⚙️ Connection Manager")
    st.markdown("Connect to any database — SQLite, PostgreSQL, MySQL, or Cloud.")
    st.markdown("---")
    st.markdown("### Add new connection")

    dialects = {
        "sqlite":     ("📁","SQLite","Local .db file — no server"),
        "postgresql": ("🐘","PostgreSQL","Most common in companies"),
        "mysql":      ("🐬","MySQL","Web apps & e-commerce"),
        "cloud":      ("☁️","Cloud DB","BigQuery / RDS / Snowflake"),
    }

    if "sel_dialect" not in st.session_state:
        st.session_state["sel_dialect"] = "sqlite"

    cols = st.columns(4)
    for i,(d,(icon,label,desc)) in enumerate(dialects.items()):
        with cols[i]:
            if st.button(f"{icon} {label}", key=f"dbtype_{d}", use_container_width=True):
                st.session_state["sel_dialect"] = d
                st.rerun()
            st.caption(desc)

    st.markdown("---")
    dialect = st.session_state["sel_dialect"]
    new_src = {"dialect": dialect}

    if dialect == "sqlite":
        st.markdown("#### 📁 SQLite — local file")
        c1,c2 = st.columns(2)
        with c1:
            new_src["name"] = st.text_input("Connection name", value="my_local_db")
        with c2:
            new_src["path"] = st.text_input("File path (.db)", value="./data/ecommerce.db")
        st.info("No installation or server needed. Just point to your .db file.")

    elif dialect == "postgresql":
        st.markdown("#### 🐘 PostgreSQL")
        c1,c2 = st.columns(2)
        with c1:
            new_src["name"]     = st.text_input("Connection name", placeholder="e.g. production_db")
            new_src["host"]     = st.text_input("Host", placeholder="localhost")
            new_src["user"]     = st.text_input("Username", placeholder="postgres")
        with c2:
            new_src["database"] = st.text_input("Database name", placeholder="myapp")
            new_src["port"]     = st.number_input("Port", value=5432, min_value=1, max_value=65535)
            new_src["password"] = st.text_input("Password", type="password")
        st.info("Install driver first: `pip install psycopg2-binary`\n\nFree cloud options: Supabase, ElephantSQL, Railway")

    elif dialect == "mysql":
        st.markdown("#### 🐬 MySQL")
        c1,c2 = st.columns(2)
        with c1:
            new_src["name"]     = st.text_input("Connection name", placeholder="e.g. shop_db")
            new_src["host"]     = st.text_input("Host", placeholder="localhost")
            new_src["user"]     = st.text_input("Username", placeholder="root")
        with c2:
            new_src["database"] = st.text_input("Database name", placeholder="shopdb")
            new_src["port"]     = st.number_input("Port", value=3306, min_value=1, max_value=65535)
            new_src["password"] = st.text_input("Password", type="password")
        st.info("Install driver first: `pip install pymysql`")

    elif dialect == "cloud":
        st.markdown("#### ☁️ Cloud Database")
        c1,c2 = st.columns(2)
        with c1:
            new_src["name"] = st.text_input("Connection name", placeholder="e.g. bigquery_prod")
        with c2:
            new_src["cloud_type"] = st.selectbox("Cloud type", [
                "Google BigQuery","AWS RDS PostgreSQL","AWS RDS MySQL","Azure SQL","Snowflake"])
        new_src["connection_string"] = st.text_input("Connection string",
            placeholder="postgresql://user:pass@host:5432/db  or  bigquery://project/dataset")
        new_src["credentials_path"] = st.text_input("Credentials file (optional, for BigQuery)",
            placeholder="./credentials/bigquery_key.json")
        st.info("BigQuery: `pip install sqlalchemy-bigquery`\nSnowflake: `pip install snowflake-sqlalchemy`")

    st.markdown("")
    b1,b2,_ = st.columns([1,1,3])
    with b1:
        if st.button("🔌 Test connection", use_container_width=True):
            with st.spinner("Testing..."):
                ok, msg = test_conn(new_src)
            if ok:
                st.success(f"✓ {msg}")
            else:
                st.error(f"✗ {msg}")
    with b2:
        if st.button("💾 Save connection", type="primary", use_container_width=True):
            if not new_src.get("name"):
                st.error("Enter a connection name.")
            else:
                srcs = load_sources()
                if new_src["name"] in [s["name"] for s in srcs]:
                    st.warning(f"'{new_src['name']}' already exists. Use a different name.")
                else:
                    srcs.append(new_src)
                    save_sources(srcs)
                    st.success(f"✓ '{new_src['name']}' saved to sources.yaml")
                    st.rerun()

    # Saved connections
    st.markdown("---")
    st.markdown("### Saved connections")
    sources = load_sources()
    if not sources:
        st.info("No connections saved yet.")
    else:
        for i, src in enumerate(sources):
            icon = {"sqlite":"📁","postgresql":"🐘","mysql":"🐬","cloud":"☁️"}.get(src.get("dialect",""),"🔌")
            loc  = src.get("host", src.get("path", src.get("connection_string","")))
            with st.expander(f"{icon} **{src['name']}** — {src.get('dialect','').upper()} · {loc}"):
                d1,d2 = st.columns(2)
                with d1:
                    st.markdown(f"**Type:** {src.get('dialect','—')}")
                    if src.get("path"):  st.markdown(f"**Path:** `{src['path']}`")
                    if src.get("host"):  st.markdown(f"**Host:** `{src['host']}:{src.get('port','')}`")
                with d2:
                    if src.get("database"): st.markdown(f"**DB:** `{src['database']}`")
                    if src.get("user"):     st.markdown(f"**User:** `{src['user']}`")

                a1,a2,a3,a4 = st.columns(4)
                with a1:
                    if st.button("🔌 Test", key=f"t_{i}", use_container_width=True):
                        ok,msg = test_conn(src)
                        st.success(f"✓ {msg}") if ok else st.error(f"✗ {msg}")
                with a2:
                    if st.button("📋 Tables", key=f"tbl_{i}", use_container_width=True):
                        tables = get_tables(src)
                        if tables:
                            st.success(f"{len(tables)} tables found:")
                            for t in tables: st.markdown(f"  - `{t}`")
                        else:
                            st.warning("No tables found or connection failed.")
                with a3:
                    if st.button("▶ Run profiler", key=f"prof_{i}", use_container_width=True):
                        st.info(f"Running profiler on `{src['name']}`...")
                        try:
                            r = subprocess.run([sys.executable,"main.py"],
                                               capture_output=True, text=True, cwd=".")
                            if r.returncode == 0:
                                st.success("Profiler completed.")
                                st.code(r.stdout[-800:] if r.stdout else "Done")
                            else:
                                st.error("Profiler error.")
                                st.code(r.stderr[-400:] if r.stderr else "")
                        except Exception as e:
                            st.error(f"Error: {e}")
                with a4:
                    if st.button("🗑 Remove", key=f"rm_{i}", use_container_width=True):
                        sources.pop(i)
                        save_sources(sources)
                        st.success("Removed.")
                        st.rerun()

    # Quick setup guide
    st.markdown("---")
    st.markdown("### Quick setup guide")
    t1,t2,t3,t4 = st.tabs(["SQLite","PostgreSQL","MySQL","Cloud"])

    with t1:
        st.markdown("""
**No install needed.** Just enter the path to your `.db` file.

Example paths:
```
./data/ecommerce.db
C:/Users/Isuri/Documents/mydata.db
```
        """)
    with t2:
        st.markdown("""
**Install:**
```
pip install psycopg2-binary
```
**Free cloud PostgreSQL:**
- [Supabase](https://supabase.com) — generous free tier
- [ElephantSQL](https://elephantsql.com) — free 20MB
- [Railway](https://railway.app) — easy setup

**Format:** `host=db.xxxx.supabase.co  port=5432`
        """)
    with t3:
        st.markdown("""
**Install:**
```
pip install pymysql
```
**Local:** host=`localhost`, port=`3306`, user=`root`

**Free cloud MySQL:**
- [PlanetScale](https://planetscale.com) — free tier
- [Railway](https://railway.app) — MySQL addon
        """)
    with t4:
        st.markdown("""
**Google BigQuery:**
```
pip install sqlalchemy-bigquery google-cloud-bigquery
```
Connection: `bigquery://your-project/your-dataset`

**AWS RDS:** Use RDS endpoint as host (same as PostgreSQL/MySQL)

**Snowflake:**
```
pip install snowflake-sqlalchemy
```
Connection: `snowflake://user:pass@account/database/schema`
        """)
