# app.py
import os
from dotenv import load_dotenv
import unicodedata
from datetime import datetime, timedelta
from typing import List, Optional

from fastapi import (
    FastAPI,
    File,
    Query,
    HTTPException,
    Depends,
    Cookie,
    Form,
    Request,
    UploadFile,
)
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from sqlalchemy import (
    Integer,
    create_engine,
    MetaData,
    Table,
    Column,
    String,
    Float,
    DateTime,
    text,
    select,
    and_,
    Index,
)
import pandas as pd
from sqlalchemy.engine import Connection
from itsdangerous import URLSafeTimedSerializer, BadSignature, SignatureExpired
import shutil
import tempfile
import zipfile
from pathlib import Path
import subprocess
import traceback

# ================== Config ==================
load_dotenv()

# Read configuration from environment (see .env.example)
DATABASE_URL = os.getenv(
    "DATABASE_URL", f"sqlite:///{os.path.join(os.path.dirname(__file__), 'app.db')}"
)

# create SQLAlchemy engine
engine = create_engine(DATABASE_URL, future=True)
metadata = MetaData()

CONTENT_FOLDER = os.getenv(
    "CONTENT_FOLDER", os.path.join(os.path.dirname(__file__), "content")
)
LISTS_FOLDER = os.getenv(
    "LISTS_FOLDER", os.path.join(os.path.dirname(__file__), "lists")
)

# ================== Auth / Session ==================
# secret used to sign the session cookie; override with env var in production
SECRET_KEY = os.getenv("SECRET_KEY", "123456")
serializer = URLSafeTimedSerializer(SECRET_KEY)
SESSION_COOKIE = os.getenv("SESSION_COOKIE_NAME", "123456")


def create_session_token(data: dict) -> str:
    return serializer.dumps(data)


def verify_session_token(token: str, max_age: int = 86400) -> dict:
    try:
        return serializer.loads(token, max_age=max_age)
    except SignatureExpired:
        raise HTTPException(status_code=401, detail="Session expired")
    except BadSignature:
        raise HTTPException(status_code=401, detail="Invalid session token")


def require_admin(session: str = Cookie(None)):
    """Dependency to require admin session cookie. Raises 401 if invalid."""
    if not session:
        raise HTTPException(status_code=401, detail="Not authenticated")
    data = verify_session_token(session)
    if not data or not data.get("admin"):
        raise HTTPException(status_code=401, detail="Not authorized")
    return True


# ================== Tabelas ==================
Parts = Table(
    "pricelist_parts",
    metadata,
    Column("id", String(50), primary_key=True),  # partid
    Column("label", String(200), nullable=False),
)
Index("ix_parts_id", Parts.c.id)

Categories = Table(
    "pricelist_categories",
    metadata,
    Column("id", String(50), primary_key=True),  # catid
    Column("name", String(200), nullable=False),
)

Prices = Table(
    "pricelist_prices",
    metadata,
    Column("partid", String(50), nullable=False),
    Column("catid", String(50), nullable=False),
    Column("sku", String(100), primary_key=True),
    Column("description", String(400), nullable=False),
    Column("price", Float, nullable=False),
    Column("currency", String(10), nullable=False, default="USD"),
    Column("effective_date", DateTime, nullable=False),
    Column("qty", Integer, nullable=False),
)

# ================== App ==================
app = FastAPI(title="Price List Management", version="3.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # ajuste em produção
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Global middleware to protect endpoints (except allowlist)
@app.middleware("http")
async def admin_middleware(request: Request, call_next):
    path = request.url.path
    # allowlist: public routes (root `/` removed to require login)
    allowlist = {"/login", "/logout", "/openapi.json", "/docs", "/redoc"}
    if path.startswith("/static") or path in allowlist:
        return await call_next(request)

    token = request.cookies.get(SESSION_COOKIE)
    # helper to decide if client likely expects HTML
    accepts_html = "text/html" in request.headers.get("accept", "")

    if not token:
        # if browser GET, redirect to login; otherwise JSON 401
        if request.method == "GET" and (accepts_html or path == "/"):
            return RedirectResponse(url="/login")
        return JSONResponse({"detail": "Not authenticated"}, status_code=401)
    try:
        # verify token (will raise HTTPException on failure)
        verify_session_token(token)
    except HTTPException as e:
        if request.method == "GET" and (accepts_html or path == "/"):
            return RedirectResponse(url="/login")
        return JSONResponse({"detail": e.detail}, status_code=e.status_code)

    return await call_next(request)


# ================== Schemas Pydantic ==================
class PartItem(BaseModel):
    id: str
    label: str


class Category(BaseModel):
    id: str
    name: str


class PriceItem(BaseModel):
    partid: str
    catid: str
    sku: str
    description: str
    price: float
    currency: str
    effective_date: datetime
    qty: int


# ================== Helpers ==================
def norm(s: str) -> str:
    """remove acentos e lowercase p/ busca em prefixo"""
    nfkd = unicodedata.normalize("NFKD", s or "")
    return "".join(c for c in nfkd if not unicodedata.combining(c)).strip()


def get_conn():
    with engine.connect() as conn:
        yield conn


app.mount(
    "/static",
    StaticFiles(directory=os.path.join(CONTENT_FOLDER)),
    name="static",
)


# ================== Endpoints ==================
@app.get("/", include_in_schema=False)
def serve_index():
    index_path = os.path.join(CONTENT_FOLDER, "index.html")
    if not os.path.exists(index_path):
        raise HTTPException(
            status_code=404, detail="index.html não encontrado na pasta content/"
        )
    return FileResponse(index_path, media_type="text/html")


@app.get("/export", include_in_schema=False)
def serve_export():
    export_path = os.path.join(CONTENT_FOLDER, "export.html")
    if os.path.exists(export_path):
        return FileResponse(export_path, media_type="text/html")
    raise HTTPException(
        status_code=404, detail="export.html não encontrado na pasta content/"
    )


@app.get("/lookup", include_in_schema=False)
def serve_lookup():
    lookup_path = os.path.join(CONTENT_FOLDER, "lookup.html")
    if os.path.exists(lookup_path):
        return FileResponse(lookup_path, media_type="text/html")
    raise HTTPException(
        status_code=404, detail="lookup.html não encontrado na pasta content/"
    )


# --- Export APIs: list/upload/delete templates, list/delete outputs, generate, download_all ---
TEMPLATES_DIR = os.path.join(LISTS_FOLDER, "templates")
OUTPUT_DIR = os.path.join(LISTS_FOLDER, "output")
LOOKUP_DIR = os.path.join(LISTS_FOLDER, "lookup")


def ensure_dirs():
    os.makedirs(TEMPLATES_DIR, exist_ok=True)
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    os.makedirs(LOOKUP_DIR, exist_ok=True)


def list_files(root_dir: str):
    files = []
    for base, dirs, filenames in os.walk(root_dir):
        for fn in filenames:
            if fn == ".gitkeep":
                continue
            full = os.path.join(base, fn)
            rel = os.path.relpath(full, root_dir)
            files.append({"name": fn, "path": rel.replace("\\\\", "/"), "full": full})
            # ignore repository placeholder files
    return files


@app.get("/api/templates")
def api_list_templates():
    ensure_dirs()
    return list_files(TEMPLATES_DIR)


@app.get("/api/lookups")
def api_list_lookups():
    ensure_dirs()
    return list_files(LOOKUP_DIR)


@app.post("/api/lookups")
def api_upload_lookup(file: UploadFile = File(...)):
    ensure_dirs()
    safe_name = os.path.basename(file.filename)
    dest = os.path.join(LOOKUP_DIR, safe_name)
    with open(dest, "wb") as out_f:
        shutil.copyfileobj(file.file, out_f)
    return {"ok": True, "name": safe_name}


@app.get("/api/lookups/{name}")
def api_get_lookup(name: str):
    ensure_dirs()
    safe = os.path.basename(name)
    target = os.path.normpath(os.path.join(LOOKUP_DIR, safe))
    if not target.startswith(LOOKUP_DIR):
        raise HTTPException(status_code=400, detail="Invalid path")
    if not os.path.exists(target):
        raise HTTPException(status_code=404, detail="File not found")

    # read CSV
    import csv

    rows = []
    with open(target, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        cols = reader.fieldnames or []
        for r in reader:
            rows.append(r)

    return {"ok": True, "name": safe, "columns": cols, "rows": rows}


@app.put("/api/lookups/{name}")
def api_put_lookup(name: str, payload: dict):
    """Replace CSV contents. Expects payload: { rows: [ {col: val}, ... ] }"""
    ensure_dirs()
    safe = os.path.basename(name)
    target = os.path.normpath(os.path.join(LOOKUP_DIR, safe))
    if not target.startswith((LOOKUP_DIR)):
        raise HTTPException(status_code=400, detail="Invalid path")

    rows = payload.get("rows")
    if rows is None:
        raise HTTPException(status_code=400, detail="Missing rows in payload")

    # determine columns from first row or empty
    cols = []
    if rows:
        first = rows[0]
        if isinstance(first, dict):
            cols = list(first.keys())
        else:
            raise HTTPException(status_code=400, detail="Rows must be list of objects")

    import csv

    # write CSV
    with open(target, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=cols)
        writer.writeheader()
        for r in rows:
            writer.writerow(r)

    return {"ok": True, "name": safe}


@app.delete("/api/lookups")
def api_delete_lookup(
    path: str = Query(..., description="relative path under lookups")
):
    ensure_dirs()
    target = os.path.normpath(os.path.join(LOOKUP_DIR, path))
    if not target.startswith(LOOKUP_DIR):
        raise HTTPException(status_code=400, detail="Invalid path")
    if not os.path.exists(target):
        raise HTTPException(status_code=404, detail="File not found")
    os.remove(target)
    return {"ok": True}


@app.post("/api/templates")
def api_upload_template(file: UploadFile = File(...)):
    ensure_dirs()
    safe_name = os.path.basename(file.filename)
    dest = os.path.join(TEMPLATES_DIR, safe_name)
    with open(dest, "wb") as out_f:
        shutil.copyfileobj(file.file, out_f)
    return {"ok": True, "name": safe_name}


@app.delete("/api/templates")
def api_delete_template(
    path: str = Query(..., description="relative path under templates")
):
    ensure_dirs()
    target = os.path.normpath(os.path.join(TEMPLATES_DIR, path))
    if not target.startswith(os.path.abspath(TEMPLATES_DIR)):
        raise HTTPException(status_code=400, detail="Invalid path")
    if not os.path.exists(target):
        raise HTTPException(status_code=404, detail="File not found")
    os.remove(target)
    return {"ok": True}


@app.get("/api/outputs")
def api_list_outputs():
    ensure_dirs()
    return list_files(OUTPUT_DIR)


@app.post("/api/import_price_update")
def api_import_price_update(payload: List[dict],
    conn: Connection = Depends(get_conn)):
    """Receive rows (array of objects) and import into dbo.0_Price_Update.

    The endpoint will truncate the target table before inserting the new rows.
    """
    if not payload:
        raise HTTPException(status_code=400, detail="No rows provided")

    try:
        # build DataFrame
        df = pd.DataFrame(payload)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid payload: {e}")

    # Ensure we have an engine defined and available
    if engine is None:
        raise HTTPException(status_code=500, detail="Database engine not configured")

    table_name = "0_Price_Update"
    schema_name = "esidemo.dbo"

    try:
        # Try to truncate first (may require higher privileges); fall back to delete
        with engine.begin() as conn:
            print(f"TRUNCATE TABLE {schema_name}.{table_name}")
            try:
                conn.execute(text(f"TRUNCATE TABLE {schema_name}.[{table_name}]"))
            except Exception as e:
                print(traceback.format_exc())
                conn.execute(text(f"DELETE FROM {schema_name}.[{table_name}]"))
    except Exception:
        print("oi1")
        # If truncation/delete failed, continue but report error
        raise HTTPException(status_code=500, detail="Failed to empty target table")

    try:
        # append dataframe to SQL table
        df.to_sql(
            table_name,
            con=engine,
            schema=schema_name,
            if_exists="append",
            index=False,
            method="multi",
        )
    except Exception as e:
        print("oi2")
        raise HTTPException(status_code=500, detail=f"Failed to insert rows: {e}")

    return {"ok": True, "rows": len(df)}


@app.delete("/api/outputs")
def api_delete_output(
    path: str = Query(..., description="relative path under outputs")
):
    ensure_dirs()
    target = os.path.normpath(os.path.join(OUTPUT_DIR, path))
    if not target.startswith(os.path.abspath(OUTPUT_DIR)):
        raise HTTPException(status_code=400, detail="Invalid path")
    if not os.path.exists(target):
        raise HTTPException(status_code=404, detail="File not found")
    os.remove(target)
    return {"ok": True}


@app.post("/api/generate")
def api_generate_exports():
    """Run the prepare-list CLI to generate export files.

    This executes the script at `libs/prepare-list.py` with the templates and output
    directories and the configured SQL/schema. Returns CLI output and the list
    of generated output files.
    """
    ensure_dirs()
    # build command
    script_path = os.path.join(os.path.dirname(__file__), "libs", "prepare-list.py")
    if not os.path.exists(script_path):
        # fallback to simple generator
        now = datetime.utcnow().strftime("%Y%m%d%H%M%S")
        outp = os.path.join(OUTPUT_DIR, f"export_{now}.csv")
        with open(outp, "w", encoding="utf-8") as f:
            f.write("sku,description,price\n")
            f.write("SAMPLE,Sample export,0.00\n")
        return {
            "ok": True,
            "generated": [os.path.relpath(outp, OUTPUT_DIR)],
            "note": "prepare-list script not found; created placeholder",
        }

    cmd = [
        os.environ.get("PYTHON_EXECUTABLE", "python"),
        script_path,
        "--input",
        TEMPLATES_DIR,
        "--output",
        OUTPUT_DIR,
        "--schema",
        os.path.join("lists", "schema.json"),
        "--sql",
        os.path.join("sql-scripts", "extract_query.sql"),
        "--db-uri",
        DATABASE_URL,
    ]

    try:
        proc = subprocess.run(
            cmd, capture_output=True, text=True, check=False, timeout=300
        )
    except subprocess.TimeoutExpired:
        raise HTTPException(status_code=500, detail="Generation timed out")

    stdout = proc.stdout or ""
    stderr = proc.stderr or ""

    if proc.returncode != 0:
        return JSONResponse(
            {
                "ok": False,
                "returncode": proc.returncode,
                "stdout": stdout,
                "stderr": stderr,
            },
            status_code=500,
        )

    # list outputs produced
    generated = [f["path"] for f in list_files(OUTPUT_DIR)]
    return {"ok": True, "generated": generated, "stdout": stdout}


@app.get("/api/download_all")
def api_download_all():
    ensure_dirs()
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".zip")
    tmp.close()
    with zipfile.ZipFile(tmp.name, "w", zipfile.ZIP_DEFLATED) as zf:
        for base, dirs, files in os.walk(OUTPUT_DIR):
            for fn in files:
                full = os.path.join(base, fn)
                arcname = os.path.relpath(full, OUTPUT_DIR)
                zf.write(full, arcname)
    return FileResponse(tmp.name, media_type="application/zip", filename="exports.zip")


# Simple login page (static)
@app.get("/login", include_in_schema=False)
def serve_login():
    login_path = os.path.join(CONTENT_FOLDER, "login.html")
    if os.path.exists(login_path):
        return FileResponse(login_path, media_type="text/html")
    # fallback - small builtin form
    html = """
    <html><body>
    <h2>Admin Login</h2>
    <form method=post action="/login">
      <label>Password: <input type=password name=password></label>
      <button type=submit>Login</button>
    </form>
    </body></html>
    """
    return HTMLResponse(content=html)


@app.post("/login", include_in_schema=False)
def do_login(request: Request, password: str = Form(...)):
    """Authenticate using the ADMIN_PASSWORD env var and set a signed cookie.

    For form POSTs from browsers: on success redirect to `/`, on failure re-render login with an error.
    For non-browser clients, fallback to JSON responses.
    """
    admin_pw = os.getenv("ADMIN_PASSWORD")
    # missing configuration
    if not admin_pw:
        # if browser, show simple page with error
        if "text/html" in request.headers.get("accept", ""):
            return HTMLResponse(
                "<h1>Server misconfigured: ADMIN_PASSWORD not set</h1>", status_code=500
            )
        raise HTTPException(
            status_code=500,
            detail="ADMIN_PASSWORD not configured. Set the ADMIN_PASSWORD environment variable.",
        )

    if password != admin_pw:
        # wrong password: for browser show login page with message, otherwise 401 JSON
        resp = RedirectResponse(url="/", status_code=302)
        return resp

    # success: create token and redirect to /
    token = create_session_token({"admin": True})
    resp = RedirectResponse(url="/", status_code=302)
    resp.set_cookie(SESSION_COOKIE, token, httponly=True, samesite="lax", max_age=86400)
    return resp


@app.post("/logout", include_in_schema=False)
def do_logout():
    resp = JSONResponse({"ok": True})
    resp.delete_cookie(SESSION_COOKIE)
    return resp


@app.get(
    "/find_partid",
    response_model=List[PartItem],
    summary="Busca items cujo label inicia com o texto",
)
def find_partid(
    q: str = Query(..., description="Pedaço de texto (prefixo)"),
    limit: int = Query(10, ge=1, le=100, description="Limite de itens retornados"),
    conn: Connection = Depends(get_conn),
):
    nq = norm(q)
    stmt = (
        select(Parts.c.id, Parts.c.label)
        .where(Parts.c.id.like(f"{nq}%"))
        .order_by(Parts.c.label)
        .limit(limit)
    )
    print(nq)
    print(stmt)
    rows = conn.execute(stmt).mappings().all()
    return [PartItem(**row) for row in rows]


@app.get(
    "/categories",
    response_model=List[Category],
    summary="Lista todas as categorias disponíveis",
)
def list_categories(conn: Connection = Depends(get_conn)):
    rows = (
        conn.execute(
            select(Categories.c.id, Categories.c.name).order_by(Categories.c.name)
        )
        .mappings()
        .all()
    )
    return [Category(**row) for row in rows]


@app.get(
    "/items",
    response_model=List[PriceItem],
    summary="Lista itens da price list por partid/categoria",
)
def list_items(
    partid: Optional[List[str]] = Query(None, description="partid=P-001&partid=P-005"),
    catid: Optional[List[str]] = Query(None, description="catid=CAT-A&catid=CAT-C"),
    limit: int = Query(100, ge=1, le=1000, description="Limite de itens retornados"),
    conn: Connection = Depends(get_conn),
):
    if not partid and not catid:
        raise HTTPException(
            status_code=400, detail="Envie ao menos um filtro: partid e/ou catid."
        )
    conditions = []
    # print(partid)
    if partid and partid[0] != "ALL":
        conditions.append(Prices.c.partid.in_(partid))
    if catid:
        conditions.append(Prices.c.catid.in_(catid))

    stmt = (
        select(
            Prices.c.partid,
            Prices.c.catid,
            Prices.c.sku,
            Prices.c.description,
            Prices.c.price,
            Prices.c.currency,
            Prices.c.effective_date,
            Prices.c.qty,
        )
        .where(and_(*conditions))
        .order_by(Prices.c.partid, Prices.c.catid, Prices.c.sku)
    )

    sql = stmt.compile(engine, compile_kwargs={"literal_binds": True})
    print(str(sql))

    rows = conn.execute(stmt).mappings().all()
    records = []
    for row in rows:
        el = {
            "partid": row["partid"],
            "catid": row["catid"],
            "sku": str(row["sku"]),
            "description": row["description"],
            "price": row["price"],
            "currency": row["currency"],
            "effective_date": row["effective_date"],
            "qty": row["qty"],
        }
        records.append(el)
    return records
