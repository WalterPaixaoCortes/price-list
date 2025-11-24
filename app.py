# app.py
import os
import unicodedata
from datetime import datetime, timedelta
from typing import List, Optional

from fastapi import FastAPI, Query, HTTPException, Depends, Cookie, Form, Request
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
from sqlalchemy.engine import Connection
from itsdangerous import URLSafeTimedSerializer, BadSignature, SignatureExpired

# ================== Config ==================
DB_PATH = os.getenv("SQLITE_PATH", "app.db")
# DATABASE_URL = "mssql+pyodbc://scheduler:Rdl2023!@rdl-srvrsql01/esidb?driver=ODBC+Driver+17+for+SQL+Server"
DATABASE_URL = f"sqlite:///{DB_PATH}"

engine = create_engine(DATABASE_URL, future=True)
metadata = MetaData()

CONTENT_FOLDER = os.path.join(os.path.dirname(__file__), "content")

# ================== Auth / Session ==================
# secret used to sign the session cookie; override with env var in production
SECRET_KEY = os.getenv("SECRET_KEY", "dev-secret-change-me")
serializer = URLSafeTimedSerializer(SECRET_KEY)
SESSION_COOKIE = "session"


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
app = FastAPI(title="Lookup & PriceList API (SQLite)", version="3.0.0")
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


def seed_if_empty():
    """Cria tabelas e injeta dados se ainda não existirem."""
    metadata.create_all(engine)
    with engine.begin() as conn:
        # parts
        total_parts = conn.execute(text("SELECT COUNT(*) FROM parts")).scalar()
        if total_parts == 0:
            parts = [
                ("P-001", "Alpha Core"),
                ("P-002", "Alpine Edge"),
                ("P-003", "Aurora X"),
                ("P-004", "Bravo One"),
                ("P-005", "Beta Pro"),
                ("P-006", "Cobalt Vision"),
                ("P-007", "Crimson Jet"),
                ("P-008", "Delta Light"),
                ("P-009", "Echo Nova"),
                ("P-010", "Falcon Prime"),
                ("P-011", "Galaxy Beam"),
                ("P-012", "Helios Max"),
                ("P-013", "Indigo Flow"),
                ("P-014", "Jade Spark"),
                ("P-015", "Kappa Shield"),
                ("P-016", "Lunar Drive"),
                ("P-017", "Mercury Pad"),
                ("P-018", "Nimbus Ray"),
                ("P-019", "Orion Pulse"),
                ("P-020", "Phoenix Ultra"),
            ]
            conn.execute(
                Parts.insert(),
                [
                    {"id": pid, "label": label, "label_norm": norm(label)}
                    for pid, label in parts
                ],
            )
            print("parts seeded.")

        # categories
        total_cat = conn.execute(text("SELECT COUNT(*) FROM categories")).scalar()
        if total_cat == 0:
            cats = [
                ("CAT-A", "Hardware"),
                ("CAT-B", "Software"),
                ("CAT-C", "Services"),
                ("CAT-D", "Licensing"),
                ("CAT-E", "Support"),
            ]
            conn.execute(
                Categories.insert(), [{"id": cid, "name": name} for cid, name in cats]
            )

        # prices (pelo menos 100 – 20 parts x 5 categories)
        total_prices = conn.execute(text("SELECT COUNT(*) FROM prices")).scalar()
        if total_prices == 0:
            # carregue parts e categories do banco
            parts_rows = (
                conn.execute(select(Parts.c.id).order_by(Parts.c.id)).scalars().all()
            )
            cat_rows = (
                conn.execute(select(Categories.c.id).order_by(Categories.c.id))
                .scalars()
                .all()
            )
            now = datetime.utcnow()

            batch = []
            for pidx, partid in enumerate(parts_rows, start=1):
                for cidx, catid in enumerate(cat_rows, start=1):
                    # sku determinístico
                    num = partid.split("-")[1]
                    letter = chr(ord("A") + (cidx - 1))  # A..E
                    sku = f"SKU-{num}-{letter}"
                    desc = f"Package for {partid} - {catid}"
                    # preço determinístico (variação por índices)
                    price = round(50 + (pidx * 7.3) + (cidx * 11.1), 2)
                    # datas entre hoje-0 e hoje-30
                    eff = now - timedelta(days=((pidx * cidx) % 30))
                    batch.append(
                        {
                            "partid": partid,
                            "catid": catid,
                            "sku": sku,
                            "description": desc,
                            "price": price,
                            "currency": "USD",
                            "effective_date": eff,
                        }
                    )
            # insere ~100 linhas
            conn.execute(Prices.insert(), batch)
            conn.commit()


# roda o seed no import/boot
# seed_if_empty()

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
def do_login(password: str = Form(...)):
    """Authenticate using the ADMIN_PASSWORD env var and set a signed cookie."""
    admin_pw = os.getenv("ADMIN_PASSWORD")
    if not admin_pw:
        raise HTTPException(
            status_code=500,
            detail="ADMIN_PASSWORD not configured. Set the ADMIN_PASSWORD environment variable.",
        )
    if password != admin_pw:
        raise HTTPException(status_code=401, detail="Invalid credentials")
    token = create_session_token({"admin": True})
    resp = JSONResponse({"ok": True})
    # httponly cookie (not secure by default in dev)
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
