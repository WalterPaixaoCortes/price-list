# app.py
import os
import unicodedata
from datetime import datetime, timedelta
from typing import List, Optional

from fastapi import FastAPI, Query, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from sqlalchemy import (
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

# ================== Config ==================
DB_PATH = os.getenv("SQLITE_PATH", "app.db")
DATABASE_URL = f"sqlite:///{DB_PATH}"

engine = create_engine(DATABASE_URL, future=True)
metadata = MetaData()

CONTENT_FOLDER = os.path.join(os.path.dirname(__file__), "content")

# ================== Tabelas ==================
Parts = Table(
    "parts",
    metadata,
    Column("id", String(50), primary_key=True),  # partid
    Column("label", String(200), nullable=False),
    Column("label_norm", String(200), nullable=False),  # para busca sem acentos/case
)
Index("ix_parts_label_norm", Parts.c.label_norm)

Categories = Table(
    "categories",
    metadata,
    Column("id", String(50), primary_key=True),  # catid
    Column("name", String(200), nullable=False),
)

Prices = Table(
    "prices",
    metadata,
    Column("partid", String(50), nullable=False),
    Column("catid", String(50), nullable=False),
    Column("sku", String(100), primary_key=True),
    Column("description", String(400), nullable=False),
    Column("price", Float, nullable=False),
    Column("currency", String(10), nullable=False, default="USD"),
    Column("effective_date", DateTime, nullable=False),
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
seed_if_empty()

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
    if partid:
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
        )
        .where(and_(*conditions))
        .order_by(Prices.c.partid, Prices.c.catid, Prices.c.sku)
        .limit(limit)
    )
    rows = conn.execute(stmt).mappings().all()
    return [PriceItem(**row) for row in rows]
