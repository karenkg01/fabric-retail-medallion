#!/usr/bin/env python3
"""
generate_retail_data.py
=======================

Source-system simulator for the *fabric-retail-medallion* portfolio project.

It plays the role of the operational systems of a fictional US retailer,
**Harborline Retail** (24 stores + 1 e-commerce site), and drops daily extract
files exactly the way a real vendor feed or SFTP drop would:

    output/landing/<source>/YYYY/MM/DD/<source>_YYYYMMDD.<csv|json>
    output/landing/_manifests/YYYY/MM/DD/manifest_YYYYMMDD.json

Sources produced
----------------
orders        POS + e-commerce order headers (daily CSV, incremental)
order_lines   order line items (daily CSV, incremental)
customers     CRM extract: full on day 1, then new + changed records (CSV)
products      catalog API dump: full on day 1, then changes only (nested JSON)
stores        store master: full extract on day 1 and when a store opens (CSV)
inventory     daily on-hand snapshot per store x product (CSV)
returns       returns / refunds (daily CSV)

Every manifest lists each file with its row count so the Bronze layer can do a
row-count reconciliation (completeness check).

The data is deliberately *imperfect* (duplicates, late-arriving orders, bad
dates, orphan keys, inconsistent casing, negative values, amount mismatches...)
so the Silver layer has real cleansing and data-quality work to do. Rates are
configurable in ``Config``.

Modes
-----
history   Build the initial backfill (default: 2 years) and save the simulator
          state.
daily     Load the saved state and simulate the next N business days, so new
          files keep arriving for incremental loads.

Determinism: every day is generated from ``seed + date``, so running
``history`` once gives exactly the same files as running ``history`` for the
first day and ``daily`` for every day after it.

Examples
--------
    python generate_retail_data.py history
    python generate_retail_data.py history --start 2024-10-01 --end 2026-09-30 --scale 0.25
    python generate_retail_data.py daily --days 1

Only depends on numpy and pandas, so it also runs inside a Microsoft Fabric
notebook (write to /lakehouse/default/Files/...).
"""
from __future__ import annotations

import argparse
import json
import pickle
import shutil
import sys
import time as _time
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from functools import lru_cache
from pathlib import Path

import numpy as np
import pandas as pd

GENERATOR_VERSION = "1.0.0"
RETAILER = "Harborline Retail"


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
@dataclass
class Config:
    seed: int = 42
    scale: float = 1.0                 # multiplies order volume and customer base
    base_orders_per_day: float = 520.0
    initial_customers: int = 32_000
    new_customers_per_day: float = 25.0
    customer_change_rate: float = 0.0012   # share of customers changing per day
    initial_products: int = 480
    inventory_days: int = 120          # history mode: write inventory snapshots for the last N days

    # --- "real world" data problems (rates are per row unless noted) ---
    dup_order_rate: float = 0.003
    dup_line_rate: float = 0.005
    late_arrival_rate: float = 0.02    # orders delivered 1-3 days late
    neg_qty_rate: float = 0.002
    orphan_product_rate: float = 0.001
    alt_date_format_rate: float = 0.005
    bad_date_rate: float = 0.0005
    amount_mismatch_rate: float = 0.002
    status_case_rate: float = 0.01
    orphan_return_rate: float = 0.01
    return_reason_variant_rate: float = 0.15
    inventory_missing_rate: float = 0.005
    inventory_negative_rate: float = 0.0005
    inventory_store_skip_rate: float = 0.002
    customer_dirty_rate: float = 0.03
    product_category_dirty_rate: float = 0.02


# ---------------------------------------------------------------------------
# Reference data
# ---------------------------------------------------------------------------
# (city, state, zip prefix, region) - one physical store per city
CITIES = [
    ("Tampa", "FL", "336", "Southeast"), ("Orlando", "FL", "328", "Southeast"),
    ("Miami", "FL", "331", "Southeast"), ("Jacksonville", "FL", "322", "Southeast"),
    ("Sarasota", "FL", "342", "Southeast"), ("Atlanta", "GA", "303", "Southeast"),
    ("Charlotte", "NC", "282", "Southeast"), ("Nashville", "TN", "372", "Southeast"),
    ("Dallas", "TX", "752", "South Central"), ("Houston", "TX", "770", "South Central"),
    ("Austin", "TX", "787", "South Central"), ("Chicago", "IL", "606", "Midwest"),
    ("Columbus", "OH", "432", "Midwest"), ("Indianapolis", "IN", "462", "Midwest"),
    ("Minneapolis", "MN", "554", "Midwest"), ("New York", "NY", "100", "Northeast"),
    ("Boston", "MA", "021", "Northeast"), ("Philadelphia", "PA", "191", "Northeast"),
    ("Denver", "CO", "802", "West"), ("Phoenix", "AZ", "850", "West"),
    ("Seattle", "WA", "981", "West"), ("Los Angeles", "CA", "900", "West"),
    ("San Diego", "CA", "921", "West"), ("Portland", "OR", "972", "West"),
]
STATE_NAMES = {
    "FL": "Florida", "GA": "Georgia", "NC": "North Carolina", "TN": "Tennessee",
    "TX": "Texas", "IL": "Illinois", "OH": "Ohio", "IN": "Indiana", "MN": "Minnesota",
    "NY": "New York", "MA": "Massachusetts", "PA": "Pennsylvania", "CO": "Colorado",
    "AZ": "Arizona", "WA": "Washington", "CA": "California", "OR": "Oregon",
}
FIRST_NAMES = np.array("""
James Mary Robert Patricia John Jennifer Michael Linda David Elizabeth William Barbara
Richard Susan Joseph Jessica Thomas Sarah Christopher Karen Daniel Lisa Matthew Nancy
Anthony Betty Mark Sandra Donald Ashley Steven Kimberly Andrew Emily Joshua Donna Kevin
Michelle Brian Carol George Amanda Timothy Melissa Ronald Deborah Jason Stephanie Ryan
Rebecca Jacob Laura Gary Sharon Nicholas Cynthia Eric Kathleen Jonathan Amy Luis Maria
Carlos Sofia Diego Valentina Wei Mei Arjun Priya Omar Fatima Kenji Yuki Andre Camila
""".split())
LAST_NAMES = np.array("""
Smith Johnson Williams Brown Jones Garcia Miller Davis Rodriguez Martinez Hernandez Lopez
Gonzalez Wilson Anderson Thomas Taylor Moore Jackson Martin Lee Perez Thompson White
Harris Sanchez Clark Ramirez Lewis Robinson Walker Young Allen King Wright Scott Torres
Nguyen Hill Flores Green Adams Nelson Baker Hall Rivera Campbell Mitchell Carter Roberts
Gomez Phillips Evans Turner Diaz Parker Cruz Edwards Collins Reyes Stewart Morris Patel
Chen Kim Singh Okafor Tanaka Rossi Muller Novak
""".split())
STREET_NAMES = np.array("""
Oak Maple Cedar Pine Elm Lake Hill Sunset Park Main River Bay Harbor Palm Willow
Highland Forest Meadow Spring Ridge Valley Washington Lincoln Jefferson Madison Franklin
""".split())
STREET_TYPES = np.array(["St", "Ave", "Blvd", "Rd", "Dr", "Ln", "Ct", "Way"])
EMAIL_DOMAINS = np.array(["gmail.com", "yahoo.com", "outlook.com", "icloud.com", "hotmail.com", "aol.com"])
LOYALTY_TIERS = ["Bronze", "Silver", "Gold", "Platinum"]

CATALOG = {
    "Electronics": {"weight": 0.16, "price": (12, 350), "return_rate": 0.09, "items": [
        ("Audio", "Wireless Earbuds"), ("Audio", "Bluetooth Speaker"), ("Audio", "Over-Ear Headphones"),
        ("Power", "USB-C Charger"), ("Power", "Portable Power Bank"), ("Smart Home", "Smart Plug"),
        ("Smart Home", "Video Doorbell"), ("Accessories", "Phone Case"), ("Accessories", "Laptop Sleeve")]},
    "Home & Kitchen": {"weight": 0.16, "price": (8, 220), "return_rate": 0.06, "items": [
        ("Cookware", "Nonstick Skillet"), ("Cookware", "Dutch Oven"), ("Small Appliances", "Air Fryer"),
        ("Small Appliances", "Coffee Maker"), ("Storage", "Glass Food Containers"),
        ("Bedding", "Cotton Sheet Set"), ("Bedding", "Throw Blanket"), ("Decor", "Ceramic Vase"),
        ("Decor", "Scented Candle")]},
    "Apparel": {"weight": 0.15, "price": (10, 160), "return_rate": 0.15, "items": [
        ("Tops", "Crew Neck Tee"), ("Tops", "Flannel Shirt"), ("Bottoms", "Slim Jeans"),
        ("Bottoms", "Jogger Pants"), ("Outerwear", "Rain Jacket"), ("Outerwear", "Puffer Vest"),
        ("Activewear", "Running Shorts"), ("Footwear", "Trail Sneakers"), ("Footwear", "Canvas Slip-Ons")]},
    "Beauty": {"weight": 0.11, "price": (5, 90), "return_rate": 0.04, "items": [
        ("Skincare", "Hydrating Serum"), ("Skincare", "Daily Moisturizer"), ("Skincare", "Mineral Sunscreen"),
        ("Haircare", "Repair Shampoo"), ("Haircare", "Hair Oil"), ("Makeup", "Tinted Lip Balm"),
        ("Fragrance", "Eau de Parfum"), ("Bath & Body", "Body Wash")]},
    "Sports & Outdoors": {"weight": 0.10, "price": (10, 300), "return_rate": 0.07, "items": [
        ("Fitness", "Yoga Mat"), ("Fitness", "Adjustable Dumbbells"), ("Camping", "Two-Person Tent"),
        ("Camping", "Insulated Water Bottle"), ("Cycling", "Bike Helmet"), ("Water Sports", "Snorkel Set"),
        ("Team Sports", "Soccer Ball")]},
    "Toys & Games": {"weight": 0.09, "price": (6, 120), "return_rate": 0.05, "items": [
        ("Board Games", "Strategy Board Game"), ("Puzzles", "1000-Piece Puzzle"),
        ("Building Sets", "Building Block Set"), ("Outdoor Play", "Kite"),
        ("Collectibles", "Trading Card Binder")]},
    "Grocery": {"weight": 0.13, "price": (2, 35), "return_rate": 0.01, "items": [
        ("Snacks", "Sea Salt Almonds"), ("Snacks", "Protein Bar Box"), ("Beverages", "Sparkling Water 12-Pack"),
        ("Coffee & Tea", "Whole Bean Coffee"), ("Coffee & Tea", "Green Tea Sachets"),
        ("Pantry", "Extra Virgin Olive Oil"), ("Pantry", "Pasta Sauce")]},
    "Pet Supplies": {"weight": 0.10, "price": (4, 110), "return_rate": 0.04, "items": [
        ("Dog", "Grain-Free Dog Food"), ("Dog", "Chew Toy"), ("Cat", "Clumping Cat Litter"),
        ("Cat", "Cat Scratcher"), ("Aquarium", "Aquarium Filter"), ("Bird", "Bird Seed Mix")]},
}
CATEGORY_NAMES = list(CATALOG)
CATEGORY_P = np.array([CATALOG[c]["weight"] for c in CATEGORY_NAMES])
CATEGORY_P = CATEGORY_P / CATEGORY_P.sum()
BRANDS = np.array("""Brightline Cedar&Co Lumora Peakform Harborlane Vantix Solace Kindred Trailwise
Pixelnest Aurelia Bluefin Ember&Oak Novacore WillowLane Ironpeak Sunhaven Quillsby Mapleton Zephyra""".split())
BRANDS = np.array([b.replace("&", " & ").replace("WillowLane", "Willow Lane") for b in BRANDS])
ADJECTIVES = np.array(["Classic", "Pro", "Essential", "Premium", "Everyday", "Ultra", "Eco", "Signature",
                       "Compact", "Deluxe", "Lite", "Max"])
COLORS = np.array(["Black", "White", "Navy", "Gray", "Red", "Green", "Blue", "Beige", "Pink", "Multi"])

CHANNELS = np.array(["In-Store", "Online", "Mobile App"])
PAYMENTS = {
    "In-Store": (["Credit Card", "Debit Card", "Cash", "Apple Pay", "Gift Card"], [0.50, 0.22, 0.15, 0.10, 0.03]),
    "Online": (["Credit Card", "Debit Card", "PayPal", "Apple Pay", "Gift Card"], [0.50, 0.15, 0.20, 0.10, 0.05]),
    "Mobile App": (["Credit Card", "Apple Pay", "PayPal", "Debit Card", "Gift Card"], [0.35, 0.35, 0.15, 0.10, 0.05]),
}
PROMOS = [("SAVE10", 0.10), ("WELCOME15", 0.15), ("HOLIDAY20", 0.20), ("FLASH25", 0.25), ("LOYALTY5", 0.05)]
PROMO_P = np.array([0.35, 0.15, 0.20, 0.10, 0.20])

RETURN_REASONS = np.array(["Defective", "Damaged in Transit", "Wrong Size", "Not as Described",
                           "Changed Mind", "Arrived Late", "Wrong Item Sent"])
RETURN_P_DEFAULT = np.array([0.20, 0.12, 0.08, 0.15, 0.30, 0.07, 0.08])
RETURN_P_APPAREL = np.array([0.08, 0.05, 0.45, 0.12, 0.22, 0.04, 0.04])

WEEKDAY_FACTOR = [0.90, 0.85, 0.90, 0.95, 1.10, 1.30, 1.15]
MONTH_FACTOR = {1: .85, 2: .88, 3: .95, 4: .97, 5: 1.0, 6: 1.0, 7: 1.05, 8: 1.12, 9: .95, 10: 1.0, 11: 1.2, 12: 1.45}
SPECIAL_DAY_FACTOR = {"Black Friday": 3.2, "Cyber Monday": 2.4, "Thanksgiving": 0.6, "Christmas Day": 0.25,
                      "Christmas Eve": 1.3, "New Year's Day": 0.7, "Independence Day": 0.9, "Memorial Day": 1.3,
                      "Labor Day": 1.25, "Presidents' Day": 1.15}
ONLINE_HOUR_P = np.array([2, 1, 1, 1, 1, 1, 2, 3, 4, 5, 5, 6, 7, 6, 5, 5, 5, 6, 7, 8, 9, 8, 6, 4], dtype=float)
ONLINE_HOUR_P /= ONLINE_HOUR_P.sum()
STORE_HOUR_P = np.array([2, 4, 6, 7, 8, 8, 8, 9, 9, 8, 7, 5], dtype=float)  # 09:00-20:59
STORE_HOUR_P /= STORE_HOUR_P.sum()

_lags = np.arange(31, dtype=float)
RETURN_LAG_W = np.where(_lags >= 2, _lags ** 1.5 * np.exp(-_lags / 5.0), 0.0)
RETURN_LAG_W /= RETURN_LAG_W.sum()

N_STORES = len(CITIES) + 1          # 24 physical + 1 online fulfilment "store"
WEB_IDX = N_STORES - 1
PRODUCT_CAPACITY = 2_000            # inventory array width; grows if needed


# ---------------------------------------------------------------------------
# Calendar helpers
# ---------------------------------------------------------------------------
def _nth_weekday(year: int, month: int, weekday: int, n: int) -> date:
    d = date(year, month, 1)
    return d + timedelta(days=(weekday - d.weekday()) % 7 + 7 * (n - 1))


def _last_weekday(year: int, month: int, weekday: int) -> date:
    d = (date(year, month + 1, 1) if month < 12 else date(year + 1, 1, 1)) - timedelta(days=1)
    return d - timedelta(days=(d.weekday() - weekday) % 7)


@lru_cache(maxsize=None)
def retail_calendar(year: int) -> dict:
    thanksgiving = _nth_weekday(year, 11, 3, 4)
    return {
        date(year, 1, 1): "New Year's Day", _nth_weekday(year, 1, 0, 3): "MLK Day",
        _nth_weekday(year, 2, 0, 3): "Presidents' Day", _last_weekday(year, 5, 0): "Memorial Day",
        date(year, 6, 19): "Juneteenth", date(year, 7, 4): "Independence Day",
        _nth_weekday(year, 9, 0, 1): "Labor Day", date(year, 11, 11): "Veterans Day",
        thanksgiving: "Thanksgiving", thanksgiving + timedelta(days=1): "Black Friday",
        thanksgiving + timedelta(days=4): "Cyber Monday", date(year, 12, 24): "Christmas Eve",
        date(year, 12, 25): "Christmas Day",
    }


def demand_factor(d: date, start: date) -> tuple[float, str | None]:
    f = WEEKDAY_FACTOR[d.weekday()] * MONTH_FACTOR[d.month]
    f *= 1.15 ** ((d - start).days / 365.0)          # 15% year-over-year growth
    name = retail_calendar(d.year).get(d)
    if name:
        f *= SPECIAL_DAY_FACTOR.get(name, 1.0)
    if d.month == 12 and 10 <= d.day <= 23:
        f *= 1.25
    return f, name


def channel_shares(d: date, start: date, holiday: str | None) -> np.ndarray:
    shift = min(0.10, 0.02 * (d - start).days / 365.0)   # app grows, stores shrink slowly
    p = np.array([0.55 - shift, 0.33, 0.12 + shift])
    if holiday == "Christmas Day":
        p = np.array([0.0, 0.75, 0.25])
    elif holiday == "Thanksgiving":
        p = np.array([0.25, 0.55, 0.20])
    elif holiday == "Cyber Monday":
        p = np.array([0.25, 0.55, 0.20])
    elif holiday == "Black Friday":
        p = np.array([0.65, 0.25, 0.10])
    return p / p.sum()


def day_rng(seed: int, d: date) -> np.random.Generator:
    return np.random.default_rng([seed, d.toordinal()])


def _ts(d: date, hours: np.ndarray, rng: np.random.Generator) -> list[str]:
    mins = rng.integers(0, 60, len(hours))
    secs = rng.integers(0, 60, len(hours))
    ds = d.isoformat()
    return [f"{ds} {h:02d}:{m:02d}:{s:02d}" for h, m, s in zip(hours, mins, secs)]


# ---------------------------------------------------------------------------
# Master-data builders
# ---------------------------------------------------------------------------
def _addresses(n: int, rng: np.random.Generator) -> dict:
    city_idx = rng.integers(0, len(CITIES), n)
    nums = rng.integers(100, 9999, n)
    streets = rng.choice(STREET_NAMES, n)
    types = rng.choice(STREET_TYPES, n)
    zips = rng.integers(0, 100, n)
    return {
        "_city_idx": city_idx,
        "address_line1": [f"{a} {b} {c}" for a, b, c in zip(nums, streets, types)],
        "city": [CITIES[i][0] for i in city_idx],
        "state": [CITIES[i][1] for i in city_idx],
        "zip_code": [f"{CITIES[i][2]}{z:02d}" for i, z in zip(city_idx, zips)],
    }


def _emails(first, last, rng) -> list[str]:
    nums = rng.integers(1, 1000, len(first))
    doms = rng.choice(EMAIL_DOMAINS, len(first))
    return [f"{f.lower()}.{l.lower()}{n}@{dm}" for f, l, n, dm in zip(first, last, nums, doms)]


def make_stores(rng: np.random.Generator, start: date) -> pd.DataFrame:
    rows = []
    for i, (city, st, zp, region) in enumerate(CITIES):
        store_type = "Flagship" if i in (0, 11, 15, 21) else ("Outlet" if i in (4, 13, 19) else "Standard")
        open_d = date(2010, 1, 1) + timedelta(days=int(rng.integers(0, 3650)))
        if i == 22:
            open_d = start + timedelta(days=210)       # opens during history
        if i == 23:
            open_d = start + timedelta(days=480)
        sqft = {"Flagship": 60_000, "Standard": 35_000, "Outlet": 25_000}[store_type]
        rows.append({
            "store_id": f"STR{i + 1:03d}",
            "store_name": f"{RETAILER.split()[0]} {city}" + (" Outlet" if store_type == "Outlet" else ""),
            "store_type": store_type,
            "address_line1": f"{int(rng.integers(100, 9999))} {rng.choice(STREET_NAMES)} {rng.choice(STREET_TYPES)}",
            "city": city, "state": st, "zip_code": f"{zp}{int(rng.integers(0, 100)):02d}",
            "region": region, "open_date": open_d.isoformat(),
            "square_feet": int(sqft * rng.uniform(0.85, 1.15)),
            "manager_name": f"{rng.choice(FIRST_NAMES)} {rng.choice(LAST_NAMES)}",
            "_open_ord": open_d.toordinal(),
        })
    rows.append({
        "store_id": "WEB001", "store_name": f"{RETAILER.split()[0]} Online", "store_type": "Online",
        "address_line1": "1 Harbor Logistics Pkwy", "city": "Tampa", "state": "FL", "zip_code": "33619",
        "region": "National", "open_date": "2015-03-01", "square_feet": None,
        "manager_name": f"{rng.choice(FIRST_NAMES)} {rng.choice(LAST_NAMES)}",
        "_open_ord": date(2015, 3, 1).toordinal(),
    })
    df = pd.DataFrame(rows)
    # Persistent source-system quirks the Silver layer has to standardize
    df.loc[1, "state"] = "Florida"
    df.loc[9, "state"] = "tx"
    df.loc[14, "region"] = "Mid-West"
    df.loc[18, "region"] = "west"
    return df


def make_customers(n: int, seq_start: int, signup_ords: np.ndarray, stores: pd.DataFrame,
                   rng: np.random.Generator) -> pd.DataFrame:
    if n == 0:
        return pd.DataFrame()
    first = rng.choice(FIRST_NAMES, n)
    last = rng.choice(LAST_NAMES, n)
    addr = _addresses(n, rng)
    area = rng.integers(201, 990, n)
    rest = rng.integers(0, 10_000_000, n)
    phone = [f"{a:03d}{r:07d}" for a, r in zip(area, rest)]
    tier = rng.choice(LOYALTY_TIERS, n, p=[0.55, 0.28, 0.13, 0.04])
    birth = [date.fromordinal(int(o) - int(rng.integers(18 * 365, 80 * 365))).isoformat() for o in signup_ords]
    open_ord = stores["_open_ord"].to_numpy()
    pref = addr["_city_idx"].copy()
    not_open = open_ord[pref] > signup_ords
    if not_open.any():
        candidates = np.flatnonzero(open_ord[:WEB_IDX] <= signup_ords.min())
        pref[not_open] = rng.choice(candidates, not_open.sum())
    hours = rng.integers(0, 24, n)
    df = pd.DataFrame({
        "customer_id": [f"CUS{s:07d}" for s in range(seq_start, seq_start + n)],
        "first_name": first, "last_name": last,
        "email": _emails(first, last, rng), "phone": phone,
        "address_line1": addr["address_line1"], "city": addr["city"], "state": addr["state"],
        "zip_code": addr["zip_code"], "loyalty_tier": tier,
        "marketing_opt_in": rng.random(n) < 0.6,
        "birth_date": birth,
        "signup_date": [date.fromordinal(int(o)).isoformat() for o in signup_ords],
        "preferred_store_id": stores["store_id"].to_numpy()[pref],
        "updated_at": [f"{date.fromordinal(int(o)).isoformat()} {h:02d}:{int(m):02d}:00"
                       for o, h, m in zip(signup_ords, hours, rng.integers(0, 60, n))],
        "_signup_ord": signup_ords.astype(int),
        "_weight": rng.lognormal(0.0, 1.0, n),          # how often this customer shops
        "_pref_idx": pref.astype(int),
    })
    return df


def make_products(n: int, seq_start: int, launch_ords: np.ndarray, rng: np.random.Generator) -> pd.DataFrame:
    if n == 0:
        return pd.DataFrame()
    cats = rng.choice(CATEGORY_NAMES, n, p=CATEGORY_P)
    rows = []
    for k, cat in enumerate(cats):
        spec = CATALOG[cat]
        sub, noun = spec["items"][int(rng.integers(0, len(spec["items"])))]
        lo, hi = spec["price"]
        price = float(np.exp(rng.uniform(np.log(lo), np.log(hi))))
        price = max(1.99, np.floor(price) + 0.99)
        seq = seq_start + k
        brand = rng.choice(BRANDS)
        rows.append({
            "product_id": f"PRD{seq:05d}",
            "sku": f"{cat[:3].upper()}-{seq:05d}",
            "product_name": f"{brand} {rng.choice(ADJECTIVES)} {noun}",
            "category": cat, "subcategory": sub, "brand": brand,
            "list_price": round(price, 2),
            "unit_cost": round(price * rng.uniform(0.35, 0.65), 2),
            "color": rng.choice(COLORS),
            "weight_kg": round(float(rng.uniform(0.05, 12.0)), 2),
            "is_active": True,
            "launch_date": date.fromordinal(int(launch_ords[k])).isoformat(),
            "updated_at": f"{date.fromordinal(int(launch_ords[k])).isoformat()} 06:00:00",
            "_launch_ord": int(launch_ords[k]),
            "_pop": float(rng.lognormal(0.0, 1.0)),
            "_return_rate": spec["return_rate"],
        })
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Simulator state
# ---------------------------------------------------------------------------
LINE_STATE_COLS = ["order_line_id", "order_id", "prod_idx", "store_idx", "channel", "quantity",
                   "line_amount", "order_ord", "status"]


@dataclass
class State:
    version: str
    config: Config
    start_date: date
    last_date: date | None
    customers: pd.DataFrame
    products: pd.DataFrame
    stores: pd.DataFrame
    recent_lines: pd.DataFrame
    returned_line_ids: set
    pending_orders: pd.DataFrame
    pending_lines: pd.DataFrame
    on_hand: np.ndarray
    on_order: np.ndarray
    eta: np.ndarray
    reorder_point: np.ndarray
    order_qty: np.ndarray
    next_customer_seq: int
    next_product_seq: int
    next_order_seq: int
    next_return_seq: int


def _stock_up(st: State, store_idx: np.ndarray, prod_idx: np.ndarray, rng: np.random.Generator) -> None:
    """Give new store x product cells an opening stock level and a demand-based reorder policy.

    Expected daily demand per cell = average daily units x product popularity share x store share.
    Reorder point covers ~5 days of demand (lead time is 3-10 days), so fast sellers stock out
    now and then - which gives the Gold layer real stock-out analysis to do.
    """
    if len(store_idx) == 0 or len(prod_idx) == 0:
        return
    cfg = st.config
    units_per_day = cfg.base_orders_per_day * cfg.scale * 2.4 * 1.35
    pop = st.products["_pop"].to_numpy()
    pop_share = pop / pop.sum()
    sqft = st.stores["square_feet"].to_numpy(dtype=float)
    store_share = np.where(np.isnan(sqft), 0.0, sqft)
    store_share = 0.55 * store_share / store_share.sum()
    store_share[WEB_IDX] = 0.45
    s, p = np.meshgrid(store_idx, prod_idx, indexing="ij")
    demand = units_per_day * pop_share[p] * store_share[s]
    st.reorder_point[s, p] = np.ceil(demand * 5).astype(np.int64) + 1
    st.order_qty[s, p] = np.ceil(demand * 21).astype(np.int64) + 3
    st.on_hand[s, p] = np.ceil(st.order_qty[s, p] * rng.uniform(0.5, 1.5, s.shape)).astype(np.int64)


def _ensure_capacity(st: State, n_products: int) -> None:
    cap = st.on_hand.shape[1]
    if n_products <= cap:
        return
    extra = max(n_products - cap, 500)
    for name in ("on_hand", "on_order", "eta", "reorder_point", "order_qty"):
        arr = getattr(st, name)
        setattr(st, name, np.pad(arr, ((0, 0), (0, extra))))


def build_initial_state(cfg: Config, start: date) -> State:
    rng = np.random.default_rng([cfg.seed, 0])
    start_ord = start.toordinal()
    stores = make_stores(rng, start)
    n_prod = cfg.initial_products
    products = make_products(n_prod, 1, rng.integers(start_ord - 1500, start_ord, n_prod), rng)
    n_cust = int(cfg.initial_customers * cfg.scale)
    signups = np.sort(rng.integers(start_ord - 6 * 365, start_ord, n_cust))
    customers = make_customers(n_cust, 1, signups, stores, rng)
    z = lambda: np.zeros((N_STORES, PRODUCT_CAPACITY), dtype=np.int64)  # noqa: E731
    st = State(
        version=GENERATOR_VERSION, config=cfg, start_date=start, last_date=None,
        customers=customers, products=products, stores=stores,
        recent_lines=pd.DataFrame(columns=LINE_STATE_COLS), returned_line_ids=set(),
        pending_orders=pd.DataFrame(), pending_lines=pd.DataFrame(),
        on_hand=z(), on_order=z(), eta=z(), reorder_point=z(), order_qty=z(),
        next_customer_seq=n_cust + 1, next_product_seq=n_prod + 1, next_order_seq=1, next_return_seq=1,
    )
    open_stores = np.flatnonzero(stores["_open_ord"].to_numpy() < start_ord)
    _stock_up(st, open_stores, np.arange(n_prod), rng)
    return st


# ---------------------------------------------------------------------------
# Output "dirtiers" - applied to file output only; the simulation stays clean
# ---------------------------------------------------------------------------
def _pick(rng, n, rate):
    return rng.random(n) < rate


def dirty_customers(df: pd.DataFrame, cfg: Config, rng: np.random.Generator) -> pd.DataFrame:
    out = df[CUSTOMER_COLS].copy().reset_index(drop=True)
    n = len(out)
    if n == 0:
        return out
    r = cfg.customer_dirty_rate
    fmt = rng.integers(0, 4, n)
    ph = out["phone"].to_numpy()
    out["phone"] = [p if f == 0 else f"({p[:3]}) {p[3:6]}-{p[6:]}" if f == 1 else
                    f"{p[:3]}-{p[3:6]}-{p[6:]}" if f == 2 else f"+1 {p[:3]}.{p[3:6]}.{p[6:]}"
                    for p, f in zip(ph, fmt)]
    m = _pick(rng, n, r)
    out.loc[m, "first_name"] = out.loc[m, "first_name"].str.upper()
    m = _pick(rng, n, r)
    out.loc[m, "last_name"] = out.loc[m, "last_name"].str.lower()
    m = _pick(rng, n, r * 0.3)
    out.loc[m, "first_name"] = "  " + out.loc[m, "first_name"] + " "
    m = _pick(rng, n, r * 0.7)
    out.loc[m, "email"] = out.loc[m, "email"].str.upper()
    m = _pick(rng, n, r * 0.7)
    out.loc[m, "email"] = None
    m = _pick(rng, n, r * 1.5)
    out.loc[m, "state"] = out.loc[m, "state"].map(STATE_NAMES)
    m = _pick(rng, n, r * 0.5)
    out.loc[m, "state"] = out.loc[m, "state"].str.lower()
    m = _pick(rng, n, r * 0.3)
    out.loc[m, "loyalty_tier"] = out.loc[m, "loyalty_tier"].str.upper()
    return out


CUSTOMER_COLS = ["customer_id", "first_name", "last_name", "email", "phone", "address_line1", "city",
                 "state", "zip_code", "loyalty_tier", "marketing_opt_in", "birth_date", "signup_date",
                 "preferred_store_id", "updated_at"]
STORE_COLS = ["store_id", "store_name", "store_type", "address_line1", "city", "state", "zip_code",
              "region", "open_date", "square_feet", "manager_name"]
ORDER_COLS = ["order_id", "order_ts", "customer_id", "store_id", "channel", "payment_method",
              "promo_code", "order_status", "shipping_amount", "currency", "source_system"]
LINE_COLS = ["order_line_id", "order_id", "line_number", "product_id", "quantity", "unit_price",
             "discount_amount", "line_amount"]
RETURN_COLS = ["return_id", "order_line_id", "order_id", "product_id", "return_ts", "quantity_returned",
               "refund_amount", "return_reason", "return_channel", "return_store_id"]
INVENTORY_COLS = ["snapshot_date", "store_id", "product_id", "quantity_on_hand", "quantity_on_order",
                  "reorder_point"]


def products_payload(df: pd.DataFrame, d: date, extract_type: str, cfg: Config,
                     rng: np.random.Generator) -> dict:
    cats = df["category"].to_numpy().astype(object)
    m = _pick(rng, len(df), cfg.product_category_dirty_rate)
    variant = rng.integers(0, 3, len(df))
    for i in np.flatnonzero(m):
        c = cats[i]
        cats[i] = c.lower() if variant[i] == 0 else (c.upper() if variant[i] == 1 else c + " ")
    records = []
    for row, cat in zip(df.itertuples(index=False), cats):
        records.append({
            "product_id": row.product_id, "sku": row.sku, "name": row.product_name,
            "category": cat, "subcategory": row.subcategory, "brand": row.brand,
            "pricing": {"list_price": row.list_price, "unit_cost": row.unit_cost, "currency": "USD"},
            "attributes": {"color": row.color, "weight_kg": row.weight_kg},
            "is_active": bool(row.is_active), "launch_date": row.launch_date, "updated_at": row.updated_at,
        })
    return {"source": "catalog-api", "extract_type": extract_type, "extract_date": d.isoformat(),
            "record_count": len(records), "products": records}


# ---------------------------------------------------------------------------
# One simulated business day
# ---------------------------------------------------------------------------
def simulate_day(st: State, d: date) -> dict:
    cfg = st.config
    rng = day_rng(cfg.seed, d)
    d_ord = d.toordinal()
    first_day = st.last_date is None
    out: dict = {}

    # ---- 1. product lifecycle -------------------------------------------
    products = st.products
    n_launch = int(rng.poisson(0.04))
    changed_prod = set()
    if n_launch:
        new = make_products(n_launch, st.next_product_seq, np.full(n_launch, d_ord), rng)
        st.next_product_seq += n_launch
        products = pd.concat([products, new], ignore_index=True)
        st.products = products
        _ensure_capacity(st, len(products))
        open_now = np.flatnonzero(st.stores["_open_ord"].to_numpy() <= d_ord)
        _stock_up(st, open_now, np.arange(len(products) - n_launch, len(products)), rng)
        changed_prod.update(range(len(products) - n_launch, len(products)))
    launched = products["_launch_ord"].to_numpy() <= d_ord
    active = launched & products["is_active"].to_numpy()
    active_idx = np.flatnonzero(active)
    n_price = min(int(rng.poisson(2.0)), len(active_idx))
    if n_price:
        idx = rng.choice(active_idx, n_price, replace=False)
        new_price = products.loc[idx, "list_price"].to_numpy() * rng.uniform(0.85, 1.15, n_price)
        products.loc[idx, "list_price"] = np.maximum(1.99, np.floor(new_price) + 0.99).round(2)
        products.loc[idx, "updated_at"] = f"{d.isoformat()} 06:00:00"
        changed_prod.update(idx.tolist())
    n_disc = min(int(rng.poisson(0.03)), len(active_idx))
    if n_disc:
        idx = rng.choice(active_idx, n_disc, replace=False)
        products.loc[idx, "is_active"] = False
        products.loc[idx, "updated_at"] = f"{d.isoformat()} 06:00:00"
        changed_prod.update(idx.tolist())
        active = launched & products["is_active"].to_numpy()
        active_idx = np.flatnonzero(active)
    st.products = products
    if first_day:
        out["products"] = products_payload(products[launched], d, "full", cfg, rng)
    elif changed_prod:
        out["products"] = products_payload(products.loc[sorted(changed_prod)], d, "incremental", cfg, rng)

    # ---- 2. stores ------------------------------------------------------
    stores = st.stores
    open_ord = stores["_open_ord"].to_numpy()
    opening = np.flatnonzero(open_ord == d_ord)
    if len(opening):
        _stock_up(st, opening, np.flatnonzero(launched), rng)
    if first_day or len(opening):
        out["stores"] = stores.loc[open_ord <= d_ord, STORE_COLS].copy()

    # ---- 3. customers ---------------------------------------------------
    cust = st.customers
    elig_prev = np.flatnonzero(cust["_signup_ord"].to_numpy() < d_ord)
    k = min(int(rng.poisson(cfg.customer_change_rate * len(elig_prev))), len(elig_prev))
    changed_cust = np.array([], dtype=int)
    if k:
        changed_cust = rng.choice(elig_prev, k, replace=False)
        typ = rng.choice(["move", "tier", "email"], k, p=[0.5, 0.35, 0.15])
        ts = _ts(d, rng.integers(0, 24, k), rng)
        mv = changed_cust[typ == "move"]
        if len(mv):
            a = _addresses(len(mv), rng)
            for col in ("address_line1", "city", "state", "zip_code"):
                cust.loc[mv, col] = a[col]
        tr = changed_cust[typ == "tier"]
        if len(tr):
            cur = cust.loc[tr, "loyalty_tier"].map({t: i for i, t in enumerate(LOYALTY_TIERS)}).to_numpy()
            step = np.where(rng.random(len(tr)) < 0.75, 1, -1)
            cust.loc[tr, "loyalty_tier"] = [LOYALTY_TIERS[i] for i in np.clip(cur + step, 0, 3)]
        em = changed_cust[typ == "email"]
        if len(em):
            cust.loc[em, "email"] = _emails(cust.loc[em, "first_name"].to_numpy(),
                                            cust.loc[em, "last_name"].to_numpy(), rng)
            cust.loc[em, "marketing_opt_in"] = rng.random(len(em)) < 0.5
        cust.loc[changed_cust, "updated_at"] = ts
    n_new = int(rng.poisson(cfg.new_customers_per_day * cfg.scale))
    if n_new:
        new = make_customers(n_new, st.next_customer_seq, np.full(n_new, d_ord), stores, rng)
        st.next_customer_seq += n_new
        cust = pd.concat([cust, new], ignore_index=True)
    st.customers = cust
    if first_day:
        cust_out = cust
    else:
        idx = np.union1d(changed_cust, np.arange(len(cust) - n_new, len(cust)))
        cust_out = cust.loc[idx]
    out["customers"] = dirty_customers(cust_out, cfg, rng)

    # ---- 4. orders ------------------------------------------------------
    factor, holiday = demand_factor(d, st.start_date)
    n = int(rng.poisson(cfg.base_orders_per_day * cfg.scale * factor))
    channel = rng.choice(CHANNELS, n, p=channel_shares(d, st.start_date, holiday))
    instore = channel == "In-Store"
    is_guest = rng.random(n) < np.where(instore, 0.25, 0.08)
    elig = np.flatnonzero(cust["_signup_ord"].to_numpy() <= d_ord)
    w = cust["_weight"].to_numpy()[elig]
    cust_idx = np.full(n, -1)
    cust_idx[~is_guest] = rng.choice(elig, int((~is_guest).sum()), p=w / w.sum())
    cust_ids = cust["customer_id"].to_numpy()
    customer_id = np.where(cust_idx >= 0, cust_ids[np.maximum(cust_idx, 0)], None)

    open_phys = np.flatnonzero((open_ord <= d_ord) & (np.arange(N_STORES) != WEB_IDX))
    sqft = stores["square_feet"].to_numpy(dtype=float)[open_phys]
    rand_store = rng.choice(open_phys, n, p=sqft / sqft.sum())
    pref = np.where(cust_idx >= 0, cust["_pref_idx"].to_numpy()[np.maximum(cust_idx, 0)], -1)
    use_pref = (pref >= 0) & (rng.random(n) < 0.6) & np.isin(pref, open_phys)
    store_idx = np.where(instore, np.where(use_pref, pref, rand_store), WEB_IDX)

    hours = np.where(instore, 9 + rng.choice(12, n, p=STORE_HOUR_P), rng.choice(24, n, p=ONLINE_HOUR_P))
    order_ts = _ts(d, hours, rng)
    order_ids = np.array([f"ORD{s:09d}" for s in range(st.next_order_seq, st.next_order_seq + n)])
    st.next_order_seq += n

    promo_rate = 0.25 if holiday in ("Black Friday", "Cyber Monday") else (0.18 if d.month == 12 else 0.12)
    has_promo = rng.random(n) < promo_rate
    promo_i = rng.choice(len(PROMOS), n, p=PROMO_P)
    promo_code = np.where(has_promo, np.array([p[0] for p in PROMOS])[promo_i], None)
    promo_pct = np.where(has_promo, np.array([p[1] for p in PROMOS])[promo_i], 0.0)
    status = rng.choice(["Completed", "Cancelled", "Pending"], n, p=[0.965, 0.03, 0.005])
    payment = np.empty(n, dtype=object)
    for ch, (opts, p) in PAYMENTS.items():
        m = channel == ch
        payment[m] = rng.choice(opts, int(m.sum()), p=p)

    nl = np.clip(1 + rng.poisson(1.4, n), 1, 8)
    tot = int(nl.sum())
    line_order = np.repeat(np.arange(n), nl)
    line_no = np.arange(tot) - np.repeat(np.cumsum(nl) - nl, nl) + 1
    pop = products["_pop"].to_numpy()[active_idx]
    prod_idx = rng.choice(active_idx, tot, p=pop / pop.sum())
    qty = np.clip(1 + rng.poisson(0.35, tot), 1, 6)
    price = products["list_price"].to_numpy(dtype=float)[prod_idx]
    disc = np.round(qty * price * promo_pct[line_order], 2)
    amount = np.round(qty * price - disc, 2)
    subtotal = np.bincount(line_order, weights=amount, minlength=n)
    shipping = np.where(instore, 0.0, np.where(subtotal >= 50, 0.0, 6.99))

    store_ids = stores["store_id"].to_numpy()
    prod_ids = products["product_id"].to_numpy()
    headers = pd.DataFrame({
        "order_id": order_ids, "order_ts": order_ts, "customer_id": customer_id,
        "store_id": store_ids[store_idx], "channel": channel, "payment_method": payment,
        "promo_code": promo_code, "order_status": status, "shipping_amount": shipping,
        "currency": "USD", "source_system": np.where(instore, "POS", "ECOM"),
    })
    line_ids = [f"{o}-{k}" for o, k in zip(order_ids[line_order], line_no)]
    lines = pd.DataFrame({
        "order_line_id": line_ids, "order_id": order_ids[line_order], "line_number": line_no,
        "product_id": prod_ids[prod_idx], "quantity": qty, "unit_price": price,
        "discount_amount": disc, "line_amount": amount,
    })

    # ---- 5. inventory (truth uses clean, on-time data) --------------------
    arrived = st.eta == d_ord
    st.on_hand[arrived] += st.on_order[arrived]
    st.on_order[arrived] = 0
    st.eta[arrived] = 0
    sold_mask = status[line_order] != "Cancelled"
    np.add.at(st.on_hand, (store_idx[line_order][sold_mask], prod_idx[sold_mask]), -qty[sold_mask])

    # ---- 6. returns -----------------------------------------------------
    rl = st.recent_lines
    ret_rows = pd.DataFrame(columns=RETURN_COLS)
    if len(rl):
        lag = d_ord - rl["order_ord"].to_numpy(dtype=int)
        ok = (lag >= 2) & (lag <= 30) & (rl["status"].to_numpy() == "Completed")
        ok &= ~rl["order_line_id"].isin(st.returned_line_ids).to_numpy()
        rr = products["_return_rate"].to_numpy()[rl["prod_idx"].to_numpy(dtype=int)]
        p = rr * RETURN_LAG_W[np.clip(lag, 0, 30)]
        hit = np.flatnonzero(ok & (rng.random(len(rl)) < p))
        if len(hit):
            h = rl.iloc[hit]
            m = len(h)
            q = h["quantity"].to_numpy(dtype=int)
            qret = np.where(q > 1, rng.integers(1, q + 1), 1)
            refund = np.round(h["line_amount"].to_numpy(dtype=float) / q * qret, 2)
            pidx = h["prod_idx"].to_numpy(dtype=int)
            is_app = products["category"].to_numpy()[pidx] == "Apparel"
            reasons = np.where(is_app, rng.choice(RETURN_REASONS, m, p=RETURN_P_APPAREL),
                               rng.choice(RETURN_REASONS, m, p=RETURN_P_DEFAULT)).astype(object)
            v = np.flatnonzero(_pick(rng, m, cfg.return_reason_variant_rate))
            vk = rng.integers(0, 3, len(v))
            for i, kk in zip(v, vk):
                reasons[i] = reasons[i].lower() if kk == 0 else (reasons[i].upper() if kk == 1 else reasons[i] + " ")
            orig_instore = h["channel"].to_numpy() == "In-Store"
            in_store_ret = np.where(orig_instore, rng.random(m) < 0.85, rng.random(m) < 0.30)
            orig_store = h["store_idx"].to_numpy(dtype=int)
            ret_store = np.where(in_store_ret, np.where(orig_instore, orig_store, rng.choice(open_phys, m)), WEB_IDX)
            ret_ids = [f"RET{s:08d}" for s in range(st.next_return_seq, st.next_return_seq + m)]
            st.next_return_seq += m
            ret_rows = pd.DataFrame({
                "return_id": ret_ids, "order_line_id": h["order_line_id"].to_numpy(),
                "order_id": h["order_id"].to_numpy(), "product_id": prod_ids[pidx],
                "return_ts": _ts(d, rng.integers(8, 22, m), rng), "quantity_returned": qret,
                "refund_amount": refund, "return_reason": reasons,
                "return_channel": np.where(in_store_ret, "In-Store", "Mail"),
                "return_store_id": store_ids[ret_store],
            })
            st.returned_line_ids.update(h["order_line_id"].tolist())
            restock = rng.random(m) < 0.7
            np.add.at(st.on_hand, (ret_store[restock], pidx[restock]), qret[restock])
    n_orph = int(rng.poisson(max(len(ret_rows), 1) * cfg.orphan_return_rate))
    if n_orph:
        fake = pd.DataFrame({
            "return_id": [f"RET{s:08d}" for s in range(st.next_return_seq, st.next_return_seq + n_orph)],
            "order_line_id": [f"ORD9{int(x):08d}-1" for x in rng.integers(0, 10**8, n_orph)],
            "order_id": None, "product_id": rng.choice(prod_ids[active_idx], n_orph),
            "return_ts": _ts(d, rng.integers(8, 22, n_orph), rng), "quantity_returned": 1,
            "refund_amount": np.round(rng.uniform(5, 120, n_orph), 2), "return_reason": "Changed Mind",
            "return_channel": "Mail", "return_store_id": "WEB001",
        })
        st.next_return_seq += n_orph
        ret_rows = pd.concat([ret_rows, fake], ignore_index=True) if len(ret_rows) else fake
    out["returns"] = ret_rows

    # ---- 7. replenishment + snapshot -------------------------------------
    np.maximum(st.on_hand, 0, out=st.on_hand)             # stock-outs floor at zero
    cap = st.on_hand.shape[1]
    cell_ok = np.zeros((N_STORES, cap), dtype=bool)
    cell_ok[np.ix_(np.flatnonzero(open_ord <= d_ord), np.flatnonzero(launched))] = True
    act_cols = np.zeros(cap, dtype=bool)
    act_cols[active_idx] = True
    need = cell_ok & act_cols & (st.on_hand <= st.reorder_point) & (st.on_order == 0)
    st.on_order[need] = st.order_qty[need]
    st.eta[need] = d_ord + rng.integers(3, 11, int(need.sum()))

    hist_end = getattr(st, "_history_end", None)
    write_inv = hist_end is None or d_ord > hist_end.toordinal() - cfg.inventory_days
    if write_inv:
        si, pi = np.nonzero(cell_ok & (act_cols | (st.on_hand > 0)))
        inv = pd.DataFrame({
            "snapshot_date": d.isoformat(), "store_id": store_ids[si], "product_id": prod_ids[pi],
            "quantity_on_hand": st.on_hand[si, pi], "quantity_on_order": st.on_order[si, pi],
            "reorder_point": st.reorder_point[si, pi],
        })
        keep = ~_pick(rng, len(inv), cfg.inventory_missing_rate)
        skip_stores = store_ids[_pick(rng, N_STORES, cfg.inventory_store_skip_rate)]
        keep &= ~inv["store_id"].isin(skip_stores).to_numpy()
        inv = inv[keep].reset_index(drop=True)
        neg = _pick(rng, len(inv), cfg.inventory_negative_rate)
        inv.loc[neg, "quantity_on_hand"] = -rng.integers(1, 20, int(neg.sum()))
        out["inventory"] = inv

    # ---- 8. remember lines for future returns -----------------------------
    new_state_lines = pd.DataFrame({
        "order_line_id": line_ids, "order_id": order_ids[line_order], "prod_idx": prod_idx,
        "store_idx": store_idx[line_order], "channel": channel[line_order], "quantity": qty,
        "line_amount": amount, "order_ord": d_ord, "status": status[line_order],
    })
    rl = pd.concat([rl, new_state_lines], ignore_index=True) if len(rl) else new_state_lines
    rl = rl[rl["order_ord"].to_numpy(dtype=int) >= d_ord - 31].reset_index(drop=True)
    st.recent_lines = rl
    st.returned_line_ids &= set(rl["order_line_id"])

    # ---- 9. late-arriving orders -------------------------------------------
    late = _pick(rng, n, cfg.late_arrival_rate)
    late_days = rng.integers(1, 4, n)
    late_hdr = headers[late].assign(_target=d_ord + late_days[late])
    target_map = dict(zip(late_hdr["order_id"], late_hdr["_target"]))
    late_ln = lines[lines["order_id"].isin(target_map)].copy()
    late_ln["_target"] = late_ln["order_id"].map(target_map)
    headers = headers[~late]
    lines = lines[~lines["order_id"].isin(target_map)]
    po, pl = st.pending_orders, st.pending_lines
    if len(po):
        due_o, due_l = po[po["_target"] == d_ord], pl[pl["_target"] == d_ord]
        headers = pd.concat([headers, due_o.drop(columns="_target")], ignore_index=True)
        lines = pd.concat([lines, due_l.drop(columns="_target")], ignore_index=True)
        po, pl = po[po["_target"] != d_ord], pl[pl["_target"] != d_ord]
    st.pending_orders = pd.concat([po, late_hdr], ignore_index=True) if len(po) else late_hdr
    st.pending_lines = pd.concat([pl, late_ln], ignore_index=True) if len(pl) else late_ln

    out["orders"], out["order_lines"] = dirty_orders(headers.reset_index(drop=True),
                                                     lines.reset_index(drop=True), cfg, rng)
    st.last_date = d
    return out


def dirty_orders(h: pd.DataFrame, ln: pd.DataFrame, cfg: Config, rng: np.random.Generator):
    h = h.copy()
    ln = ln.copy()
    n, m = len(h), len(ln)
    # status casing
    s = _pick(rng, n, cfg.status_case_rate)
    h.loc[s, "order_status"] = h.loc[s, "order_status"].str.upper()
    # alternate + invalid date formats
    a = np.flatnonzero(_pick(rng, n, cfg.alt_date_format_rate))
    for i in a:
        dt = datetime.strptime(h.at[i, "order_ts"], "%Y-%m-%d %H:%M:%S")
        h.at[i, "order_ts"] = dt.strftime("%m/%d/%Y %H:%M")
    b = np.flatnonzero(_pick(rng, n, cfg.bad_date_rate))
    for i in b:
        h.at[i, "order_ts"] = h.at[i, "order_ts"][:4] + rng.choice(["-02-30", "-13-01", "-00-15"]) + h.at[i, "order_ts"][10:]
    # line-level problems
    q = _pick(rng, m, cfg.neg_qty_rate)
    ln.loc[q, "quantity"] = -ln.loc[q, "quantity"]
    o = _pick(rng, m, cfg.orphan_product_rate)
    ln.loc[o, "product_id"] = [f"PRD9{int(x):04d}" for x in rng.integers(0, 10_000, int(o.sum()))]
    mm = _pick(rng, m, cfg.amount_mismatch_rate)
    ln.loc[mm, "line_amount"] = (ln.loc[mm, "line_amount"] + rng.choice([0.01, 1.0, 10.0], int(mm.sum()))).round(2)
    # duplicates (exact re-sends)
    h = pd.concat([h, h[_pick(rng, n, cfg.dup_order_rate)]], ignore_index=True)
    ln = pd.concat([ln, ln[_pick(rng, m, cfg.dup_line_rate)]], ignore_index=True)
    return h[ORDER_COLS], ln[LINE_COLS]


# ---------------------------------------------------------------------------
# Writing
# ---------------------------------------------------------------------------
def _day_dir(root: Path, source: str, d: date) -> Path:
    p = root / "landing" / source / f"{d:%Y}" / f"{d:%m}" / f"{d:%d}"
    p.mkdir(parents=True, exist_ok=True)
    return p


def write_day(root: Path, d: date, outputs: dict) -> dict:
    files = []
    stamp = f"{d:%Y%m%d}"
    for source in ("orders", "order_lines", "customers", "products", "stores", "inventory", "returns"):
        if source not in outputs:
            continue
        data = outputs[source]
        folder = _day_dir(root, source, d)
        if source == "products":
            path = folder / f"products_{stamp}.json"
            path.write_text(json.dumps(data, indent=2), encoding="utf-8")
            rows = data["record_count"]
        else:
            path = folder / f"{source}_{stamp}.csv"
            data.to_csv(path, index=False)
            rows = len(data)
        files.append({"source": source, "path": path.relative_to(root / "landing").as_posix(),
                      "row_count": int(rows), "bytes": path.stat().st_size})
    manifest = {"business_date": d.isoformat(), "generated_at": datetime.now().isoformat(timespec="seconds"),
                "generator_version": GENERATOR_VERSION, "file_count": len(files), "files": files}
    mdir = _day_dir(root, "_manifests", d)
    (mdir / f"manifest_{stamp}.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return manifest


def _state_path(root: Path) -> Path:
    return root / "_state" / "generator_state.pkl"


def save_state(root: Path, st: State) -> None:
    p = _state_path(root)
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("wb") as f:
        pickle.dump(st, f)


def load_state(root: Path) -> State:
    p = _state_path(root)
    if not p.exists():
        sys.exit(f"No simulator state at {p}. Run the 'history' command first.")
    with p.open("rb") as f:
        return pickle.load(f)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def run_days(root: Path, st: State, days: list[date], quiet: bool = False) -> dict:
    totals: dict = {}
    t0 = _time.time()
    for k, d in enumerate(days, 1):
        man = write_day(root, d, simulate_day(st, d))
        for f in man["files"]:
            totals[f["source"]] = totals.get(f["source"], 0) + f["row_count"]
        if not quiet and (k % 30 == 0 or k == len(days)):
            print(f"  {d}  day {k}/{len(days)}  ({_time.time() - t0:,.0f}s)", flush=True)
    return totals


def _print_totals(totals: dict) -> None:
    width = max(len(s) for s in totals) if totals else 10
    for s, v in totals.items():
        print(f"  {s:<{width}}  {v:>12,} rows")


def cmd_history(args) -> None:
    root = Path(args.out)
    cfg = Config(seed=args.seed, scale=args.scale, inventory_days=args.inventory_days)
    start, end = date.fromisoformat(args.start), date.fromisoformat(args.end)
    if end < start:
        sys.exit("--end must be on or after --start")
    if _state_path(root).exists():
        if not args.overwrite:
            sys.exit(f"Output already exists in {root}. Use --overwrite to rebuild it.")
        for sub in ("landing", "_state"):
            shutil.rmtree(root / sub, ignore_errors=True)
    print(f"{RETAILER} simulator v{GENERATOR_VERSION}: history {start} -> {end} (scale {cfg.scale}, seed {cfg.seed})")
    st = build_initial_state(cfg, start)
    st._history_end = end  # only write inventory for the last N days of the backfill
    days = [start + timedelta(days=i) for i in range((end - start).days + 1)]
    totals = run_days(root, st, days)
    del st._history_end
    save_state(root, st)
    print("Done. Rows written:")
    _print_totals(totals)
    print(f"Files are in {root / 'landing'}  |  state saved to {_state_path(root)}")


def cmd_daily(args) -> None:
    root = Path(args.out)
    st = load_state(root)
    days = [st.last_date + timedelta(days=i) for i in range(1, args.days + 1)]
    print(f"{RETAILER} simulator: daily run for {days[0]} -> {days[-1]}")
    totals = run_days(root, st, days, quiet=args.days < 30)
    save_state(root, st)
    _print_totals(totals)


def main(argv=None) -> None:
    ap = argparse.ArgumentParser(description=f"{RETAILER} source-system simulator")
    sub = ap.add_subparsers(dest="command", required=True)
    h = sub.add_parser("history", help="build the initial multi-year backfill")
    h.add_argument("--start", default="2024-10-01")
    h.add_argument("--end", default="2026-09-30")
    h.add_argument("--scale", type=float, default=1.0, help="volume multiplier (0.25 = quick test)")
    h.add_argument("--seed", type=int, default=42)
    h.add_argument("--inventory-days", type=int, default=120)
    h.add_argument("--out", default="output")
    h.add_argument("--overwrite", action="store_true")
    h.set_defaults(func=cmd_history)
    dly = sub.add_parser("daily", help="simulate the next N days after the last generated day")
    dly.add_argument("--days", type=int, default=1)
    dly.add_argument("--out", default="output")
    dly.set_defaults(func=cmd_daily)
    args = ap.parse_args(argv)
    args.func(args)


if __name__ == "__main__":
    main()
