"""Builds docs/images/architecture.svg (run: python make_architecture_svg.py)."""
from pathlib import Path

W, H = 1500, 1010
FONT = "Segoe UI, Helvetica Neue, Arial, sans-serif"
INK = "#1F2937"
MUTED = "#4B5563"
STYLE = {
    "src": ("#F4F4F5", "#71717A"),
    "ops": ("#DBE7FB", "#2F5597"),
    "bronze": ("#EBCBA8", "#8C5A2B"),
    "silver": ("#E4E8EC", "#5F6B78"),
    "gold": ("#F7E39A", "#A07D00"),
    "bad": ("#F8D7D7", "#B03A3A"),
}
PANEL = {"src": "#FAFAFA", "ops": "#F3F7FE", "bronze": "#FBF3EA", "silver": "#F4F6F8", "gold": "#FDF8E3"}

out: list[str] = []


def text_block(cx, cy, lines, size=15, title_size=17.5):
    """lines[0] is the bold title; the rest are regular. Vertically centered on cy."""
    gap = 4
    heights = [title_size] + [size] * (len(lines) - 1)
    total = sum(heights) + gap * (len(lines) - 1)
    y = cy - total / 2
    for i, (line, h) in enumerate(zip(lines, heights)):
        y += h
        weight = "700" if i == 0 else "400"
        color = INK if i == 0 else MUTED
        out.append(f'<text x="{cx}" y="{y - 3:.1f}" font-size="{h}" font-weight="{weight}" '
                   f'fill="{color}" text-anchor="middle">{line}</text>')
        y += gap


def box(x, y, w, h, kind, lines, rx=10, shape="rect", **kw):
    fill, stroke = STYLE[kind]
    if shape == "rect":
        out.append(f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="{rx}" fill="{fill}" '
                   f'stroke="{stroke}" stroke-width="2.2"/>')
        text_block(x + w / 2, y + h / 2, lines, **kw)
    else:  # cylinder (data store)
        e = 13
        out.append(f'<path d="M{x},{y + e} L{x},{y + h - e} A{w / 2},{e} 0 0 0 {x + w},{y + h - e} '
                   f'L{x + w},{y + e} A{w / 2},{e} 0 0 0 {x},{y + e} Z" fill="{fill}" stroke="{stroke}" stroke-width="2.4"/>')
        out.append(f'<ellipse cx="{x + w / 2}" cy="{y + e}" rx="{w / 2}" ry="{e}" fill="{fill}" '
                   f'stroke="{stroke}" stroke-width="2.4"/>')
        text_block(x + w / 2, y + h / 2 + e / 2, lines, **kw)


def arrow(points, dashed=False, color="#52525B", label=None, label_at=None, width=2.2):
    d = "M" + " L".join(f"{px},{py}" for px, py in points)
    dash = ' stroke-dasharray="7 5"' if dashed else ""
    out.append(f'<path d="{d}" fill="none" stroke="{color}" stroke-width="{width}"{dash} '
               f'marker-end="url(#ah{color[1:]})"/>')
    if label:
        lx, ly = label_at
        wbox = len(label) * 7.4 + 14
        out.append(f'<rect x="{lx - wbox / 2}" y="{ly - 13}" width="{wbox}" height="22" rx="5" fill="#FFFFFF" '
                   f'stroke="#D4D4D8"/>')
        out.append(f'<text x="{lx}" y="{ly + 3}" font-size="13.5" fill="{MUTED}" text-anchor="middle" '
                   f'font-style="italic">{label}</text>')


def panel(x, w, kind, title, y=78, h=592):
    stroke = STYLE[kind][1]
    out.append(f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="14" fill="{PANEL[kind]}" '
               f'stroke="{stroke}" stroke-opacity="0.45" stroke-width="1.6" stroke-dasharray="8 5"/>')
    out.append(f'<text x="{x + w / 2}" y="{y + 32}" font-size="19" font-weight="700" fill="{stroke}" '
               f'text-anchor="middle" letter-spacing="1.2">{title}</text>')


# ---- canvas --------------------------------------------------------------
out.append(f'<rect width="{W}" height="{H}" fill="#FFFFFF"/>')
out.append(f'<text x="30" y="46" font-size="26" font-weight="700" fill="{INK}">Harborline Retail · '
           f'Medallion Architecture on Microsoft Fabric</text>')
out.append(f'<text x="{W - 30}" y="46" font-size="15" fill="{MUTED}" text-anchor="end">'
           f'Workspace: Retail Medallion Dev → Prod · Git: GitHub</text>')

# columns: x positions and width
CW, GAP, X0 = 218, 26, 25
cols = [X0 + i * (CW + GAP) for i in range(6)]
cx = [c + CW / 2 for c in cols]
panel(cols[0], CW, "src", "1 · SOURCES")
panel(cols[1], CW, "ops", "2 · INGESTION")
panel(cols[2], CW, "bronze", "3 · BRONZE")
panel(cols[3], CW, "silver", "4 · SILVER")
panel(cols[4], CW, "gold", "5 · GOLD")
panel(cols[5], CW, "gold", "6 · SERVING")

# 1 · sources
bw, bh = 190, 84
sx = cols[0] + (CW - bw) / 2
src_y = [130, 233, 336, 439, 560]
box(sx, src_y[0], bw, bh, "src", ["POS + e-commerce", "orders · order lines"])
box(sx, src_y[1], bw, bh, "src", ["CRM", "customers"])
box(sx, src_y[2], bw, bh, "src", ["Catalog API", "products (JSON)"])
box(sx, src_y[3], bw, bh, "src", ["Store operations", "stores · inventory", "returns"])
box(sx, src_y[4], bw, bh, "src", ["Public API", "US holidays"])

# 2 · ingestion
ix = cols[1] + (CW - bw) / 2
box(ix, 175, bw, 150, "src", ["lh_source", "landing zone", "daily files", "+ manifests"], shape="cyl")
box(ix, 375, bw, 110, "ops", ["pl_ingest_bronze", "config-driven loop", "→ nb_bronze_load"])
box(ix, 560, bw, bh, "ops", ["df_bronze_holidays", "Dataflow Gen2"])
for y in src_y[:4]:
    arrow([(sx + bw, y + bh / 2), (ix - 4, 250)])
arrow([(ix + bw / 2, 325), (ix + bw / 2, 371)])
arrow([(sx + bw, src_y[4] + bh / 2), (ix - 4, src_y[4] + bh / 2)])

# 3 · bronze
cw_ = 196
bx = cols[2] + (CW - cw_) / 2
box(bx, 190, cw_, 330, "bronze", ["lh_bronze", "raw Delta tables", "all columns as text", "replace by date", "",
                                  "+ _ingest_ts", "+ _source_file", "+ _batch_id", "+ _row_hash"], shape="cyl")
arrow([(ix + bw, 430), (bx - 4, 430)])
arrow([(ix + bw, 602), (cols[2] - 6, 602), (cols[2] - 6, 490), (bx - 4, 490)])

# 4 · silver
vx = cols[3] + (CW - bw) / 2
box(vx, 130, bw, 92, "silver", ["nb_silver_*", "clean · type", "dedupe · MERGE"])
box(vx, 262, bw, 92, "silver", ["nb_dq_checks", "20 data-quality", "rules"])
box(vx, 394, bw, 150, "silver", ["lh_silver", "validated tables", "SCD Type 2", "customers + products"], shape="cyl")
box(vx + 25, 580, bw - 50, 70, "bad", ["quarantine", "rejected rows"], title_size=16, size=14)
arrow([(bx + cw_, 260), (vx - 4, 176)])
arrow([(vx + bw / 2, 222), (vx + bw / 2, 258)])
arrow([(vx + bw / 2, 354), (vx + bw / 2, 390)], label="valid rows", label_at=(vx + bw / 2 + 52, 372))
arrow([(vx, 330), (cols[3] + 6, 330), (cols[3] + 6, 615), (vx + 21, 615)], color="#B03A3A")

# 5 · gold
gx = cols[4] + (CW - cw_) / 2
box(gx, 160, cw_, 390, "gold", ["lh_gold", "star schema", "built by nb_gold_*", "", "dim_date", "dim_customer", "dim_product",
                                "dim_store", "", "fact_sales", "fact_returns", "fact_inventory_daily",
                                "agg_daily_store_sales"], size=14.5, shape="cyl")
arrow([(vx + bw, 470), (gx - 4, 470)])

# 6 · serving
rx_ = cols[5] + (CW - bw) / 2
box(rx_, 150, bw, 100, "gold", ["sm_retail_sales", "Direct Lake model", "measures + RLS"])
box(rx_, 300, bw, 100, "gold", ["Power BI reports", "sales overview", "pipeline health"])
box(rx_, 450, bw, 100, "gold", ["Sales forecast", "ML notebook"])
arrow([(gx + cw_, 220), (rx_ - 4, 200)])
arrow([(rx_ + bw / 2, 250), (rx_ + bw / 2, 296)])
arrow([(gx + cw_, 480), (rx_ - 4, 500)])

# ---- operations band ------------------------------------------------------
oy, oh = 730, 200
out.append(f'<rect x="30" y="{oy}" width="{W - 60}" height="{oh}" rx="14" fill="{PANEL["ops"]}" '
           f'stroke="#2F5597" stroke-opacity="0.45" stroke-width="1.6" stroke-dasharray="8 5"/>')
out.append(f'<text x="{W / 2}" y="{oy + 32}" font-size="19" font-weight="700" fill="#2F5597" '
           f'text-anchor="middle" letter-spacing="1.2">OPERATIONS · ORCHESTRATION · MONITORING</text>')
ow, ohh = 330, 110
ox = [90, (W - ow) / 2, W - 90 - ow]
box(ox[0], oy + 60, ow, ohh, "ops", ["pl_master_daily", "runs 2 → 6 in order, daily 06:00", "retries · failure paths"])
box(ox[1], oy + 52, ow, ohh + 16, "ops", ["lh_framework", "ingestion config · watermarks", "audit log · DQ results"],
    shape="cyl")
box(ox[2], oy + 60, ow, ohh, "ops", ["act_pipeline_alerts", "Activator: failures, DQ breaches,", "volume drops, late data"])
arrow([(ox[0] + ow, oy + 115), (ox[1] - 4, oy + 115)], label="logs every step", label_at=((ox[0] + ow + ox[1]) / 2, oy + 93))
arrow([(ox[1] + ow, oy + 115), (ox[2] - 4, oy + 115)], label="watches", label_at=((ox[1] + ow + ox[2]) / 2, oy + 93))
# orchestration links up into the layers
bus_y = 700
out.append(f'<path d="M{ox[0] + ow / 2},{oy + 60} L{ox[0] + ow / 2},{bus_y} L{cx[5]},{bus_y}" fill="none" '
           f'stroke="#2F5597" stroke-width="2" stroke-dasharray="7 5"/>')
for c in (cx[1], cx[3], cx[4], cx[5]):
    arrow([(c, bus_y), (c, 674)], dashed=True, color="#2F5597", width=2)
out.append(f'<text x="{cx[2]}" y="{bus_y - 8}" font-size="13.5" fill="#2F5597" text-anchor="middle" '
           f'font-style="italic">triggers</text>')

# ---- legend --------------------------------------------------------------
ly = 968
items = [("src", "source / landing"), ("ops", "pipeline · dataflow · framework"), ("bronze", "bronze"),
         ("silver", "silver"), ("gold", "gold"), ("bad", "rejected data")]
lx = 30
out.append(f'<text x="{lx}" y="{ly + 5}" font-size="14" font-weight="700" fill="{MUTED}">LEGEND</text>')
lx += 80
for kind, label in items:
    fill, stroke = STYLE[kind]
    out.append(f'<rect x="{lx}" y="{ly - 10}" width="22" height="18" rx="4" fill="{fill}" stroke="{stroke}" stroke-width="2"/>')
    out.append(f'<text x="{lx + 30}" y="{ly + 4}" font-size="14" fill="{MUTED}">{label}</text>')
    lx += 30 + len(label) * 7.6 + 34
out.append(f'<text x="{W - 30}" y="{ly + 4}" font-size="14" fill="{MUTED}" text-anchor="end">'
           f'solid arrow = data flow · dashed arrow = control flow</text>')

svg = (f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {W} {H}" width="{W}" height="{H}" '
       f'font-family="{FONT}">\n<defs>' + "".join(
           f'<marker id="ah{c}" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" '
           f'orient="auto-start-reverse"><path d="M0,0 L10,5 L0,10 z" fill="#{c}"/></marker>'
           for c in ("52525B", "2F5597", "B03A3A")) + '</defs>\n' + "\n".join(out) + "\n</svg>\n")
Path(__file__).with_name("architecture.svg").write_text(svg, encoding="utf-8")
print("wrote architecture.svg")
