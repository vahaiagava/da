"""Portal Penjualan — Penjualan Langsung dari stok barang jadi (FG internal).

Satu nota = stok FG keluar (FIFO, `production_qty_ledger.issue_fg`) + jurnal COGS
(Dr 5-xxxx / Cr 1-1404) + invoice AR kanonik (`rahaza_ar_invoices`, source_module
`direct_sale`) + (tunai) penerimaan kas. Semua jurnal lewat mesin posting yang sudah ada.
"""
import io
import logging
import uuid
from datetime import date, datetime, timedelta, timezone
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import Response
from pymongo import ReturnDocument

from auth import log_activity, require_auth, serialize_doc
from database import get_db
from routes.shared import require_portal_dep
from utils.counters import gen_prefixed_number

log = logging.getLogger(__name__)
router = APIRouter(prefix="/api/sales", tags=["sales-direct"],
                   dependencies=[Depends(require_portal_dep("sales"))])

COLL = "sales_direct_notes"
WRITE_ROLES = ("superadmin", "admin", "owner", "accounting", "staff_keuangan", "manager_keuangan",
               "sales", "admin_sales", "pic_toko", "cs_staff", "manager_marketing", "admin_gudang")
TERMS_DAYS = {"cash": 0, "net_7": 7, "net_14": 14, "net_30": 30}


def _uid(): return str(uuid.uuid4())
def _now(): return datetime.now(timezone.utc)


def _num(v, field="nilai"):
    try:
        f = float(v or 0)
    except (TypeError, ValueError):
        raise HTTPException(400, f"{field} harus berupa angka.")
    if f < 0:
        raise HTTPException(400, f"{field} tidak boleh negatif.")
    return f


async def _require_write(request: Request):
    user = await require_auth(request)
    role = (user.get("role") or "").lower()
    perms = user.get("_permissions") or []
    if role in WRITE_ROLES or "*" in perms or "sales.manage" in perms or "finance.manage" in perms:
        return user
    raise HTTPException(403, "Forbidden: butuh hak membuat penjualan.")


# ── MASTER PELANGGAN (SSOT rahaza_customers) ─────────────────────────────────
@router.get("/customers")
async def list_customers(request: Request, include_inactive: bool = False):
    await require_auth(request)
    db = get_db()
    q = {} if include_inactive else {"active": {"$ne": False}}
    rows = await db.rahaza_customers.find(q, {"_id": 0}).sort("name", 1).to_list(1000)
    return serialize_doc(rows)


@router.post("/customers")
async def create_customer(request: Request):
    user = await _require_write(request)
    db = get_db()
    body = await request.json()
    name = (body.get("name") or "").strip()
    if not name:
        raise HTTPException(400, "Nama pelanggan wajib.")
    code = (body.get("code") or "").strip().upper()
    if not code:
        code = await gen_prefixed_number(db, "rahaza_customers", "code", "CUST-", 4)
    if await db.rahaza_customers.find_one({"code": code, "active": True}):
        raise HTTPException(409, f"Kode '{code}' sudah terpakai.")
    doc = {
        "id": _uid(), "code": code, "name": name,
        "company_type": body.get("company_type") or "personal",
        "npwp": (body.get("npwp") or "").strip(), "phone": (body.get("phone") or "").strip(),
        "email": (body.get("email") or "").strip(), "address": body.get("address") or "",
        "payment_terms": body.get("payment_terms") or "cash",
        "credit_limit": _num(body.get("credit_limit"), "credit_limit"),
        "notes": body.get("notes") or "", "active": True,
        "created_at": _now(), "updated_at": _now(), "created_by": user.get("id"),
    }
    await db.rahaza_customers.insert_one(doc)
    try:
        from routes.coa_auto import ensure_subledger_for_entity
        await ensure_subledger_for_entity(db, "customer", doc, user)
    except Exception as e:  # noqa: BLE001
        log.error("[sales] subledger piutang gagal utk %s: %s", code, e)
    await log_activity(user["id"], user.get("name", ""), "create", "sales.customer", code)
    return serialize_doc(await db.rahaza_customers.find_one({"id": doc["id"]}, {"_id": 0}))


@router.put("/customers/{cid}")
async def update_customer(cid: str, request: Request):
    user = await _require_write(request)
    db = get_db()
    body = await request.json()
    upd = {k: body[k] for k in ("name", "company_type", "npwp", "phone", "email", "address",
                                "payment_terms", "notes", "active") if k in body}
    if "credit_limit" in body:
        upd["credit_limit"] = _num(body.get("credit_limit"), "credit_limit")
    upd["updated_at"] = _now()
    res = await db.rahaza_customers.update_one({"id": cid}, {"$set": upd})
    if not res.matched_count:
        raise HTTPException(404, "Pelanggan tidak ditemukan.")
    await log_activity(user["id"], user.get("name", ""), "update", "sales.customer", cid)
    return serialize_doc(await db.rahaza_customers.find_one({"id": cid}, {"_id": 0}))


@router.delete("/customers/{cid}")
async def deactivate_customer(cid: str, request: Request):
    await _require_write(request)
    db = get_db()
    await db.rahaza_customers.update_one({"id": cid}, {"$set": {"active": False, "updated_at": _now()}})
    return {"status": "deactivated"}


# ── STOK FG YANG BISA DIJUAL ──────────────────────────────────────────────────
async def _fg_available_map(db, material_ids: Optional[list] = None) -> dict:
    match = {"inventory_category": "fg_internal"}
    if material_ids is not None:
        match["material_id"] = {"$in": material_ids}
    rows = await db.rahaza_material_stock.aggregate([
        {"$match": match},
        {"$group": {"_id": "$material_id", "qty": {"$sum": {"$ifNull": ["$qty", 0]}}}},
    ]).to_list(20000)
    return {r["_id"]: float(r.get("qty") or 0) for r in rows if r.get("_id")}


@router.get("/fg-stock")
async def fg_stock(request: Request, q: Optional[str] = None):
    await require_auth(request)
    db = get_db()
    avail = await _fg_available_map(db)
    ids = [k for k, v in avail.items() if v > 0]
    if not ids:
        return []
    mats = await db.rahaza_materials.find({"id": {"$in": ids}}, {"_id": 0, "id": 1, "code": 1, "sku": 1, "name": 1,
                                                                  "hpp": 1, "hpp_source": 1, "hpp_fifo_avg": 1}).to_list(20000)
    cats = await db.marketing_catalog_items.find({"fg_product_id": {"$in": ids}},
                                                 {"_id": 0, "fg_product_id": 1, "selling_price": 1, "price": 1}).to_list(20000)
    price_map = {c["fg_product_id"]: float(c.get("selling_price") or c.get("price") or 0) for c in cats}
    out = []
    ql = (q or "").lower()
    for m in mats:
        if ql and ql not in (m.get("code") or "").lower() and ql not in (m.get("name") or "").lower():
            continue
        out.append({"material_id": m["id"], "sku": m.get("code") or m.get("sku") or "", "name": m.get("name") or "",
                    "available_qty": avail.get(m["id"], 0), "hpp": float(m.get("hpp_fifo_avg") or m.get("hpp") or 0),
                    "hpp_source": m.get("hpp_source") or "none", "default_price": price_map.get(m["id"], 0)})
    out.sort(key=lambda r: r["sku"])
    return out


# ── NOTA PENJUALAN LANGSUNG ───────────────────────────────────────────────────
def _calc(items, tax_pct, discount):
    subtotal = sum(i["amount"] for i in items)
    tax = round(subtotal * tax_pct / 100)
    total = round(subtotal + tax - discount)
    if total < 0:
        raise HTTPException(400, "Diskon melebihi nilai nota.")
    return round(subtotal), tax, total


@router.post("/direct-sales")
async def create_direct_sale(request: Request):
    user = await _require_write(request)
    db = get_db()
    body = await request.json()
    cust = await db.rahaza_customers.find_one({"id": body.get("customer_id")}, {"_id": 0})
    if not cust:
        raise HTTPException(400, "Pelanggan wajib dipilih (master pelanggan).")
    payment_type = body.get("payment_type") or "cash"
    if payment_type not in ("cash", "credit"):
        raise HTTPException(400, "payment_type harus cash|credit.")
    raw_items = body.get("items") or []
    if not raw_items:
        raise HTTPException(400, "Minimal satu item.")
    ids = [i.get("material_id") for i in raw_items if i.get("material_id")]
    mats = {m["id"]: m async for m in db.rahaza_materials.find({"id": {"$in": ids}}, {"_id": 0, "id": 1, "code": 1, "name": 1})}
    avail = await _fg_available_map(db, ids)
    items, want = [], {}
    for idx, it in enumerate(raw_items):
        m = mats.get(it.get("material_id"))
        if not m:
            raise HTTPException(400, f"Item ke-{idx + 1}: SKU FG tidak ditemukan.")
        qty = int(_num(it.get("qty"), "qty"))
        price = _num(it.get("price"), "harga")
        if qty <= 0:
            raise HTTPException(400, f"Item ke-{idx + 1}: qty harus > 0.")
        want[m["id"]] = want.get(m["id"], 0) + qty
        if want[m["id"]] > avail.get(m["id"], 0):
            raise HTTPException(400, f"Stok {m.get('code')} tidak cukup (tersedia {int(avail.get(m['id'], 0))}, diminta {want[m['id']]}).")
        items.append({"material_id": m["id"], "sku": m.get("code") or "", "name": m.get("name") or "",
                      "qty": qty, "price": price, "amount": round(qty * price)})
    tax_pct = _num(body.get("tax_pct"), "tax_pct")
    discount = _num(body.get("discount_amount"), "discount_amount")
    subtotal, tax, total = _calc(items, tax_pct, discount)
    sale_date = body.get("sale_date") or date.today().isoformat()
    if payment_type == "cash":
        if not body.get("cash_account_id"):
            raise HTTPException(400, "Rekening kas/bank wajib untuk penjualan tunai.")
        if not await db.rahaza_cash_accounts.find_one({"id": body["cash_account_id"]}):
            raise HTTPException(404, "Rekening kas tidak ditemukan.")
        due_date = sale_date
    else:
        days = TERMS_DAYS.get(cust.get("payment_terms") or "", 30)
        due_date = body.get("due_date") or (date.fromisoformat(sale_date) + timedelta(days=days or 30)).isoformat()
    doc = {
        "id": _uid(), "note_number": await gen_prefixed_number(db, COLL, "note_number", f"SL-{sale_date.replace('-', '')}-", 3),
        "customer_id": cust["id"], "customer_name": cust.get("name"), "customer_code": cust.get("code"),
        "sale_date": sale_date, "due_date": due_date, "payment_type": payment_type,
        "cash_account_id": body.get("cash_account_id") if payment_type == "cash" else None,
        "items": items, "subtotal": subtotal, "tax_pct": tax_pct, "tax_amount": tax,
        "discount_amount": round(discount), "total": total,
        "status": "draft", "notes": body.get("notes") or "",
        "created_at": _now(), "updated_at": _now(), "created_by": user.get("id"), "created_by_name": user.get("name", ""),
    }
    await db[COLL].insert_one(doc)
    await log_activity(user["id"], user.get("name", ""), "create", "sales.direct", doc["note_number"])
    return serialize_doc(doc)


async def _apply_ar_payment(db, inv_id: str, amount: float, cash_account_id: str, pay_date: str, user: dict, notes: str = ""):
    """Terima pembayaran AR: update invoice atomik + kas + jurnal (pola rahaza_finance.record_ar_payment)."""
    from routes.rahaza_posting import post_ar_invoice, post_ar_payment
    acc = await db.rahaza_cash_accounts.find_one({"id": cash_account_id}, {"_id": 0})
    if not acc:
        raise HTTPException(404, "Rekening kas tidak ditemukan.")
    updated = await db.rahaza_ar_invoices.find_one_and_update(
        {"id": inv_id, "status": {"$nin": ["cancelled", "void", "written_off"]},
         "$expr": {"$lte": [{"$add": [{"$ifNull": ["$paid_amount", 0]}, amount]}, {"$add": [{"$ifNull": ["$total", 0]}, 0.01]}]}},
        [{"$set": {
            "paid_amount": {"$round": [{"$add": [{"$ifNull": ["$paid_amount", 0]}, amount]}, 0]},
            "balance": {"$max": [0, {"$round": [{"$subtract": [{"$ifNull": ["$total", 0]}, {"$add": [{"$ifNull": ["$paid_amount", 0]}, amount]}]}, 0]}]},
            "status": {"$cond": [{"$gte": [{"$add": [{"$ifNull": ["$paid_amount", 0]}, amount]}, {"$subtract": [{"$ifNull": ["$total", 0]}, 0.01]}]}, "paid", "partial_paid"]},
            "updated_at": _now()}},
         {"$set": {"total_amount": {"$ifNull": ["$total", 0]}, "amount_paid": "$paid_amount", "amount_due": "$balance"}}],
        return_document=ReturnDocument.AFTER)
    if not updated:
        raise HTTPException(400, "Pembayaran melebihi sisa tagihan atau invoice sudah ditutup.")
    movement_id = _uid()
    await db.rahaza_cash_movements.insert_one({
        "id": movement_id, "account_id": cash_account_id, "account_name": acc.get("name"), "direction": "in",
        "amount": round(amount), "category": "ar_payment", "ref_id": inv_id, "ref_label": updated.get("invoice_number"),
        "date": pay_date, "notes": notes, "timestamp": _now(), "created_by": user["id"], "created_by_name": user.get("name", "")})
    await db.rahaza_cash_accounts.update_one({"id": cash_account_id}, {"$inc": {"balance": round(amount)}})
    if not updated.get("gl_je_id"):
        await post_ar_invoice(db, updated, user)
        updated = await db.rahaza_ar_invoices.find_one({"id": inv_id}, {"_id": 0})
    posting = await post_ar_payment(db, updated, amount, cash_account_id, pay_date, user, movement_id=movement_id)
    return {"invoice": await db.rahaza_ar_invoices.find_one({"id": inv_id}, {"_id": 0}), "movement_id": movement_id, "posting": posting}


async def _post_cogs(db, note: dict, issued_rows: list, user: dict) -> dict:
    from routes.rahaza_posting import _create_posted_je, _fifo_rows_to_components, _find_existing_je
    from routes.rahaza_posting_profiles import get_mapping
    source_ref = f"cogs_sale:{note['id']}"
    existing = await _find_existing_je(db, "cogs_direct_sale", source_ref)
    if existing:
        return {"ok": True, "je_id": existing["id"], "je_number": existing["je_number"], "already_posted": True}
    mapping = await get_mapping(db, "cogs_shipment")
    dm, dl, do, cfg = (mapping.get(k) for k in ("debit_cogs_material", "debit_cogs_labor", "debit_cogs_overhead", "credit_fg_inventory"))
    if not all([dm, dl, do, cfg]):
        return {"ok": False, "error": "Mapping 'cogs_shipment' belum lengkap."}
    fifo = await _fifo_rows_to_components(db, issued_rows)
    # Fallback JUJUR (seperti post_cogs_shipment): pcs tanpa lapisan biaya memakai HPP master (perkiraan), diberi label.
    est_total, est_rows = 0.0, []
    for r in issued_rows:
        if r.get("fg_cogs_uncosted_qty") and r.get("material_id"):
            m = await db.rahaza_materials.find_one({"id": r["material_id"]}, {"_id": 0, "hpp": 1})
            hpp = float((m or {}).get("hpp") or 0)
            if hpp > 0:
                est = round(hpp * int(r["fg_cogs_uncosted_qty"]), 2)
                est_total += est
                r["fg_cogs_estimated"] = est
                est_rows.append(f"{r['sku']}: {r['fg_cogs_uncosted_qty']} pcs × HPP master Rp {hpp:,.0f}")
    total = round(fifo["total"] + est_total, 2)
    basis = "fifo_batch" if fifo["total"] > 0 and not est_total else ("hpp_master" if not fifo["total"] and est_total else "mixed")
    if total <= 0:
        return {"ok": False, "reason": "zero_cogs", "uncosted_qty": fifo["uncosted_qty"], "basis": basis,
                "error": "COGS = 0: barang keluar tanpa lapisan biaya batch dan tanpa HPP master — HPP tidak bisa dibukukan."}
    label = {"fifo_batch": "biaya batch FIFO", "hpp_master": "perkiraan HPP master", "mixed": "FIFO + perkiraan HPP master"}[basis]
    memo = f"COGS Penjualan Langsung {note['note_number']} ({label})"
    lines = []
    for code, amt, lbl in ((dm, fifo["material"] + est_total, "Material"), (dl, fifo["labor"], "Labor"), (do, fifo["overhead"], "Overhead")):
        if amt > 0:
            lines.append({"account_code": code, "debit": round(amt, 2), "credit": 0, "description": f"{memo} - {lbl}"})
    lines.append({"account_code": cfg, "debit": 0, "credit": total, "description": f"{memo} - FG Inventory"})
    res = await _create_posted_je(db, date.fromisoformat(note["sale_date"]), memo, "cogs_direct_sale", source_ref, lines, user)
    res.update({"total_cogs": total, "fifo_cogs": round(fifo["total"], 2), "estimated_cogs": round(est_total, 2), "basis": basis,
                "uncosted_qty": fifo["uncosted_qty"], "gaps": fifo["gaps"] + est_rows})
    return res


@router.post("/direct-sales/{sid}/confirm")
async def confirm_direct_sale(sid: str, request: Request):
    user = await _require_write(request)
    db = get_db()
    from core.production_qty_ledger import FGStockShortfall, issue_fg
    from routes.rahaza_posting import post_ar_invoice
    note = await db[COLL].find_one_and_update({"id": sid, "status": "draft"}, {"$set": {"status": "confirming", "updated_at": _now()}},
                                              projection={"_id": 0}, return_document=ReturnDocument.AFTER)
    if not note:
        cur = await db[COLL].find_one({"id": sid}, {"_id": 0, "status": 1})
        if not cur:
            raise HTTPException(404, "Nota tidak ditemukan.")
        raise HTTPException(400, f"Nota berstatus '{cur.get('status')}' tidak bisa dikonfirmasi.")
    # cek stok dulu supaya tidak keluar separuh
    avail = await _fg_available_map(db, [i["material_id"] for i in note["items"]])
    need = {}
    for it in note["items"]:
        need[it["material_id"]] = need.get(it["material_id"], 0) + it["qty"]
    short = [f"{it['sku']} (tersedia {int(avail.get(it['material_id'], 0))}, perlu {need[it['material_id']]})"
             for it in note["items"] if need[it["material_id"]] > avail.get(it["material_id"], 0)]
    if short:
        await db[COLL].update_one({"id": sid}, {"$set": {"status": "draft"}})
        raise HTTPException(400, "Stok tidak cukup: " + "; ".join(sorted(set(short))))

    ref = {"type": "direct_sale", "id": sid, "note_number": note["note_number"], "doc": "sales_direct_notes"}
    issued_rows, total_uncosted = [], 0
    try:
        for it in note["items"]:
            r = await issue_fg(db, material_id=it["material_id"], qty=it["qty"], ref=ref, actor=user, sku=it["sku"])
            it["fg_cogs"] = float(r.get("cogs") or 0)
            it["fg_cogs_layers"] = r.get("cogs_layers") or []
            it["fg_cogs_uncosted_qty"] = int(r.get("uncosted_qty") or 0)
            total_uncosted += it["fg_cogs_uncosted_qty"]
            issued_rows.append({"material_id": it["material_id"], "sku": it["sku"], "qty_shipped": it["qty"], "fg_cogs": it["fg_cogs"],
                                "fg_cogs_layers": it["fg_cogs_layers"], "fg_cogs_uncosted_qty": it["fg_cogs_uncosted_qty"]})
    except FGStockShortfall as e:
        await db[COLL].update_one({"id": sid}, {"$set": {"status": "draft", "items": note["items"]}})
        raise HTTPException(400, f"Stok tidak cukup saat pengeluaran: {e}")

    from routes.rahaza_finance import _gen_number
    invoice_number = await _gen_number(db, "rahaza_ar_invoices", "AR")
    inv = {
        "id": _uid(), "invoice_number": invoice_number, "customer_id": note["customer_id"], "customer_name": note["customer_name"],
        "order_id": None, "issue_date": note["sale_date"], "due_date": note["due_date"],
        "items": [{"description": f"{i['sku']} {i['name']}".strip(), "qty": i["qty"], "unit": "pcs", "price": i["price"], "amount": i["amount"]} for i in note["items"]],
        "subtotal": note["subtotal"], "tax_pct": note["tax_pct"], "tax_amount": note["tax_amount"], "discount_amount": note["discount_amount"],
        "total": note["total"], "paid_amount": 0, "balance": note["total"],
        "total_amount": note["total"], "amount_paid": 0, "amount_due": note["total"],
        "status": "issued", "notes": f"Penjualan langsung {note['note_number']}", "sales_channel": "direct",
        "source_module": "direct_sale", "direct_sale_id": sid, "created_at": _now(), "updated_at": _now(),
        "created_by": user["id"], "created_by_name": user.get("name", ""),
    }
    await db.rahaza_ar_invoices.insert_one(inv)
    inv.pop("_id", None)
    ar_post = await post_ar_invoice(db, inv, user)
    cogs_post = await _post_cogs(db, note, issued_rows, user)
    for it, r in zip(note["items"], issued_rows):
        if r.get("fg_cogs_estimated"):
            it["fg_cogs_estimated"] = r["fg_cogs_estimated"]
    upd = {"status": "confirmed", "items": note["items"], "ar_invoice_id": inv["id"], "invoice_number": invoice_number,
           "ar_je_id": ar_post.get("je_id"), "ar_post_error": ar_post.get("error"),
           "cogs_total": round(float(cogs_post.get("total_cogs") or 0), 2), "cogs_basis": cogs_post.get("basis"),
           "cogs_estimated": round(float(cogs_post.get("estimated_cogs") or 0), 2), "cogs_je_id": cogs_post.get("je_id"),
           "cogs_je_number": cogs_post.get("je_number"), "cogs_post_error": cogs_post.get("error"),
           "uncosted_qty": total_uncosted, "confirmed_at": _now(), "confirmed_by": user.get("name", ""), "updated_at": _now()}
    payment = None
    if note["payment_type"] == "cash":
        try:
            payment = await _apply_ar_payment(db, inv["id"], float(note["total"]), note["cash_account_id"], note["sale_date"], user,
                                              notes=f"Tunai {note['note_number']}")
            upd["status"] = "paid"
            upd["paid_amount"] = note["total"]
            upd["payment_je_id"] = (payment.get("posting") or {}).get("je_id")
        except HTTPException as e:
            upd["payment_error"] = e.detail
    await db[COLL].update_one({"id": sid}, {"$set": upd})
    await log_activity(user["id"], user.get("name", ""), "confirm", "sales.direct", note["note_number"])
    out = await db[COLL].find_one({"id": sid}, {"_id": 0})
    out["_ar_posting"] = ar_post
    out["_cogs_posting"] = cogs_post
    return serialize_doc(out)


@router.post("/direct-sales/{sid}/payment")
async def pay_direct_sale(sid: str, request: Request):
    user = await _require_write(request)
    db = get_db()
    body = await request.json()
    note = await db[COLL].find_one({"id": sid}, {"_id": 0})
    if not note or not note.get("ar_invoice_id"):
        raise HTTPException(404, "Nota belum dikonfirmasi / tidak ditemukan.")
    amount = _num(body.get("amount"), "amount")
    if amount <= 0:
        raise HTTPException(400, "amount harus > 0.")
    if not body.get("cash_account_id"):
        raise HTTPException(400, "Rekening kas wajib.")
    res = await _apply_ar_payment(db, note["ar_invoice_id"], amount, body["cash_account_id"], body.get("date") or date.today().isoformat(), user,
                                  notes=body.get("notes") or f"Pelunasan {note['note_number']}")
    inv = res["invoice"]
    await db[COLL].update_one({"id": sid}, {"$set": {"paid_amount": inv.get("paid_amount"), "status": "paid" if inv.get("status") == "paid" else "confirmed",
                                                     "updated_at": _now()}})
    return serialize_doc({"note": await db[COLL].find_one({"id": sid}, {"_id": 0}), "invoice": inv, "posting": res["posting"]})


@router.post("/direct-sales/{sid}/cancel")
async def cancel_direct_sale(sid: str, request: Request):
    user = await _require_write(request)
    db = get_db()
    res = await db[COLL].find_one_and_update({"id": sid, "status": "draft"},
                                             {"$set": {"status": "cancelled", "cancelled_at": _now(), "cancelled_by": user.get("name", ""), "updated_at": _now()}},
                                             projection={"_id": 0}, return_document=ReturnDocument.AFTER)
    if not res:
        raise HTTPException(400, "Hanya nota draft yang bisa dibatalkan (nota terkonfirmasi sudah mengurangi stok & berjurnal).")
    return serialize_doc(res)


@router.get("/direct-sales")
async def list_direct_sales(request: Request, status: Optional[str] = None, customer_id: Optional[str] = None, limit: int = 300):
    await require_auth(request)
    db = get_db()
    q = {}
    if status:
        q["status"] = status
    if customer_id:
        q["customer_id"] = customer_id
    rows = await db[COLL].find(q, {"_id": 0}).sort("created_at", -1).to_list(min(limit, 1000))
    inv_ids = [r["ar_invoice_id"] for r in rows if r.get("ar_invoice_id")]
    invs = {i["id"]: i async for i in db.rahaza_ar_invoices.find({"id": {"$in": inv_ids}}, {"_id": 0, "id": 1, "status": 1, "balance": 1, "paid_amount": 1})} if inv_ids else {}
    for r in rows:
        i = invs.get(r.get("ar_invoice_id")) or {}
        r["ar_status"] = i.get("status")
        r["ar_balance"] = i.get("balance")
    return serialize_doc(rows)


@router.get("/direct-sales/{sid}")
async def get_direct_sale(sid: str, request: Request):
    await require_auth(request)
    db = get_db()
    note = await db[COLL].find_one({"id": sid}, {"_id": 0})
    if not note:
        raise HTTPException(404, "Nota tidak ditemukan.")
    if note.get("ar_invoice_id"):
        note["invoice"] = await db.rahaza_ar_invoices.find_one({"id": note["ar_invoice_id"]}, {"_id": 0})
        note["payments"] = await db.rahaza_cash_movements.find({"ref_id": note["ar_invoice_id"], "category": "ar_payment"}, {"_id": 0}).sort("timestamp", 1).to_list(100)
    return serialize_doc(note)


@router.get("/dashboard")
async def sales_dashboard(request: Request):
    await require_auth(request)
    db = get_db()
    today = date.today().isoformat()
    month = today[:7]
    rows = await db[COLL].find({"status": {"$in": ["confirmed", "paid"]}}, {"_id": 0, "sale_date": 1, "total": 1, "cogs_total": 1, "items": 1,
                                                                          "status": 1, "payment_type": 1, "ar_invoice_id": 1}).to_list(5000)
    inv_ids = [r["ar_invoice_id"] for r in rows if r.get("ar_invoice_id")]
    invs = {i["id"]: i async for i in db.rahaza_ar_invoices.find({"id": {"$in": inv_ids}}, {"_id": 0, "id": 1, "balance": 1, "status": 1, "due_date": 1})} if inv_ids else {}
    out = {"today_sales": 0.0, "today_count": 0, "month_sales": 0.0, "month_count": 0, "month_cogs": 0.0,
           "open_ar": 0.0, "open_ar_count": 0, "overdue_ar": 0.0, "overdue_count": 0, "top_skus": [], "draft_count": 0}
    sku_acc = {}
    for r in rows:
        if r.get("sale_date") == today:
            out["today_sales"] += float(r.get("total") or 0)
            out["today_count"] += 1
        if (r.get("sale_date") or "")[:7] == month:
            out["month_sales"] += float(r.get("total") or 0)
            out["month_cogs"] += float(r.get("cogs_total") or 0)
            out["month_count"] += 1
            for it in r.get("items") or []:
                s = sku_acc.setdefault(it["sku"], {"sku": it["sku"], "name": it.get("name"), "qty": 0, "amount": 0.0})
                s["qty"] += it.get("qty") or 0
                s["amount"] += float(it.get("amount") or 0)
        inv = invs.get(r.get("ar_invoice_id")) or {}
        bal = float(inv.get("balance") or 0)
        if bal > 0 and inv.get("status") not in ("cancelled", "written_off"):
            out["open_ar"] += bal
            out["open_ar_count"] += 1
            if (inv.get("due_date") or today) < today:
                out["overdue_ar"] += bal
                out["overdue_count"] += 1
    out["draft_count"] = await db[COLL].count_documents({"status": "draft"})
    out["month_gross_margin"] = round(out["month_sales"] - out["month_cogs"], 2)
    out["top_skus"] = sorted(sku_acc.values(), key=lambda s: -s["amount"])[:5]
    for k in ("today_sales", "month_sales", "month_cogs", "open_ar", "overdue_ar"):
        out[k] = round(out[k], 2)
    return out


@router.get("/cash-accounts")
async def cash_accounts(request: Request):
    await require_auth(request)
    db = get_db()
    rows = await db.rahaza_cash_accounts.find({"active": {"$ne": False}}, {"_id": 0, "id": 1, "name": 1, "type": 1, "bank_name": 1}).to_list(200)
    return serialize_doc(rows)


# ── CETAK NOTA (PDF) ──────────────────────────────────────────────────────────
def _rp(n) -> str:
    return "Rp " + f"{int(round(float(n or 0))):,}".replace(",", ".")


@router.get("/direct-sales/{sid}/pdf")
async def direct_sale_pdf(sid: str, request: Request):
    await require_auth(request)
    db = get_db()
    note = await db[COLL].find_one({"id": sid}, {"_id": 0})
    if not note:
        raise HTTPException(404, "Nota tidak ditemukan.")
    cust = await db.rahaza_customers.find_one({"id": note["customer_id"]}, {"_id": 0}) or {}
    from utils.pdf_common import get_company_profile
    prof = await get_company_profile(db) or {}
    from reportlab.lib import colors
    from reportlab.lib.pagesizes import A5, landscape
    from reportlab.lib.styles import getSampleStyleSheet
    from reportlab.lib.units import mm
    from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle
    buf = io.BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=landscape(A5), leftMargin=10 * mm, rightMargin=10 * mm, topMargin=8 * mm, bottomMargin=8 * mm)
    st = getSampleStyleSheet()
    small = st["Normal"].clone("small", fontSize=8, leading=10)
    el = [Paragraph(f"<b>{prof.get('company_name') or 'CV. DEWI ADITYA'}</b>", st["Title"].clone("t", fontSize=13, leading=15, alignment=0)),
          Paragraph(" · ".join(x for x in [prof.get("address"), prof.get("phone"), prof.get("email")] if x), small), Spacer(1, 4)]
    status_lbl = {"draft": "DRAFT", "confirmed": "BELUM LUNAS", "paid": "LUNAS", "cancelled": "BATAL"}.get(note.get("status"), note.get("status", "").upper())
    head = Table([[Paragraph(f"<b>NOTA PENJUALAN</b><br/>No: {note['note_number']}<br/>Tanggal: {note['sale_date']}<br/>Invoice: {note.get('invoice_number') or '-'}", small),
                   Paragraph(f"<b>Kepada:</b> {cust.get('name') or note.get('customer_name')}<br/>{cust.get('address') or ''}<br/>{cust.get('phone') or ''}", small),
                   Paragraph(f"<b>Pembayaran:</b> {'TUNAI' if note['payment_type'] == 'cash' else 'TEMPO'}<br/>Jatuh tempo: {note.get('due_date')}<br/><b>Status: {status_lbl}</b>", small)]],
                 colWidths=[68 * mm, 68 * mm, 50 * mm])
    el += [head, Spacer(1, 5)]
    rows = [["#", "SKU", "Nama Barang", "Qty", "Harga", "Jumlah"]]
    for i, it in enumerate(note["items"], 1):
        rows.append([str(i), it["sku"], Paragraph(it.get("name") or "", small), str(it["qty"]), _rp(it["price"]), _rp(it["amount"])])
    rows.append(["", "", "", "", "Subtotal", _rp(note["subtotal"])])
    if note.get("tax_amount"):
        rows.append(["", "", "", "", f"PPN {int(note.get('tax_pct') or 0)}%", _rp(note["tax_amount"])])
    if note.get("discount_amount"):
        rows.append(["", "", "", "", "Diskon", f"- {_rp(note['discount_amount'])}"])
    rows.append(["", "", "", "", "TOTAL", _rp(note["total"])])
    t = Table(rows, colWidths=[8 * mm, 34 * mm, 70 * mm, 14 * mm, 30 * mm, 30 * mm], repeatRows=1)
    t.setStyle(TableStyle([("FONTSIZE", (0, 0), (-1, -1), 8), ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#e5e7eb")),
                           ("GRID", (0, 0), (-1, len(note["items"])), 0.4, colors.grey), ("ALIGN", (3, 1), (-1, -1), "RIGHT"),
                           ("FONTNAME", (4, -1), (-1, -1), "Helvetica-Bold"), ("LINEABOVE", (4, -1), (-1, -1), 0.8, colors.black)]))
    el += [t, Spacer(1, 6)]
    if note.get("notes"):
        el.append(Paragraph(f"Catatan: {note['notes']}", small))
    el.append(Spacer(1, 10))
    el.append(Table([[Paragraph("Penerima,<br/><br/><br/>(________________)", small), Paragraph(f"Hormat kami,<br/><br/><br/>({note.get('confirmed_by') or note.get('created_by_name') or '________________'})", small)]],
                    colWidths=[93 * mm, 93 * mm]))
    doc.build(el)
    return Response(content=buf.getvalue(), media_type="application/pdf",
                    headers={"Content-Disposition": f'inline; filename="Nota_{note["note_number"]}.pdf"'})
