"""
PULSY Options Collector
=======================
Щоденний snapshot опціонного ринку з Deribit Public API.
Запускається о 08:05 UTC після щоденної експірації Deribit.

Зберігає: Dropbox/pulsy/options/{COIN}/{YYYY-MM-DD}.json

Метрики:
  1. Критичні:    Put/Call Ratio, Max Pain, DVOL Index
  2. Стратегічні: OI by Strike, 25D Skew, Total OI (USD)
  3. Динаміка:    OI Change, Volume, Term Structure
  4. Технічні:    IV Surface, Greeks (Delta/Gamma/Theta/Vega)

Railway Variables:
  DROPBOX_TOKEN   — обов'язково
  COINS           — BTC,ETH (за замовч.)
"""

import asyncio, httpx, json, os, sys
from datetime import datetime, timezone, timedelta, date as dt_date
from collections import defaultdict

# ══════════════════════════════════════════════════════════════
# КОНФІГ — всі змінні з Railway Variables
# ══════════════════════════════════════════════════════════════

DBX_TOKEN = os.environ.get("DROPBOX_TOKEN", "")
COINS     = [c.strip().upper()
             for c in os.environ.get("COINS", "BTC,ETH").split(",")]

if not DBX_TOKEN:
    print("❌ DROPBOX_TOKEN не знайдено в Variables"); sys.exit(1)

DATA_DIR        = "/pulsy"
TIMEOUT         = 20
DERIBIT         = "https://www.deribit.com/api/v2/public"
SCHEDULE_HOUR   = 8    # 08:05 UTC — після щоденної експірації
SCHEDULE_MINUTE = 5


# ══════════════════════════════════════════════════════════════
# DERIBIT PUBLIC API (без ключа)
# ══════════════════════════════════════════════════════════════

async def deribit(c, method: str, params: dict = None):
    try:
        r = await c.get(
            f"{DERIBIT}/{method}",
            params=params or {},
            timeout=TIMEOUT,
        )
        if r.status_code == 200:
            data = r.json()
            if data.get("result") is not None:
                return data["result"]
            print(f"  [Deribit] {method}: {data.get('error', {})}")
        else:
            print(f"  [Deribit] {method}: HTTP {r.status_code}")
    except Exception as e:
        print(f"  [Deribit] {method}: {type(e).__name__}: {str(e)[:60]}")
    return None


# ══════════════════════════════════════════════════════════════
# DROPBOX
# ══════════════════════════════════════════════════════════════

async def dbx_save(path: str, data: dict) -> bool:
    try:
        body = json.dumps(data, ensure_ascii=False,
                          indent=2, default=str).encode("utf-8")
        arg  = json.dumps({"path": path, "mode": "overwrite",
                           "autorename": False}, ensure_ascii=False)
        async with httpx.AsyncClient() as c:
            r = await c.post(
                "https://content.dropboxapi.com/2/files/upload",
                headers={"Authorization":   f"Bearer {DBX_TOKEN}",
                         "Content-Type":    "application/octet-stream",
                         "Dropbox-API-Arg": arg},
                content=body, timeout=30,
            )
        if r.status_code == 200:
            return True
        print(f"  [DBX] ❌ {r.status_code}: {r.text[:100]}")
    except Exception as e:
        print(f"  [DBX] ❌ {e}")
    return False


async def dbx_download(path: str):
    try:
        async with httpx.AsyncClient() as c:
            r = await c.post(
                "https://content.dropboxapi.com/2/files/download",
                headers={"Authorization":   f"Bearer {DBX_TOKEN}",
                         "Dropbox-API-Arg": json.dumps({"path": path})},
                timeout=15,
            )
        if r.status_code == 200:
            return r.json()
    except Exception:
        pass
    return None


# ══════════════════════════════════════════════════════════════
# ЗБІР ДАНИХ З DERIBIT
# ══════════════════════════════════════════════════════════════

async def get_book_summaries(c, coin: str) -> list:
    """Book summary всіх опціонів — OI, Volume, IV, Mark Price, Greeks"""
    result = await deribit(c, "get_book_summary_by_currency", {
        "currency": coin,
        "kind":     "option",
    })
    return result or []


async def get_dvol(c, coin: str) -> dict:
    """DVOL Index — 30-денна implied volatility (аналог VIX для крипто)"""
    now      = datetime.now(timezone.utc)
    end_ts   = int(now.timestamp()) * 1000
    start_ts = int((now - timedelta(hours=25)).timestamp()) * 1000

    result = await deribit(c, "get_volatility_index_data", {
        "currency":        coin,
        "start_timestamp": start_ts,
        "end_timestamp":   end_ts,
        "resolution":      "3600",
    })

    if not result or not result.get("data"):
        return {}

    data   = result["data"]  # [ts, open, high, low, close]
    closes = [d[4] for d in data if len(d) > 4 and d[4]]
    if not closes:
        return {}

    return {
        "current": closes[-1],
        "open":    data[0][1]  if data else None,
        "high":    max(d[2] for d in data if len(d) > 2),
        "low":     min(d[3] for d in data if len(d) > 3),
        "close":   closes[-1],
        "change":  round(closes[-1] - data[0][1], 2) if data else None,
        "zone": (
            "low"      if closes[-1] < 30 else
            "normal"   if closes[-1] < 50 else
            "elevated" if closes[-1] < 70 else
            "high"     if closes[-1] < 90 else
            "extreme"
        ),
    }


# ══════════════════════════════════════════════════════════════
# ОБРОБКА — розрахунок всіх метрик
# ══════════════════════════════════════════════════════════════

def safe_float(v, default=0.0):
    try:
        return float(v) if v is not None else default
    except (TypeError, ValueError):
        return default


def process_options(summaries: list) -> dict:
    if not summaries:
        return {}

    calls = [s for s in summaries if s.get("instrument_name", "").endswith("-C")]
    puts  = [s for s in summaries if s.get("instrument_name", "").endswith("-P")]

    # Поточна ціна індексу
    index_price = None
    for s in summaries:
        if s.get("underlying_price"):
            index_price = safe_float(s["underlying_price"])
            break

    # ── 1. ЗАГАЛЬНІ OI та VOLUME ──────────────────────────────

    calls_oi  = sum(safe_float(s.get("open_interest")) for s in calls)
    puts_oi   = sum(safe_float(s.get("open_interest")) for s in puts)
    calls_vol = sum(safe_float(s.get("volume"))         for s in calls)
    puts_vol  = sum(safe_float(s.get("volume"))         for s in puts)
    total_oi  = calls_oi + puts_oi
    total_vol = calls_vol + puts_vol

    px = index_price or 0
    calls_oi_usd  = round(calls_oi  * px)
    puts_oi_usd   = round(puts_oi   * px)
    total_oi_usd  = calls_oi_usd + puts_oi_usd
    calls_vol_usd = round(calls_vol * px)
    puts_vol_usd  = round(puts_vol  * px)

    pc_oi  = round(puts_oi  / calls_oi,  4) if calls_oi  > 0 else None
    pc_vol = round(puts_vol / calls_vol, 4) if calls_vol > 0 else None

    # ── 2. OI BY STRIKE ───────────────────────────────────────

    by_strike = defaultdict(lambda: {
        "call_oi": 0.0, "put_oi": 0.0,
        "call_vol": 0.0, "put_vol": 0.0,
        "call_ivs": [], "put_ivs": [],
    })

    for s in summaries:
        parts = s.get("instrument_name", "").split("-")
        if len(parts) < 4:
            continue
        try:
            strike = int(parts[2])
        except ValueError:
            continue
        oi  = safe_float(s.get("open_interest"))
        vol = safe_float(s.get("volume"))
        iv  = safe_float(s.get("mark_iv"))
        if s["instrument_name"].endswith("-C"):
            by_strike[strike]["call_oi"]  += oi
            by_strike[strike]["call_vol"] += vol
            if iv > 0: by_strike[strike]["call_ivs"].append(iv)
        else:
            by_strike[strike]["put_oi"]  += oi
            by_strike[strike]["put_vol"] += vol
            if iv > 0: by_strike[strike]["put_ivs"].append(iv)

    oi_by_strike = {}
    for strike, d in sorted(by_strike.items()):
        if d["call_oi"] + d["put_oi"] < 0.1:
            continue
        oi_by_strike[str(strike)] = {
            "call_oi":  round(d["call_oi"],  2),
            "put_oi":   round(d["put_oi"],   2),
            "call_vol": round(d["call_vol"], 2),
            "put_vol":  round(d["put_vol"],  2),
            "call_iv":  round(sum(d["call_ivs"]) / len(d["call_ivs"]), 2)
                        if d["call_ivs"] else None,
            "put_iv":   round(sum(d["put_ivs"]) / len(d["put_ivs"]), 2)
                        if d["put_ivs"] else None,
        }

    # Top-10 страйків по OI
    top_strikes = sorted(
        [{"strike": int(k),
          "total_oi":   round(v["call_oi"] + v["put_oi"], 2),
          "call_oi":    round(v["call_oi"], 2),
          "put_oi":     round(v["put_oi"],  2)}
         for k, v in oi_by_strike.items()],
        key=lambda x: x["total_oi"], reverse=True
    )[:10]

    # ── 3. MAX PAIN ───────────────────────────────────────────

    def calc_max_pain(items):
        """Страйк з мінімальними виплатами для продавців опціонів"""
        strikes = set()
        for item in items:
            parts = item.get("instrument_name", "").split("-")
            if len(parts) >= 4:
                try: strikes.add(int(parts[2]))
                except ValueError: pass
        if not strikes:
            return None

        min_pain   = None
        min_strike = None
        for s in sorted(strikes):
            pain = 0
            for item in items:
                parts = item.get("instrument_name", "").split("-")
                if len(parts) < 4:
                    continue
                try: k = int(parts[2])
                except ValueError: continue
                oi = safe_float(item.get("open_interest"))
                if item["instrument_name"].endswith("-C"):
                    pain += max(0, s - k) * oi
                else:
                    pain += max(0, k - s) * oi
            if min_pain is None or pain < min_pain:
                min_pain   = pain
                min_strike = s
        return min_strike

    # Max Pain по кожній експірації
    by_expiry = defaultdict(list)
    for s in summaries:
        parts = s.get("instrument_name", "").split("-")
        if len(parts) >= 3:
            by_expiry[parts[1]].append(s)

    max_pain_by_expiry = {}
    for expiry, items in sorted(by_expiry.items()):
        mp = calc_max_pain(items)
        if mp:
            max_pain_by_expiry[expiry] = mp

    nearest_mp   = next(iter(max_pain_by_expiry.values()), None)
    mp_vs_spot   = None
    if nearest_mp and index_price:
        mp_vs_spot = round((nearest_mp - index_price) / index_price * 100, 2)

    # ── 4. TERM STRUCTURE і 25D SKEW ─────────────────────────

    now        = datetime.now(timezone.utc)
    by_days    = defaultdict(list)

    for s in summaries:
        parts = s.get("instrument_name", "").split("-")
        if len(parts) < 4:
            continue
        try:
            exp  = datetime.strptime(parts[1], "%d%b%y").replace(tzinfo=timezone.utc)
            days = max(1, (exp - now).days)
            strike = int(parts[2])
            iv     = safe_float(s.get("mark_iv"))
            greeks = s.get("greeks") or {}
            delta  = abs(safe_float(greeks.get("delta")))
        except (ValueError, IndexError):
            continue
        if iv > 0:
            by_days[days].append({
                "strike": strike,
                "iv":     iv,
                "delta":  delta,
                "type":   "C" if s["instrument_name"].endswith("-C") else "P",
            })

    term_labels = {7: "1w", 14: "2w", 30: "1m", 60: "2m", 90: "3m", 180: "6m"}
    term_structure = {}
    skew_25d       = {}

    for target, label in term_labels.items():
        if not by_days:
            continue
        closest = min(by_days.keys(), key=lambda d: abs(d - target))
        if abs(closest - target) > max(target * 0.6, 5):
            continue

        opts = by_days[closest]

        # ATM IV (найближчий до spot)
        if index_price:
            atm = sorted(opts, key=lambda o: abs(o["strike"] - index_price))[:4]
            if atm:
                term_structure[label] = round(
                    sum(o["iv"] for o in atm) / len(atm), 2
                )

        # 25D Skew
        puts_25  = [o for o in opts if o["type"] == "P" and 0.20 <= o["delta"] <= 0.35]
        calls_25 = [o for o in opts if o["type"] == "C" and 0.20 <= o["delta"] <= 0.35]
        if puts_25 and calls_25:
            p_iv = sum(o["iv"] for o in puts_25)  / len(puts_25)
            c_iv = sum(o["iv"] for o in calls_25) / len(calls_25)
            skew_25d[label] = round(p_iv - c_iv, 2)

    # ── 5. IV SURFACE ─────────────────────────────────────────
    # {strike: {term: iv}} — тільки ±40% від spot, топ-3 терміни

    iv_surface = {}
    if index_price and by_days:
        rel_strikes = sorted([
            int(k) for k in oi_by_strike
            if index_price * 0.6 <= int(k) <= index_price * 1.4
        ])[:15]

        surf_labels = {7: "1w", 30: "1m", 90: "3m"}
        for strike in rel_strikes:
            row = {}
            for target, label in surf_labels.items():
                if not by_days:
                    continue
                closest = min(by_days.keys(), key=lambda d: abs(d - target))
                if abs(closest - target) > max(target * 0.6, 5):
                    continue
                opts = [o for o in by_days[closest] if o["strike"] == strike]
                if opts:
                    row[label] = round(sum(o["iv"] for o in opts) / len(opts), 2)
            if row:
                iv_surface[str(strike)] = row

    # ── 6. GREEKS (зважені по OI) ─────────────────────────────

    def agg_greeks(items):
        total = {"delta": 0, "gamma": 0, "theta": 0, "vega": 0, "w": 0}
        for s in items:
            g  = s.get("greeks") or {}
            oi = safe_float(s.get("open_interest"))
            if oi <= 0 or not g:
                continue
            for k in ["delta", "gamma", "theta", "vega"]:
                total[k] += safe_float(g.get(k)) * oi
            total["w"] += oi
        if total["w"] == 0:
            return {}
        return {k: round(total[k] / total["w"], 6)
                for k in ["delta", "gamma", "theta", "vega"]}

    return {
        # 1. Критичні
        "put_call_ratio": {
            "by_oi":     pc_oi,
            "by_volume": pc_vol,
            "signal": (
                "fear"    if (pc_oi or 0) > 0.7 else
                "greed"   if (pc_oi or 0) < 0.4 else
                "neutral"
            ),
        },
        "max_pain": {
            "by_expiry": max_pain_by_expiry,
            "nearest":   nearest_mp,
            "vs_spot_pct": mp_vs_spot,
        },

        # 2. Стратегічні
        "oi_total": {
            "calls_contracts": round(calls_oi,  2),
            "puts_contracts":  round(puts_oi,   2),
            "total_contracts": round(total_oi,  2),
            "calls_usd":       calls_oi_usd,
            "puts_usd":        puts_oi_usd,
            "total_usd":       total_oi_usd,
        },
        "oi_by_strike":  oi_by_strike,
        "top_strikes":   top_strikes,
        "skew_25d":      skew_25d,

        # 3. Динаміка
        "volume": {
            "calls":      round(calls_vol, 2),
            "puts":       round(puts_vol,  2),
            "total":      round(total_vol, 2),
            "calls_usd":  calls_vol_usd,
            "puts_usd":   puts_vol_usd,
            "total_usd":  round((calls_vol + puts_vol) * px),
        },
        "term_structure": term_structure,

        # 4. Технічні
        "iv_surface": iv_surface,
        "greeks": {
            "calls": agg_greeks(calls),
            "puts":  agg_greeks(puts),
        },

        # Мета
        "index_price":    index_price,
        "active_options": len(summaries),
        "expiries":       len(by_expiry),
    }


# ══════════════════════════════════════════════════════════════
# SNAPSHOT
# ══════════════════════════════════════════════════════════════

async def take_snapshot(date_str: str):
    now_utc = datetime.now(timezone.utc).strftime("%H:%M UTC")
    print(f"\n{'='*58}")
    print(f"  Options Snapshot — {date_str} @ {now_utc}")
    print(f"{'='*58}")

    async with httpx.AsyncClient() as c:
        for coin in COINS:
            print(f"\n  [{coin}]", end=" ", flush=True)

            # Паралельно: summaries + DVOL
            summaries, dvol = await asyncio.gather(
                get_book_summaries(c, coin),
                get_dvol(c, coin),
            )

            if not summaries:
                print("⚠ немає даних з Deribit")
                continue

            print(f"{len(summaries)} опціонів...", end=" ", flush=True)

            # Обробка
            processed = process_options(summaries)
            if not processed:
                print("⚠ обробка не вдалась")
                continue

            # OI Change (порівняння з вчора)
            oi_change = None
            yesterday = str(dt_date.fromisoformat(date_str) - timedelta(days=1))
            prev      = await dbx_download(
                f"{DATA_DIR}/options/{coin}/{yesterday}.json"
            )
            if prev:
                prev_oi = (prev.get("oi_total") or {}).get("total_contracts", 0)
                curr_oi = processed["oi_total"]["total_contracts"]
                if prev_oi and curr_oi:
                    delta = curr_oi - prev_oi
                    oi_change = {
                        "contracts": round(delta, 2),
                        "pct":       round(delta / prev_oi * 100, 2),
                        "direction": "inflow" if delta > 0 else "outflow",
                    }

            # Документ
            doc = {
                "date":          date_str,
                "coin":          coin,
                "source":        "deribit_public",
                "snapshot_time": now_utc,

                # 1. Критичні
                "put_call_ratio":  processed["put_call_ratio"],
                "max_pain":        processed["max_pain"],
                "dvol":            dvol,

                # 2. Стратегічні
                "oi_total":        processed["oi_total"],
                "oi_by_strike":    processed["oi_by_strike"],
                "top_strikes":     processed["top_strikes"],
                "skew_25d":        processed["skew_25d"],

                # 3. Динаміка
                "oi_change":       oi_change,
                "volume":          processed["volume"],
                "term_structure":  processed["term_structure"],

                # 4. Технічні
                "iv_surface":      processed["iv_surface"],
                "greeks":          processed["greeks"],

                # Мета
                "index_price":     processed["index_price"],
                "active_options":  processed["active_options"],
                "expiries_count":  processed["expiries"],
            }

            path = f"{DATA_DIR}/options/{coin}/{date_str}.json"
            ok   = await dbx_save(path, doc)

            if ok:
                pc  = (doc["put_call_ratio"] or {}).get("by_oi", "—")
                mp  = (doc["max_pain"] or {}).get("nearest")
                dv  = (doc["dvol"] or {}).get("current", "—")
                mp_str = f"${mp:,}" if isinstance(mp, (int, float)) else "—"
                print(f"✅  P/C={pc}  MaxPain={mp_str}  DVOL={dv}")
            else:
                print("❌ Dropbox помилка")

            await asyncio.sleep(0.5)

    print(f"\n  ✓ Snapshot завершено\n")


# ══════════════════════════════════════════════════════════════
# ГОЛОВНИЙ ЦИКЛ
# ══════════════════════════════════════════════════════════════

async def run():
    print("=" * 58)
    print("  PULSY Options Collector")
    print(f"  Монети:    {', '.join(COINS)}")
    print(f"  Schedule:  {SCHEDULE_HOUR:02d}:{SCHEDULE_MINUTE:02d} UTC щодня")
    print(f"  Джерело:   Deribit Public API (без ключа)")
    print(f"  Зберігає:  Dropbox{DATA_DIR}/options/{{COIN}}/{{date}}.json")
    print("=" * 58)

    last_run = None

    # При першому запуску — snapshot одразу
    today = datetime.now(timezone.utc).date()
    print(f"\nПерший запуск — збираємо snapshot за {today}...")
    await take_snapshot(str(today))
    last_run = today

    while True:
        now   = datetime.now(timezone.utc)
        today = now.date()

        # О 08:05 UTC — snapshot за вчора
        if (now.hour == SCHEDULE_HOUR and
                now.minute >= SCHEDULE_MINUTE and
                last_run != today):
            yesterday = str(today - timedelta(days=1))
            print(f"[Scheduled] Snapshot за {yesterday}...")
            await take_snapshot(yesterday)
            last_run = today

        # Статус раз на годину (о :00)
        if now.minute == 0:
            next_run = now.replace(
                hour=SCHEDULE_HOUR,
                minute=SCHEDULE_MINUTE,
                second=0, microsecond=0,
            )
            if next_run <= now:
                next_run += timedelta(days=1)
            wait_min = int((next_run - now).total_seconds() / 60)
            print(f"[{now.strftime('%H:%M')} UTC] "
                  f"Наступний snapshot через ~{wait_min} хв | "
                  f"Останній: {last_run}")

        await asyncio.sleep(60)


if __name__ == "__main__":
    asyncio.run(run())
