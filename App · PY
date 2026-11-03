#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
🏆 محلل القطاعات v3.4 — خادم + محلل + ذاكرة + فاحص سياق العملة (/coin)

الجديد في v3.4:
  A. /coin?symbol=NEAR : حكم قطاع العملة (هل أقرانها يتحركون معها أم هي معزولة؟)
  B. أيام "لا قطاع قوي": الترتيب بالزخم حرفياً (كان يتناقض مع وسوم الترتيب)
  C. رقم النسخة في /health وفي السجل، وDEBUG_MATCH=1 لفحص مطابقة العملات

التعديلات السابقة (v3.3):

التعديلات عن v3.2:
  1. بيانات السوق العالمية الناقصة تظهر "غير متاحة" بدل رقم افتراضي (كان 50%)
  2. يوم هابط لكل القطاعات ← "لا قطاع قوي الآن" بدل ترتيب يبدو كفرص
  3. الترتيب النسبي عاد، ولا يمنح مكافأة لقطاع زخمه سالب
  4. الذاكرة: قراءة واحدة لكل دورة، تحذير واحد، وحفظ كل القطاعات (لا أفضل 6 فقط)
  5. مطابقة العملات: التطابق التام أولاً بدل البحث الجزئي العشوائي
  6. فشل CoinGecko النهائي يُسجَّل في السجل (كان صامتاً)
  7. تيليغرام: تقسيم آمن بالأسطر + إرجاع نتيجة حقيقية + توقيت UTC
  8. حماية اختيارية لـ /scan عبر SCAN_KEY
"""

import asyncio
import aiohttp
import re
import json
import os
import time
import shutil
import tempfile
import logging
import threading
import html
import statistics
import weakref
from datetime import datetime, timezone

from flask import Flask, jsonify, request

try:
    import gspread
    from google.oauth2.service_account import Credentials
except ImportError:  # الذاكرة تتعطل بأمان لو المكتبة غير مثبتة
    gspread = None
    Credentials = None

# ════════════════════════════════════
# ⚙️ الإعدادات العامة
# ════════════════════════════════════
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[logging.StreamHandler()]
)
log = logging.getLogger("sector_analyzer")

COINGECKO_URL = "https://api.coingecko.com/api/v3"
KUCOIN_URL    = "https://api.kucoin.com/api/v1"

TELEGRAM_TOKEN   = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
SEND_TELEGRAM    = bool(TELEGRAM_TOKEN and TELEGRAM_CHAT_ID)

# حماية اختيارية: لو ضبطت SCAN_KEY يصبح الرابط /scan?key=قيمتك
SCAN_KEY = os.environ.get("SCAN_KEY", "")

TOP_SECTORS_SHOW   = 6
WORST_SECTORS_SHOW = 3

CACHE_DIR      = os.environ.get("CACHE_DIR", "/tmp")
CACHE_FILE     = os.path.join(CACHE_DIR, "coins_list_cache.json")
CACHE_MAX_AGE  = 7 * 86400

ALLOWED_CATEGORIES = {
    'layer-1', 'layer-2', 'artificial-intelligence',
    'decentralized-exchange', 'gaming', 'meme-token',
    'metaverse', 'real-world-assets', 'depin',
    'decentralized-finance-defi', 'oracle', 'yield-farming',
}

MAX_RETRIES     = 4
BASE_BACKOFF    = 3.0
REQUEST_TIMEOUT = 25

# إعدادات Google Sheets (الذاكرة)
GOOGLE_CREDS_JSON = os.environ.get("GOOGLE_CREDS_JSON", "")
SHEET_ID          = os.environ.get("GOOGLE_SHEET_ID", "")
SHEET_TAB_NAME    = "sector_history"
SCOPES = ["https://www.googleapis.com/auth/spreadsheets",
          "https://www.googleapis.com/auth/drive.file"]
# نحفظ كل القطاعات في كل دورة + أسعار عملاتها، ليمكن لاحقاً التحقق من التوصيات رقمياً
HEADERS = ["timestamp", "sector", "score", "momentum", "alpha", "rank",
           "confidence", "n_coins", "coins", "btc_price", "btc_change"]
TREND_LOOKBACK = 24   # عدد القراءات الأخيرة المستخدمة في حساب الاتجاه


VERSION = "3.4.0"
DEBUG_MATCH = os.environ.get("DEBUG_MATCH") == "1"   # يطبع (اسم الصورة → الرمز) للتحقق من المطابقة

# ── فاحص سياق العملة ──
SECTOR_NAMES = {
    'layer-1': 'Layer 1 (L1)', 'layer-2': 'Layer 2 (L2)',
    'artificial-intelligence': 'Artificial Intelligence (AI)',
    'decentralized-exchange': 'Decentralized Exchange (DEX)',
    'gaming': 'Gaming (GameFi)', 'meme-token': 'Meme', 'metaverse': 'Metaverse',
    'real-world-assets': 'Real World Assets (RWA)', 'depin': 'DePIN',
    'decentralized-finance-defi': 'Decentralized Finance (DeFi)',
    'oracle': 'Oracle', 'yield-farming': 'Yield Farming',
}
MEMBERS_CACHE_FILE = os.path.join(CACHE_DIR, "sector_members.json")
MEMBERS_MAX_AGE = 24 * 3600          # عمر قاعدة الأعضاء قبل إعادة بنائها
MEMBERS_MIN_SECTORS = 6              # أقل عدد قطاعات ناجحة لقبول البناء
MEMBERS_RETRY_COOLDOWN = 600         # مهلة بين محاولات البناء (ثوانٍ)
TICKERS_TTL = 45                     # كاش أسعار KuCoin للفحص (ثوانٍ)
PEERS_MIN = 4                        # أقل عدد أقران لإصدار حكم
PEER_MIN_VOL = 50_000                # أدنى حجم 24h (USDT) لاحتساب القرين
MARKET_MIN_VOL = 100_000             # أدنى حجم لاحتساب العملة في إحصاءات السوق
STABLES = {'USDT', 'USDC', 'DAI', 'TUSD', 'FDUSD', 'USDE', 'USDD', 'USDS', 'PYUSD', 'USDP', 'BUSD', 'USTC'}
LOOKUP_TAB = "f1_checks"
LOOKUP_HEADERS = ["timestamp", "source", "symbol", "price", "coin_change", "verdict", "sector",
                  "n_peers", "peers_up", "up_ratio", "peers_median", "market_median",
                  "market_breadth", "btc_change"]


def safe_float(value, default=0.0):
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


# ════════════════════════════════════
# 🌐 طبقة الشبكة
# ════════════════════════════════════
_cg_sems = weakref.WeakKeyDictionary()   # سيمافور لكل event loop (الدورة والفحص يعملان في حلقتين)


def _get_cg_sem():
    loop = asyncio.get_running_loop()
    sem = _cg_sems.get(loop)
    if sem is None:
        sem = asyncio.Semaphore(1)
        _cg_sems[loop] = sem
    return sem


async def _get_once(session, url, params, timeout, label, attempt, retries, is_cg):
    async with session.get(url, params=params,
                           timeout=aiohttp.ClientTimeout(total=timeout)) as r:
        if r.status == 200:
            return await r.json()
        if r.status == 429:
            wait = 8.0 if is_cg else BASE_BACKOFF * (2 ** (attempt - 1))
            log.warning(f"[{label}] Rate limited (429) — الانتظار {wait:.0f}s "
                        f"(محاولة {attempt}/{retries})")
        else:
            wait = BASE_BACKOFF * attempt
            log.warning(f"[{label}] HTTP {r.status} (محاولة {attempt}/{retries})")
        await asyncio.sleep(wait)
        return None


async def fetch_json(session, url, params=None, retries=MAX_RETRIES,
                     timeout=REQUEST_TIMEOUT, label=""):
    is_cg = 'coingecko' in url.lower()
    for attempt in range(1, retries + 1):
        try:
            if is_cg:
                async with _get_cg_sem():
                    await asyncio.sleep(0.7)
                    res = await _get_once(session, url, params, timeout, label,
                                          attempt, retries, is_cg)
            else:
                res = await _get_once(session, url, params, timeout, label,
                                      attempt, retries, is_cg)
            if res is not None:
                return res
        except Exception as e:
            log.warning(f"[{label}] خطأ (محاولة {attempt}/{retries}): {e}")
            await asyncio.sleep(BASE_BACKOFF * attempt)
    log.error(f"[{label}] فشل نهائي بعد {retries} محاولات")   # كان صامتاً في v3.2
    return None


def safe_json_dump(data, filepath):
    tmp_path = None
    try:
        directory = os.path.dirname(filepath) or '.'
        os.makedirs(directory, exist_ok=True)
        with tempfile.NamedTemporaryFile('w', dir=directory, delete=False,
                                         encoding='utf-8') as tmp:
            json.dump(data, tmp, ensure_ascii=False)
            tmp_path = tmp.name
        shutil.move(tmp_path, filepath)
        return True
    except Exception as e:
        log.warning(f"فشل حفظ الكاش: {e}")
        if tmp_path and os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except OSError:
                pass
        return False


def _read_cache():
    try:
        with open(CACHE_FILE, encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return None


async def fetch_coins_list(session):
    if os.path.exists(CACHE_FILE) and time.time() - os.path.getmtime(CACHE_FILE) < CACHE_MAX_AGE:
        cached = _read_cache()
        if cached:
            return cached
    data = await fetch_json(session, f"{COINGECKO_URL}/coins/list", label="coins_list")
    if not isinstance(data, list):
        stale = _read_cache() if os.path.exists(CACHE_FILE) else None
        if stale:
            log.warning("قائمة العملات: استخدام كاش قديم كبديل")
            return stale
        return {}
    result = {c['id']: c['symbol'].upper() for c in data if 'id' in c and 'symbol' in c}
    safe_json_dump(result, CACHE_FILE)
    log.info(f"قائمة العملات: جلب جديد ({len(result)} عملة)")
    return result


async def fetch_categories(session):
    data = await fetch_json(session, f"{COINGECKO_URL}/coins/categories", label="categories")
    if not isinstance(data, list):
        return []
    return [c for c in data if c.get('id') in ALLOWED_CATEGORIES]


async def fetch_global(session):
    """يرجع None (لا قاموساً فارغاً) عند الفشل، ليعرف الكود أن البيانات غير متاحة"""
    data = await fetch_json(session, f"{COINGECKO_URL}/global", label="global")
    if isinstance(data, dict) and isinstance(data.get('data'), dict) and data['data']:
        return data['data']
    log.warning("بيانات السوق العالمية غير متاحة — لن تُعرض الهيمنة")
    return None


async def fetch_btc_simple(session):
    data = await fetch_json(
        session, f"{COINGECKO_URL}/simple/price",
        params={"ids": "bitcoin", "vs_currencies": "usd", "include_24hr_change": "true"},
        label="btc")
    if isinstance(data, dict) and isinstance(data.get('bitcoin'), dict):
        b = data['bitcoin']
        if b.get('usd') is not None and b.get('usd_24h_change') is not None:
            return {'price': safe_float(b['usd']), 'change': safe_float(b['usd_24h_change'])}
    return None


async def fetch_btc_from_kucoin(session):
    data = await fetch_json(session, f"{KUCOIN_URL}/market/stats",
                            params={"symbol": "BTC-USDT"}, label="btc_kucoin")
    if isinstance(data, dict) and data.get('code') == '200000' and data.get('data'):
        d = data['data']
        return {'price': safe_float(d.get('last')),
                'change': safe_float(d.get('changeRate')) * 100}
    return None


async def fetch_kucoin_tickers(session):
    data = await fetch_json(session, f"{KUCOIN_URL}/market/allTickers", label="kucoin_tickers")
    if not isinstance(data, dict) or data.get('code') != '200000':
        return {}
    res = {}
    for t in data['data']['ticker']:
        if t['symbol'].endswith('-USDT'):
            res[t['symbol'].replace('-USDT', '').upper()] = {
                'price': safe_float(t.get('last')),
                'change': safe_float(t.get('changeRate')) * 100,
                'volume': safe_float(t.get('volValue')),
            }
    return res


# ════════════════════════════════════
# 🧮 مطابقة العملات
# ════════════════════════════════════
_SUFFIX_RE = re.compile(r'(-logo|-token|-coin|-round|-small|-icon|-symbol|-square|-circle)')


def _clean_name(raw):
    return re.sub(r'[_0-9]+$', '', _SUFFIX_RE.sub('', raw))


def _resolve_symbol(raw_name, coin_id_to_symbol, kucoin_data):
    """
    الترتيب: 1) تطابق تام مع id في CoinGecko  2) تطابق تام مع رمز في KuCoin
             3) بحث جزئي أخير (بشرط أن يكون رمزه موجوداً في KuCoin، ويُفضَّل الأقصر)
    كان v3.2 يبدأ بالبحث الجزئي، فيلتقط أول id في القاموس يحتوي الاسم
    (مثل توكنات الرافعة) ويُسقط العملة الصحيحة بصمت.
    """
    clean = _clean_name(raw_name).lower()
    for cand in (clean, raw_name.lower()):
        if cand and cand in coin_id_to_symbol:
            return coin_id_to_symbol[cand].upper()
    if raw_name.upper() in kucoin_data:
        return raw_name.upper()
    if not clean:
        return None
    cands = [cid for cid in coin_id_to_symbol
             if len(cid) >= 4 and (clean in cid or cid in clean)
             and coin_id_to_symbol[cid].upper() in kucoin_data]
    return coin_id_to_symbol[min(cands, key=len)].upper() if cands else None


def get_top_coins_data(top_entries, kucoin_data, coin_id_to_symbol):
    seen, result = set(), []
    for entry in top_entries:
        if not isinstance(entry, str) or not entry.startswith('http'):
            continue
        raw_name = entry.rsplit('/', 1)[-1].split('?')[0].rsplit('.', 1)[0]
        if not raw_name:
            continue
        sym = _resolve_symbol(raw_name, coin_id_to_symbol, kucoin_data)
        if DEBUG_MATCH:
            log.info(f"مطابقة: {raw_name} → {sym}")
        if sym and sym in kucoin_data and sym not in seen:
            seen.add(sym)
            kd = kucoin_data[sym]
            result.append({'name': raw_name.capitalize(), 'symbol': sym,
                           'price': kd['price'], 'change': kd['change'],
                           'volume': kd['volume']})
    result.sort(key=lambda c: -c['change'])
    return result


# ════════════════════════════════════
# 🧮 حساب القطاعات (منطق v3.2 كما هو)
# ════════════════════════════════════
def calc_sector_score(cat, coins_data, btc_change):
    m   = cat.get('market_cap_change_24h', 0) or 0
    vol = cat.get('volume_24h', 0) or 0
    cap = cat.get('market_cap', 0) or 0
    s = 0

    if m > 8:    s += 35
    elif m > 5:  s += 28
    elif m > 2:  s += 18
    elif m > 0:  s += 8
    elif m > -3: pass
    else:        s -= 10

    if cap > 0:
        vr = vol / cap
        if vr > 0.25:   s += 20
        elif vr > 0.12: s += 13
        elif vr > 0.05: s += 6
        else:           s -= 3

    alpha = m - btc_change
    if alpha > 5:    s += 20
    elif alpha > 2:  s += 13
    elif alpha > 0:  s += 6
    elif alpha < -3: s -= 8

    if cap > 50e9:   s += 10
    elif cap > 10e9: s += 6
    elif cap > 1e9:  s += 2

    breadth_ratio = None
    if coins_data and len(coins_data) >= 3:
        rising = sum(1 for c in coins_data if c['change'] > 0)
        breadth_ratio = rising / len(coins_data)
        if breadth_ratio < 0.4:
            s -= 15
        elif breadth_ratio >= 0.8:
            s += 8
    return round(max(s, -25)), alpha, breadth_ratio


def get_confidence(n):
    return "🟢 موثوق" if n >= 2 else "🟡 محدود" if n == 1 else "🔴 لا عملات"


def add_cross_sectional_rank(results):
    """
    ترتيب نسبي بين القطاعات في نفس اللحظة.
    الفرق عن النسخة السابقة: لا مكافأة موجبة لقطاع زخمه سالب أو صفر،
    فلا يُكافأ "الأقل سوءاً" في يوم هابط للجميع كأنه قطاع قوي.
    """
    n = len(results)
    if not n:
        return results
    ordered = sorted(results, key=lambda x: -x['momentum'])
    top_cut = max(2, n // 3)
    bottom_start = n - max(1, n // 3) + 1
    for idx, s in enumerate(ordered):
        rank = idx + 1
        s['rank'] = rank
        s['total_sectors'] = n
        s['rs_score'] = round(100 * (n - rank) / (n - 1), 1) if n > 1 else 50.0

        if rank == 1:
            bonus, label = 15, f"🥇 #1 من {n} — الأقوى نسبياً"
        elif rank >= bottom_start:
            bonus, label = -8, f"📉 #{rank} من {n} — ضمن الأضعف نسبياً"
        elif rank <= top_cut:
            bonus, label = 8, f"📈 #{rank} من {n} — ضمن الأقوى"
        else:
            bonus, label = 0, f"➡️ #{rank} من {n} — متوسط"

        if bonus > 0 and s['momentum'] <= 0:
            bonus = 0
            label = f"➡️ #{rank} من {n} — الأقل سوءاً فقط (زخم سالب)"

        s['rank_bonus'] = bonus
        s['rank_label'] = label
        s['score'] = round(s['score'] + bonus)
    return results


def diagnose_market(global_data, btc_change):
    """لا قيم افتراضية صامتة: ما لا يُجلب يُعلَّم غير متاح، والمرحلة تُوسَم تقديرية"""
    dom = tot = None
    if global_data:
        dom = global_data.get('market_cap_percentage', {}).get('btc')
        tot = global_data.get('market_cap_change_percentage_24h_usd')
    dom = None if dom is None else safe_float(dom)
    tot = None if tot is None else safe_float(tot)
    reliable = dom is not None and tot is not None

    score = 0
    if dom is not None:
        if dom > 55:   score -= 2
        elif dom < 45: score += 2
    if btc_change > 2:    score += 1
    elif btc_change < -2: score -= 2
    if tot is not None:
        if tot > 3:    score += 2
        elif tot < -3: score -= 2

    if score >= 3:    phase, advice = "🌅 Altcoin Season — بداية", "ركّز على L1 + AI + DeFi + DePIN"
    elif score >= 1:  phase, advice = "🟡 انتقال — حذر", "انتقائي — أقوى القطاعات فقط"
    elif score >= -1: phase, advice = "⚪ محايد", "ابحث عن قطاع يتحرك وحده"
    elif score >= -3: phase, advice = "🟠 BTC Season", "ركّز على BTC + Exchange Tokens"
    else:             phase, advice = "🔴 سوق هابط", "تجنب المضاربة"
    if not reliable:
        phase += " (تقديرية)"
    return {'phase': phase, 'advice': advice,
            'btc_dominance': None if dom is None else round(dom, 1),
            'total_change': None if tot is None else round(tot, 1),
            'reliable': reliable}


# ════════════════════════════════════
# 🧠 الذاكرة الزمنية (Google Sheets)
# ════════════════════════════════════
def _open_worksheet(tab=None, headers=None, quiet=False):
    """يفتح تبويباً (الافتراضي: sector_history). quiet=True لإسكات تحذير عدم الضبط."""
    tab = tab or SHEET_TAB_NAME
    headers = headers or HEADERS
    if gspread is None:
        if not quiet:
            log.warning("مكتبة gspread غير مثبتة — الذاكرة معطلة")
        return None
    if not GOOGLE_CREDS_JSON or not SHEET_ID:
        if not quiet:
            log.warning("الذاكرة الزمنية معطلة — GOOGLE_CREDS_JSON/GOOGLE_SHEET_ID غير مضبوطين")
        return None
    try:
        creds = Credentials.from_service_account_info(json.loads(GOOGLE_CREDS_JSON), scopes=SCOPES)
        sh = gspread.authorize(creds).open_by_key(SHEET_ID)
        try:
            return sh.worksheet(tab)
        except gspread.WorksheetNotFound:
            log.info(f"إنشاء تبويب جديد: {tab}")
            ws = sh.add_worksheet(title=tab, rows=2000, cols=len(headers))
            ws.append_row(headers)
            return ws
    except Exception as e:
        log.error(f"فشل الاتصال بـ Google Sheets: {e}")
        return None


def load_history(ws):
    try:
        return ws.get_all_records()
    except Exception as e:
        log.warning(f"فشل قراءة الذاكرة: {e}")
        return []


def compute_trend(history, sector_name, current_score, lookback=TREND_LOOKBACK):
    """
    اتجاه النقاط عبر آخر القراءات (يشمل قراءة هذه الدورة).
    المقارنة بفارق نقاط (±5) لا بنسبة، لأن النقاط قد تكون سالبة.
    يرجع None إذا لم تتوفر 3 قراءات على الأقل.
    """
    scores = [safe_float(r.get('score')) for r in history if r.get('sector') == sector_name]
    scores = scores[-(lookback - 1):] + [current_score]
    if len(scores) < 3:
        return None
    mid = len(scores) // 2
    first = sum(scores[:mid]) / mid
    second = sum(scores[mid:]) / (len(scores) - mid)
    delta = second - first
    trend = "🚀 يتحسّن" if delta >= 5 else "📉 يتراجع" if delta <= -5 else "➡️ مستقر"
    consecutive = 0
    for sc in reversed(scores):
        if sc > 0:
            consecutive += 1
        else:
            break
    return {'trend': trend, 'consecutive': consecutive, 'points': len(scores)}


def save_snapshot(ws, results, btc):
    now = datetime.now(timezone.utc).isoformat()
    rows = []
    for s in results:
        coins = ";".join(f"{c['symbol']}={c['price']}" for c in s['coins_data'])
        rows.append([now, s['name'], s['score'], s['momentum'], s['alpha'],
                     s.get('rank', ''), s['confidence'], s['n_coins'], coins,
                     btc['price'], round(btc['change'], 2)])
    try:
        ws.append_rows(rows, value_input_option="USER_ENTERED")
        log.info(f"الذاكرة: حُفظت {len(rows)} صفاً ✅")
        return True
    except Exception as e:
        log.error(f"فشل حفظ الذاكرة: {e}")
        return False


# ════════════════════════════════════
# 📱 تيليغرام والتنسيق
# ════════════════════════════════════
async def send_telegram(session, message):
    if not SEND_TELEGRAM:
        log.info("تيليغرام غير مفعّل (لا توكن) — تخطي الإرسال")
        return False
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    # تقسيم بالأسطر حتى لا ينقطع وسم HTML في منتصفه
    chunks, cur = [], ""
    for line in message.split('\n'):
        if len(cur) + len(line) + 1 > 3900:
            chunks.append(cur)
            cur = line
        else:
            cur = f"{cur}\n{line}" if cur else line
    if cur:
        chunks.append(cur)

    all_ok = True
    for i, chunk in enumerate(chunks, 1):
        sent = False
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                async with session.post(
                    url, json={"chat_id": TELEGRAM_CHAT_ID, "text": chunk, "parse_mode": "HTML"},
                    timeout=aiohttp.ClientTimeout(total=15)
                ) as r:
                    res = await r.json()
                    if res.get('ok'):
                        sent = True
                        break
                    log.warning(f"تيليغرام رفض الرسالة: {res.get('description')}")
                    break
            except Exception as e:
                log.warning(f"تيليغرام: محاولة {attempt}/{MAX_RETRIES} فشلت — {e}")
                await asyncio.sleep(BASE_BACKOFF * attempt)
        log.info(f"تيليغرام: جزء {i}/{len(chunks)} {'✅' if sent else '❌'}")
        all_ok = all_ok and sent
        await asyncio.sleep(0.5)
    return all_ok


def format_telegram_message(market, best, worst, no_strong):
    esc = html.escape
    now = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')
    sep = '═' * 30
    dom = market.get('btc_dominance')
    dom_txt = f"{dom:.1f}%" if dom is not None else "غير متاحة ⚠️"

    lines = [f"🏆 <b>محلل القطاعات — {now}</b>", sep,
             f"🧠 <b>المرحلة:</b> {esc(market['phase'])}",
             f"💡 {esc(market['advice'])}",
             f"💰 BTC Dominance: {dom_txt}"]
    if not market.get('reliable', True):
        lines.append("⚠️ بيانات السوق العالمية ناقصة — المرحلة تقديرية")
    lines.append(sep)

    if no_strong:
        lines += ["\n⛔ <b>لا قطاع قوي الآن</b> — كل القطاعات بزخم سالب",
                  "الترتيب أدناه للأقل سوءاً فقط، وليس فرصاً.",
                  "\n📊 <b>الأقل سوءاً:</b>"]
    else:
        lines.append("\n🔥 <b>أفضل القطاعات:</b>")

    for i, s in enumerate(best, 1):
        medal = f"{i}." if no_strong else (["🥇", "🥈", "🥉"][i - 1] if i <= 3 else f"{i}️⃣")
        lines.append(f"\n{medal} <b>{esc(s['name'])}</b> — {s['score']} نقطة ({s['confidence']})")
        lines.append(f"📈 {s['momentum']:+.1f}% | Alpha: {s['alpha']:+.1f}%")
        if s.get('rank_label'):
            lines.append(f"🏆 {s['rank_label']}")
        if s.get('trend'):
            lines.append(f"🧠 الاتجاه: {s['trend']} ({s['consecutive']} دورة موجبة متتالية)")
        if s.get('breadth') is not None and s['breadth'] < 0.4:
            lines.append("⚠️ تحذير: عملة واحدة قد ترفع المتوسط")
        for coin in s['coins_data']:
            lines.append(f"  ➤ {esc(coin['symbol'])}: {coin['change']:+.2f}%")

    lines.append(f"\n{'─' * 30}\n⚠️ <b>تجنب:</b>")
    for s in worst:
        lines.append(f"❌ {esc(s['name'])}: {s['momentum']:+.1f}%")
    lines.append(f"\n{sep}\n⚠️ تحليل تقني فقط — ليس نصيحة مالية")
    return "\n".join(lines)


# ════════════════════════════════════
# 🚀 التشغيل الرئيسي
# ════════════════════════════════════
async def run_analysis():
    log.info("=" * 60)
    log.info(f"🏆 محلل القطاعات v{VERSION} — بدء التشغيل")
    log.info("=" * 60)

    async with aiohttp.ClientSession() as session:
        # جلب متسلسل لتفادي 429 من CoinGecko
        categories = await fetch_categories(session)
        global_data = await fetch_global(session)
        btc = await fetch_btc_simple(session)
        kucoin_data = await fetch_kucoin_tickers(session)
        coins_list = await fetch_coins_list(session)

        if not categories:
            log.error("لا قطاعات من CoinGecko — إيقاف الدورة")
            return {"status": "failed", "reason": "no_categories"}
        if not kucoin_data:
            log.error("لا بيانات من KuCoin — إيقاف الدورة")
            return {"status": "failed", "reason": "no_kucoin_data"}
        if btc is None:
            log.warning("سعر BTC من CoinGecko فشل — التحويل لـ KuCoin")
            btc = await fetch_btc_from_kucoin(session)
        if btc is None:
            log.error("لا سعر BTC من أي مصدر — إيقاف الدورة")
            return {"status": "failed", "reason": "no_btc"}

        btc_change = btc['change']
        market = diagnose_market(global_data, btc_change)
        dom_txt = "غير متاحة" if market['btc_dominance'] is None else f"{market['btc_dominance']}%"
        log.info(f"{len(categories)} قطاع | {len(kucoin_data)} عملة | "
                 f"BTC {btc_change:+.2f}% | الهيمنة: {dom_txt} | {market['phase']}")

        results = []
        for cat in categories:
            if (cat.get('market_cap', 0) or 0) < 10_000_000:
                continue
            coins_data = get_top_coins_data(cat.get('top_3_coins', []) or [],
                                            kucoin_data, coins_list)
            score, alpha, breadth = calc_sector_score(cat, coins_data, btc_change)
            results.append({
                'name': cat.get('name', '?'), 'score': score,
                'momentum': round(cat.get('market_cap_change_24h', 0) or 0, 2),
                'alpha': round(alpha, 2), 'breadth': breadth,
                'coins_data': coins_data, 'n_coins': len(coins_data),
                'confidence': get_confidence(len(coins_data)),
            })
        if not results:
            log.warning("لا قطاعات تجاوزت الحد الأدنى للقيمة السوقية")
            return {"status": "empty"}

        results = add_cross_sectional_rank(results)
        no_strong = max(r['momentum'] for r in results) <= 0
        if no_strong:
            # يوم هابط للجميع: "الأقل سوءاً" = الأعلى زخماً حرفياً (النقاط تتأثر بحجم
            # القطاع وسيولته فكانت تناقض وسوم الترتيب في هذه الأيام)
            ordered = sorted(results, key=lambda x: (-x['momentum'], -x['alpha']))
            best = ordered[:TOP_SECTORS_SHOW]
            worst = ordered[::-1][:WORST_SECTORS_SHOW]
        else:
            results.sort(key=lambda x: -x['score'])
            best = results[:TOP_SECTORS_SHOW]
            worst = sorted(results, key=lambda x: x['score'])[:WORST_SECTORS_SHOW]

        # الذاكرة: فتح واحد، قراءة واحدة، ثم حفظ كل القطاعات
        ws = await asyncio.to_thread(_open_worksheet)
        history = await asyncio.to_thread(load_history, ws) if ws else []
        for s in best:
            t = compute_trend(history, s['name'], s['score']) if ws else None
            s['trend'] = t['trend'] if t else None
            s['consecutive'] = t['consecutive'] if t else 0
        memory_saved = await asyncio.to_thread(save_snapshot, ws, results, btc) if ws else False

        for i, s in enumerate(best, 1):
            log.info(f"  {i}. {s['name']} — {s['score']} نقطة | {s['momentum']:+.1f}% | "
                     f"{s['confidence']} | {s.get('rank_label', '')}")
        if no_strong:
            log.info("⛔ لا قطاع قوي الآن — كل القطاعات بزخم سالب")

        msg = format_telegram_message(market, best, worst, no_strong)
        telegram_ok = await send_telegram(session, msg)

        return {"status": "success", "market": market, "no_strong_sector": no_strong,
                "best_sectors": [{"name": s["name"], "score": s["score"]} for s in best],
                "memory_saved": memory_saved, "telegram_sent": telegram_ok}


# ════════════════════════════════════
# 🧭 فاحص سياق العملة
# السؤال: هذه العملة تحركت — هل أقرانها في قطاعها يتحركون معها، أم هي معزولة؟
# المقياس الأساسي: نسبة الأقران الصاعدين *دون العملة نفسها* (حتى لا ترفع العملة
# المنفجرة متوسط قطاعها بنفسها فتوهمنا أن القطاع قوي).
# العتبات أدناه تقديرية وغير مُثبتة بعد — لذلك يُسجَّل كل فحص للمراجعة لاحقاً.
# ════════════════════════════════════
_members = None                    # {"built_at": ts, "sectors": {id: {"name":..., "symbols":[...]}}}
_members_lock = threading.Lock()   # يمنع بناءين متزامنين
_last_build_try = 0.0
_tickers_cache = {"t": 0.0, "data": None}
_tickers_lock = threading.Lock()

VERDICT_LABELS = {
    "sector_move":  "🟢 حركة قطاع",
    "partial":      "🟡 دعم جزئي",
    "weak":         "🟠 قطاع ضعيف",
    "isolated":     "🔴 ضخ معزول",
    "insufficient": "⚪ عيّنة صغيرة",
}
VERDICT_PRIORITY = {"sector_move": 4, "partial": 3, "weak": 2, "isolated": 1, "insufficient": 0}


def _load_members_from_disk():
    try:
        with open(MEMBERS_CACHE_FILE, encoding='utf-8') as f:
            d = json.load(f)
        if isinstance(d, dict) and d.get('sectors') and d.get('built_at'):
            return d
    except Exception:
        pass
    return None


def get_members():
    """يرجع قاعدة القطاعات المتاحة (قد تكون قديمة) أو None. لا يبني شيئاً."""
    global _members
    if _members is None:
        _members = _load_members_from_disk()
    return _members


def _members_age():
    m = get_members()
    return None if not m else time.time() - m['built_at']


async def _build_members_async():
    sectors = {}
    async with aiohttp.ClientSession() as session:
        for cid, name in SECTOR_NAMES.items():
            data = await fetch_json(
                session, f"{COINGECKO_URL}/coins/markets",
                params={"vs_currency": "usd", "category": cid, "order": "market_cap_desc",
                        "per_page": 250, "page": 1},
                label=f"members:{cid}")
            if isinstance(data, list) and data:
                syms = list(dict.fromkeys(str(c['symbol']).upper() for c in data if c.get('symbol')))
                sectors[cid] = {"name": name, "symbols": syms}
    return sectors


def _build_members_thread():
    global _members
    if not _members_lock.acquire(blocking=False):
        return
    try:
        log.info("بناء قاعدة أعضاء القطاعات من CoinGecko...")
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            fresh = loop.run_until_complete(_build_members_async())
        finally:
            loop.close()
        old = (get_members() or {}).get('sectors', {})
        missing = [c for c in SECTOR_NAMES if c not in fresh]
        if len(fresh) >= MEMBERS_MIN_SECTORS:
            _members = {"built_at": time.time(), "sectors": {**old, **fresh}}  # ما فشل نُبقي نسخته القديمة
            safe_json_dump(_members, MEMBERS_CACHE_FILE)
            log.info(f"قاعدة القطاعات جاهزة: {len(fresh)} قطاع جديد"
                     + (f" | فشل جلب: {missing}" if missing else ""))
        else:
            log.error(f"فشل بناء قاعدة القطاعات (نجح {len(fresh)} فقط) — ستُعاد المحاولة لاحقاً")
    except Exception as e:
        log.error(f"خطأ في بناء قاعدة القطاعات: {e}", exc_info=True)
    finally:
        _members_lock.release()


def ensure_members():
    """
    يرجع True إن وُجدت قاعدة قابلة للاستخدام. إن كانت مفقودة أو عمرها > 24 ساعة
    يبدأ البناء في الخلفية (مع مهلة 10 دقائق بين المحاولات حتى لا نضغط CoinGecko).
    """
    global _last_build_try
    m = get_members()
    age = _members_age()
    if (m is None or age is None or age > MEMBERS_MAX_AGE) \
            and time.time() - _last_build_try > MEMBERS_RETRY_COOLDOWN:
        _last_build_try = time.time()
        threading.Thread(target=_build_members_thread, daemon=True).start()
    return m is not None


async def get_tickers_cached():
    with _tickers_lock:
        if _tickers_cache["data"] and time.time() - _tickers_cache["t"] < TICKERS_TTL:
            return _tickers_cache["data"]
    async with aiohttp.ClientSession() as session:
        data = await fetch_kucoin_tickers(session)
    if data:
        with _tickers_lock:
            _tickers_cache.update(t=time.time(), data=data)
    return data


def classify_sector(n, up_ratio, rel, coin_change):
    if n < PEERS_MIN:
        return "insufficient"
    if up_ratio >= 0.6 and rel >= 1.5:
        return "sector_move"
    if up_ratio >= 0.5 and rel >= 0:
        return "partial"
    if up_ratio < 0.4 and coin_change >= 5:
        return "isolated"
    return "weak"


def build_line(best, market):
    if best is None:
        return "🧭 غير مصنّف في القطاعات المتابَعة — لا حكم من السياق"
    name = html.escape(best['sector'], quote=False)
    if best['verdict'] == 'insufficient':
        line = f"🧭 {name}: {best['label']} ({best['n_peers']} أقران فقط) — لا حكم"
    else:
        mm = market['median_change'] if market['median_change'] is not None else 0.0
        line = (f"🧭 {name}: {best['label']} — {best['peers_up']}/{best['n_peers']} من أقرانه صاعدة "
                f"| وسيطهم {best['peers_median']:+.1f}% مقابل السوق {mm:+.1f}%")
    btc = market.get('btc_change')
    if btc is not None and btc <= -3:
        line += f" | ⚠️ BTC {btc:+.1f}%"
    return line


def evaluate_coin(symbol, tickers, members):
    """دالة نقية (بلا شبكة): ترجع None إن لم تكن العملة على KuCoin."""
    t = tickers.get(symbol)
    if not t:
        return None
    coin_change = t['change']

    liquid = [v['change'] for s, v in tickers.items()
              if v['volume'] >= MARKET_MIN_VOL and s not in STABLES]
    market = {"n": len(liquid), "breadth": None, "median_change": None,
              "btc_change": (tickers.get('BTC') or {}).get('change')}
    if liquid:
        market["breadth"] = round(sum(1 for c in liquid if c > 0) / len(liquid), 3)
        market["median_change"] = round(statistics.median(liquid), 2)
    mkt_med = market["median_change"] or 0.0

    sectors = []
    for cid, sec in members['sectors'].items():
        if symbol not in sec['symbols']:
            continue
        peers = [s for s in dict.fromkeys(sec['symbols'])
                 if s != symbol and s in tickers and s not in STABLES
                 and tickers[s]['volume'] >= PEER_MIN_VOL]
        n = len(peers)
        ups = sum(1 for s in peers if tickers[s]['change'] > 0)
        up_ratio = ups / n if n else 0.0
        med = statistics.median([tickers[s]['change'] for s in peers]) if n else 0.0
        rel = med - mkt_med
        verdict = classify_sector(n, up_ratio, rel, coin_change)
        sectors.append({
            "id": cid, "sector": sec['name'], "verdict": verdict,
            "label": VERDICT_LABELS[verdict], "n_peers": n, "peers_up": ups,
            "up_ratio": round(up_ratio, 3), "peers_median": round(med, 2),
            "rel_to_market": round(rel, 2)})

    best = None
    if sectors:
        best = max(sectors, key=lambda s: (VERDICT_PRIORITY[s['verdict']],
                                           s['up_ratio'] if s['verdict'] != 'insufficient' else 0,
                                           s['n_peers']))
    return {"status": "ok", "symbol": symbol, "price": t['price'],
            "coin_change": round(coin_change, 2), "market": market,
            "best": best, "sectors": sectors, "line": build_line(best, market)}


def _log_lookup(p, source):
    """يسجّل كل فحص في تبويب f1_checks (إن كانت Sheets مضبوطة) للمراجعة لاحقاً."""
    try:
        ws = _open_worksheet(LOOKUP_TAB, LOOKUP_HEADERS, quiet=True)
        if not ws:
            return
        b = p.get('best') or {}
        mk = p['market']
        ws.append_row([datetime.now(timezone.utc).isoformat(), source, p['symbol'], p['price'],
                       p['coin_change'], b.get('verdict', ''), b.get('sector', ''),
                       b.get('n_peers', ''), b.get('peers_up', ''), b.get('up_ratio', ''),
                       b.get('peers_median', ''), mk.get('median_change'), mk.get('breadth'),
                       mk.get('btc_change')], value_input_option="USER_ENTERED")
    except Exception as e:
        log.warning(f"فشل تسجيل الفحص: {e}")


def _page(body, title="فاحص سياق العملة"):
    return ("<!doctype html><html lang='ar' dir='rtl'><head><meta charset='utf-8'>"
            "<meta name='viewport' content='width=device-width,initial-scale=1'>"
            "<meta name='color-scheme' content='light dark'>"
            f"<title>{html.escape(title)}</title>"
            "<style>body{font-family:system-ui,sans-serif;max-width:640px;margin:0 auto;"
            "padding:16px;line-height:1.8}.v{font-size:1.2rem;font-weight:700;margin:12px 0}"
            "table{width:100%;border-collapse:collapse}td,th{padding:6px;"
            "border-bottom:1px solid #8884;text-align:right}"
            "input,button{font-size:1rem;padding:8px}small{opacity:.75}</style></head>"
            f"<body>{body}</body></html>")


def _form_html(key=""):
    k = f"<input type='hidden' name='key' value='{html.escape(key)}'>" if key else ""
    return ("<form method='get' action='/coin'><input name='symbol' placeholder='رمز العملة مثل NEAR' "
            f"autocapitalize='characters' required> {k}<button>افحص</button></form>")


def _result_html(p, key=""):
    esc = html.escape
    mk = p['market']
    rows = "".join(
        f"<tr><td>{esc(s['sector'])}</td><td>{s['label']}</td>"
        f"<td>{s['peers_up']}/{s['n_peers']}</td><td>{s['peers_median']:+.1f}%</td></tr>"
        for s in p['sectors'])
    table = ("<table><tr><th>القطاع</th><th>الحكم</th><th>أقران صاعدة</th><th>وسيطهم</th></tr>"
             f"{rows}</table>") if rows else ""
    mkt = ""
    if mk['median_change'] is not None:
        mkt = (f"<p>السوق (عملات KuCoin السائلة): {mk['breadth'] * 100:.0f}% صاعدة "
               f"| وسيط التغيّر {mk['median_change']:+.1f}%</p>")
    body = (f"<h2>{esc(p['symbol'])} <small>{p['coin_change']:+.1f}% — السعر {p['price']}</small></h2>"
            f"<div class='v'>{p['line']}</div>{table}{mkt}"
            "<p><small>كيف تقرأ: 🟢 أقران قطاعها يتحركون معها (الحركة مدعومة). 🔴 هي صاعدة وأقرانها لا "
            "(ضخ معزول). الحكم تقديري بعتبات لم تُختبر بعد، ولا يغني عن مراجعتك للرسم والأخبار.</small></p>"
            f"{_form_html(key)}")
    return _page(body, f"{p['symbol']} — فاحص السياق")


# ════════════════════════════════════
# 🌐 خادم Flask + خيط الخلفية
# ════════════════════════════════════
app = Flask(__name__)
_scan_lock = threading.Lock()
_last_run_info = {"status": "never_run", "started_at": None,
                  "finished_at": None, "duration_seconds": None}


def _authorized():
    return (not SCAN_KEY) or request.args.get("key") == SCAN_KEY


def _run_analysis_in_thread():
    started = datetime.now(timezone.utc)
    _last_run_info.update(status="running", started_at=started.isoformat(),
                          finished_at=None, duration_seconds=None)
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            result = loop.run_until_complete(run_analysis())
        finally:
            loop.close()
        finished = datetime.now(timezone.utc)
        _last_run_info.update(status=result.get("status", "unknown"),
                              finished_at=finished.isoformat(),
                              duration_seconds=round((finished - started).total_seconds(), 1))
        log.info(f"✅ اكتمل في {_last_run_info['duration_seconds']} ثانية")
    except Exception as e:
        log.error(f"❌ خطأ غير متوقع: {e}", exc_info=True)
        _last_run_info["status"] = "error"
    finally:
        _scan_lock.release()


@app.route('/')
@app.route('/health')
def health():
    ready = ensure_members()   # يوقظ بناء قاعدة القطاعات في الخلفية عند الحاجة
    age = _members_age()
    return jsonify({"status": "alive", "version": VERSION,
                    "scan_running": _scan_lock.locked(),
                    "members_ready": ready,
                    "members_age_hours": None if age is None else round(age / 3600, 1),
                    "last_scan": _last_run_info}), 200


@app.route('/scan')
def trigger_scan():
    if not _authorized():
        return jsonify({"status": "forbidden"}), 403
    if not _scan_lock.acquire(blocking=False):
        return jsonify({"status": "already_running"}), 429
    try:
        threading.Thread(target=_run_analysis_in_thread, daemon=True).start()
    except Exception:
        _scan_lock.release()
        raise
    return jsonify({"status": "started"}), 200


@app.route('/coin')
def coin_lookup():
    if not _authorized():
        return jsonify({"status": "forbidden"}), 403
    key = request.args.get("key", "")
    want_json = request.args.get("format") == "json"
    symbol = (request.args.get("symbol") or "").strip().upper().split('/')[0]

    def reply(payload, code):
        if want_json:
            return jsonify(payload), code
        if payload.get("status") == "ok":
            return _result_html(payload, key), code
        msgs = {"building": "⏳ قاعدة القطاعات قيد التحضير (تحدث بعد نوم السيرفر). أعد المحاولة بعد دقيقة.",
                "not_found": f"لا توجد عملة بهذا الرمز على KuCoin: {html.escape(symbol)}",
                "kucoin_unavailable": "تعذّر جلب أسعار KuCoin الآن. أعد المحاولة.",
                "missing_symbol": "اكتب رمز العملة."}
        return _page(f"<p>{msgs.get(payload.get('status'), 'خطأ')}</p>{_form_html(key)}"), code

    if not symbol:
        if want_json:
            return jsonify({"status": "missing_symbol"}), 400
        return _page("<h2>🧭 فاحص سياق العملة</h2>"
                     "<p>اكتب رمز عملة خرجت من F1 لتعرف: هل قطاعها يتحرك معها؟</p>"
                     f"{_form_html(key)}"), 200

    if not ensure_members():
        return reply({"status": "building"}, 202)
    try:
        tickers = asyncio.run(get_tickers_cached())
    except Exception as e:
        log.error(f"فشل جلب أسعار KuCoin للفحص: {e}")
        tickers = None
    if not tickers:
        return reply({"status": "kucoin_unavailable"}, 503)

    payload = evaluate_coin(symbol, tickers, get_members())
    if payload is None:
        return reply({"status": "not_found", "symbol": symbol}, 404)
    age = _members_age()
    payload["members_age_hours"] = None if age is None else round(age / 3600, 1)
    threading.Thread(target=_log_lookup, args=(payload, request.args.get("src", "manual")),
                     daemon=True).start()
    return reply(payload, 200)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port, threaded=True)
