"""
BYTETRACK - DATA GENERATOR (clean-pool version)

Design
------
* Parents (restaurants, customers, agents, orders) remember which IDs they produced
  *cleanly* (every row carrying that ID passes the cleaner's validation).
* Children (orders, items, payments, complaints) take their foreign keys from those
  clean pools, plus a small, explicit "phantom" share that is a deliberate orphan.
  => Bad% = garbage we inject on purpose. No hidden cascade.
* Duplicates: every 10th row re-uses the previous row's ID (PK collision) + 2% exact
  copies + 2% "variant" copies (only non-validated fields differ), ~13.5% dupes.
* Every table prints / saves the EXPECTED Read | Dupes | Good | Bad so you can compare
  with the cleaner's logs.  (expected_summary.json in the bronze folder)

Tune everything in the RATES block.  Set BRONZE_DIR / SCALE env vars to test elsewhere.
"""
import os, gc, io, json, random, copy
import pandas as pd
from faker import Faker
from datetime import datetime, timedelta

fake = Faker('en_IN')
Faker.seed(42)
random.seed(42)

OUTPUT_DIR = os.environ.get("BRONZE_DIR", "/opt/airflow/data/bronze")
SCALE = float(os.environ.get("SCALE", "1.0"))
os.makedirs(OUTPUT_DIR, exist_ok=True)

N_REST  = int(7500   * SCALE)
N_CUST  = int(150000 * SCALE)
N_AGENT = int(15000  * SCALE)
N_ORD   = int(300000 * SCALE)
N_ITEM  = int(300000 * SCALE)
N_PAY   = int(285000 * SCALE)
N_COMP  = int(15000  * SCALE)

# --------------------------------------------------------------------------------------
# RATES  (probability per row).  A "dirty" row is one the cleaner will reject.
# Lists are mutually exclusive buckets: [(kind, probability), ...]
# --------------------------------------------------------------------------------------
RATES = {
    'dup_exact':   0.02,
    'dup_variant': 0.02,
    'schema_drift_orders': 0.02,       # extra trailing column on some order lines (kept by loader)

    'restaurants': {'rating': 0.03, 'prep': 0.03, 'commission': 0.02},
    'customers':   {'future_signup': 0.03, 'orders_bad': 0.05},
    'agents':      {'rating': 0.03, 'deliveries': 0.025, 'future_join': 0.025},

    'orders': {
        'cust':   [('null', 0.015), ('empty', 0.010), ('phantom', 0.005)],
        'rest':   [('null', 0.025), ('phantom', 0.005)],
        'agent':  [('null_ok', 0.10), ('phantom', 0.005)],      # null agent is VALID (unassigned)
        'ts':     [('extreme', 0.010), ('future', 0.015)],
        'status': [('invalid', 0.015), ('empty', 0.010), ('null', 0.020)],
        'amount': [('neg', 0.020), ('str', 0.010)],
        'forced_cancel': 0.010,        # cancelled with non-zero amount
        'delivery_bad':  0.037,
        'lowercase_id':  0.03,         # 'ord_0000123' -> fixed by upper(trim()) in cleaner (NOT an error)
    },
    'items': {
        'order': [('null', 0.035), ('phantom', 0.035)],
        'qty_neg': 0.04, 'price_neg': 0.04, 'discount_neg': 0.047,
    },
    'payments': {
        'order':  [('null', 0.03), ('phantom', 0.035)],
        'amount_neg': 0.03, 'method_invalid': 0.025,
        'status': [('null', 0.02), ('invalid', 0.02)],
        'future_ts': 0.037,
    },
    'complaints': {
        'order':    [('null', 0.02), ('phantom', 0.02)],
        'customer': [('null', 0.015), ('phantom', 0.015)],
        'category': [('null', 0.02), ('invalid', 0.02)],
        'description': [('null', 0.015), ('empty', 0.01)],
        'future_ts': 0.015, 'resolved_null': 0.02,
    },
}

# --------------------------------------------------------------------------------------
STATUS_VALUES        = ['delivered', 'cancelled', 'pending', 'refunded']
INVALID_STATUS       = ['undelivered', 'in_progress', 'shipped', 'na']
PAYMENT_METHODS      = ['UPI', 'Card', 'Cash', 'Wallet']
INVALID_PAYMENT      = ['Bitcoin', 'PayPal', 'Crypto']
TIERS                = ['Bronze', 'Silver', 'Gold']
INVALID_TIERS        = ['Platinum', 'Diamond', 'Premium']
VEHICLE_TYPES        = ['Bike', 'Scooter', 'Car']
MISSPELLED_VEHICLES  = ['Bikee', 'Scootr', 'Carr']
CUISINE_TYPES        = ['North Indian', 'South Indian', 'Chinese', 'Italian', 'Fast Food', 'Biryani', 'Pizza', 'Continental']
MISSPELLED_CUISINE   = ['Nrth Indian', 'Sout Inian', 'Chineese', 'Italan', 'Birani', 'Pia']
CITY_NAMES           = ['Bangalore', 'Mumbai', 'Chennai', 'Hyderabad', 'Kolkata', 'Delhi', 'Pune', 'Ahmedabad']
MISSPELLED_CITIES    = ['Bnglr', 'Mum', 'Chenai', 'Hydrabad', 'Kolkta', 'Blore']
COMPLAINT_CATEGORIES = ['Delivery Delay', 'Wrong Item', 'Quality Issue', 'Payment Problem']
INVALID_COMPLAINT    = ['Billing', 'Technical', 'General']
ZONES                = ['North', 'South', 'East', 'West', 'Central']
GATEWAYS             = ['Razorpay', 'Paytm', 'PhonePe', 'GooglePay', 'Stripe']
COMPLAINT_SUBCATS    = {
    'Delivery Delay' : ['Late arrival', 'Agent cancelled late', 'Wrong address'],
    'Wrong Item'     : ['Wrong dish', 'Missing item', 'Extra item'],
    'Quality Issue'  : ['Food cold', 'Bad taste', 'Poor packaging'],
    'Payment Problem': ['Payment failed', 'Wrong amount', 'Refund issue'],
}

# --------------------------------------------------------------------------------------
# helpers
# --------------------------------------------------------------------------------------
def chance(p):
    return random.random() < p

def pick_kind(spec):
    """spec = [(kind, prob), ...] mutually exclusive. Returns kind or None."""
    r, acc = random.random(), 0.0
    for kind, p in spec:
        acc += p
        if r < acc:
            return kind
    return None

def row_id(prefix, i, width):
    """Every 10th row (after the first few) re-uses the previous row's ID -> PK collision."""
    if i > 4 and i % 10 == 0:
        return f"{prefix}_{i-4:0{width}d}"
    return f"{prefix}_{i+1:0{width}d}"

def is_collision(i):
    return i > 4 and i % 10 == 0

def pad(x):
    """2% of IDs get leading/trailing spaces (cleaner trims them)."""
    return f" {x} " if (x and chance(0.02)) else x

class Tracker:
    """Per canonical ID: [clean_rows, dirty_rows].  Lets us (a) build clean FK pools and
    (b) print the EXPECTED Read/Dupes/Good/Bad for each table."""
    def __init__(self):
        self.state = {}
    def add(self, cid, dirty):
        s = self.state.setdefault(cid, [0, 0])
        s[1 if dirty else 0] += 1
    def clean_ids(self):
        return [k for k, (c, d) in self.state.items() if d == 0]
    def summary(self, name, rows_written):
        good = bad = mixed = 0
        for c, d in self.state.values():
            if d == 0: good += 1
            elif c == 0: bad += 1
            else: mixed += 1
        return {'table': name, 'read': rows_written, 'dupes': rows_written - len(self.state),
                'good': good, 'bad': bad, 'mixed': mixed}

def emit(data, tracker, cid, row, dirty, variant_fn):
    """Append row (+ optional exact copy / variant copy).  Variants only change fields the
    cleaner does not validate, so every copy of an ID has the same clean/dirty status."""
    if chance(RATES['dup_exact']):
        data.append(copy.copy(row)); tracker.add(cid, dirty)
    if chance(RATES['dup_variant']):
        data.append(variant_fn(copy.copy(row))); tracker.add(cid, dirty)
    data.append(row); tracker.add(cid, dirty)

SUMMARIES = []

def phantom(prefix, base_n, width):
    return f"{prefix}_{random.randint(base_n + 1, 999999):0{width}d}"

# --------------------------------------------------------------------------------------
# PARENTS
# --------------------------------------------------------------------------------------
def generate_restaurants(n):
    print(f"\n Generating {n:,} restaurants...")
    R = RATES['restaurants']; tr = Tracker(); data = []
    for i in range(n):
        cid = row_id("RES", i, 5); dirty = False
        name = fake.company()
        r = random.random()
        city = None if r < 0.05 else random.choice(MISSPELLED_CITIES) if r < 0.15 else random.choice(CITY_NAMES)
        zone = random.choice(ZONES)
        r = random.random()
        cuisine = None if r < 0.05 else '' if r < 0.07 else random.choice(MISSPELLED_CUISINE) if r < 0.17 else random.choice(CUISINE_TYPES)
        if chance(R['rating']):
            avg_rating = round(random.uniform(5.1, 10.0), 1) if chance(0.5) else round(random.uniform(-5.0, -0.1), 1); dirty = True
        else:
            avg_rating = round(random.uniform(1.0, 5.0), 1)
        if chance(R['prep']):
            prep = random.randint(121, 300) if chance(0.5) else random.randint(-30, -1); dirty = True
        else:
            prep = random.randint(10, 60)
        tier = random.choice(TIERS)
        join_date = fake.date_between(start_date='-730d', end_date='today')
        is_active = random.choice([True, False])
        if chance(R['commission']):
            commission = round(random.uniform(-10, -1), 2); dirty = True
        else:
            commission = round(random.uniform(5, 30), 2)
        row = [pad(cid), name, city, zone, cuisine, avg_rating, prep, tier, join_date, is_active, commission]
        def variant(r_):
            r_[1] = fake.company(); r_[7] = random.choice(TIERS); return r_
        emit(data, tr, cid, row, dirty, variant)
    cols = ['restaurant_id','name','city','zone','cuisine_type','avg_rating','avg_prep_time_mins','tier','join_date','is_active','commission_pct']
    df = pd.DataFrame(data, columns=cols)
    path = f"{OUTPUT_DIR}/stg_restaurants_raw.csv"; df.to_csv(path, index=False)
    print(f"Saved: {len(df):,} rows -> {path}")
    SUMMARIES.append(tr.summary('stg_restaurants_raw', len(df)))
    del df, data; gc.collect()
    return tr.clean_ids()

def generate_customers(n):
    print(f"\n Generating {n:,} customers...")
    R = RATES['customers']; tr = Tracker()
    cols = ['customer_id','name','phone','email','city','signup_date','loyalty_tier','total_orders']
    path = f"{OUTPUT_DIR}/stg_customers_raw.csv"
    CHUNK, total, first = 10000, 0, True
    for start in range(0, n, CHUNK):
        data = []
        for i in range(start, min(start + CHUNK, n)):
            cid = row_id("CUS", i, 6); dirty = False
            name, email = fake.name(), fake.email()
            r = random.random()
            phone = None if r < 0.05 else '' if r < 0.07 else fake.numerify(text='##########')
            r = random.random()
            city = None if r < 0.05 else random.choice(MISSPELLED_CITIES) if r < 0.15 else random.choice(CITY_NAMES)
            if chance(R['future_signup']):
                signup = fake.date_between(start_date='+1d', end_date='+30d'); dirty = True
            else:
                signup = fake.date_between(start_date='-730d', end_date='today')
            r = random.random()
            tier = None if r < 0.05 else random.choice(INVALID_TIERS) if r < 0.08 else random.choice(TIERS)
            if chance(R['orders_bad']):
                total_orders = random.randint(10001, 50000) if chance(0.5) else random.randint(-100, -1); dirty = True
            else:
                total_orders = random.randint(0, 5000)
            row = [pad(cid), name, phone, email, city, signup, tier, total_orders]
            def variant(r_):
                r_[1] = fake.name(); r_[2] = fake.numerify(text='##########'); return r_
            emit(data, tr, cid, row, dirty, variant)
        df = pd.DataFrame(data, columns=cols)
        df.to_csv(path, index=False, mode='w' if first else 'a', header=first); first = False
        total += len(df); del df, data; gc.collect()
        print(f"   ... {total:,} rows written so far")
    print(f"Saved: {total:,} rows -> {path}")
    SUMMARIES.append(tr.summary('stg_customers_raw', total))
    return tr.clean_ids()

def generate_agents(n):
    print(f"\n Generating {n:,} agents...")
    R = RATES['agents']; tr = Tracker(); data = []
    for i in range(n):
        cid = row_id("AGT", i, 5); dirty = False
        name, phone, city = fake.name(), fake.numerify(text='##########'), random.choice(CITY_NAMES)
        r = random.random()
        vehicle = None if r < 0.05 else random.choice(MISSPELLED_VEHICLES) if r < 0.15 else random.choice(VEHICLE_TYPES)
        if chance(R['future_join']):
            join_date = fake.date_between(start_date='+1d', end_date='+30d'); dirty = True
        else:
            join_date = fake.date_between(start_date='-730d', end_date='today')
        r = random.random()
        tier = None if r < 0.05 else random.choice(INVALID_TIERS) if r < 0.08 else random.choice(TIERS)
        if chance(R['rating']):
            rating = round(random.uniform(5.1, 10.0), 1) if chance(0.5) else round(random.uniform(-5.0, -0.1), 1); dirty = True
        else:
            rating = round(random.uniform(1.0, 5.0), 1)
        if chance(R['deliveries']):
            deliveries = random.randint(50001, 100000) if chance(0.5) else random.randint(-100, -1); dirty = True
        else:
            deliveries = random.randint(0, 50000)
        row = [pad(cid), name, phone, city, vehicle, join_date, tier, rating, deliveries]
        def variant(r_):
            r_[1] = fake.name(); return r_
        emit(data, tr, cid, row, dirty, variant)
    cols = ['agent_id','name','phone','city','vehicle_type','join_date','tier','avg_rating','total_deliveries']
    df = pd.DataFrame(data, columns=cols)
    path = f"{OUTPUT_DIR}/stg_agents_raw.csv"; df.to_csv(path, index=False)
    print(f"Saved: {len(df):,} rows -> {path}")
    SUMMARIES.append(tr.summary('stg_agents_raw', len(df)))
    del df, data; gc.collect()
    return tr.clean_ids()

# --------------------------------------------------------------------------------------
# ORDERS  (child of customers / restaurants / agents, parent of items / payments / complaints)
# --------------------------------------------------------------------------------------
def generate_orders(n, cust_pool, rest_pool, agent_pool):
    print(f"\n Generating {n:,} orders...")
    R = RATES['orders']; tr = Tracker(); data = []
    for i in range(n):
        cid = row_id("ORD", i, 7); dirty = False

        k = pick_kind(R['cust'])
        if   k == 'null':    customer_id = None; dirty = True
        elif k == 'empty':   customer_id = '';   dirty = True
        elif k == 'phantom': customer_id = phantom("CUS", N_CUST, 6); dirty = True
        else:                customer_id = random.choice(cust_pool)

        k = pick_kind(R['rest'])
        if   k == 'null':    restaurant_id = None; dirty = True
        elif k == 'phantom': restaurant_id = phantom("RES", N_REST, 5); dirty = True
        else:                restaurant_id = random.choice(rest_pool)

        k = pick_kind(R['agent'])
        if   k == 'null_ok': agent_id = None                       # valid: unassigned order
        elif k == 'phantom': agent_id = phantom("AGT", N_AGENT, 5); dirty = True
        else:                agent_id = random.choice(agent_pool)

        k = pick_kind(R['ts'])
        if k == 'extreme':
            order_ts = datetime(1990, 1, 1) if chance(0.5) else datetime(2050, 1, 1); dirty = True
        elif k == 'future':
            order_ts = fake.date_time_between(start_date='+1d', end_date='+30d'); dirty = True
        else:
            order_ts = fake.date_time_between(start_date='-365d', end_date='now')
        delivery_ts = None if chance(0.30) else order_ts + timedelta(minutes=random.randint(15, 90))

        k = pick_kind(R['status'])
        if   k == 'invalid': status = random.choice(INVALID_STATUS); dirty = True
        elif k == 'empty':   status = '';   dirty = True
        elif k == 'null':    status = None; dirty = True
        else:                status = random.choice(STATUS_VALUES)

        k = pick_kind(R['amount']); amount_dirty = False
        if   k == 'neg': total = round(random.uniform(-100, -1), 2); amount_dirty = True
        elif k == 'str': total = 'ERROR_STR_AMT';                    amount_dirty = True
        else:            total = round(random.uniform(50, 1500), 2)
        discount = round(random.uniform(0, total * 0.3), 2) if isinstance(total, float) and total > 0 else 0.0
        if status == 'cancelled':                       # cancelled orders are 0 / 0 (this also "heals" a bad amount)
            total, discount, amount_dirty = 0.0, 0.0, False
        if chance(R['forced_cancel']):                  # cancelled but charged -> rejected
            status, total, discount, amount_dirty = 'cancelled', round(random.uniform(50, 1500), 2), 0.0, True
        dirty = dirty or amount_dirty

        promo = fake.bothify(text='PROMO##??').upper() if chance(0.20) else None
        r = random.random()
        city = None if r < 0.05 else random.choice(MISSPELLED_CITIES) if r < 0.15 else random.choice(CITY_NAMES)
        zone, pay_method = random.choice(ZONES), random.choice(PAYMENT_METHODS)
        cuisine = random.choice(MISSPELLED_CUISINE) if chance(0.08) else random.choice(CUISINE_TYPES)
        if chance(R['delivery_bad']):
            dtm = random.randint(-30, -1) if chance(0.5) else random.randint(121, 300); dirty = True
        else:
            dtm = random.randint(10, 120)

        written_id = cid
        if not is_collision(i) and chance(R['lowercase_id']):
            written_id = 'ord_' + cid[4:]
        row = [pad(written_id), customer_id, restaurant_id, agent_id, order_ts, delivery_ts, status, total, discount,
               promo, city, zone, pay_method, cuisine, dtm]
        def variant(r_):
            r_[9] = fake.bothify(text='PROMO##??').upper(); r_[11] = random.choice(ZONES); r_[12] = random.choice(PAYMENT_METHODS); return r_
        emit(data, tr, cid, row, dirty, variant)

    cols = ['order_id','customer_id','restaurant_id','agent_id','order_ts','delivery_ts','status','total_amount','discount_amount',
            'promo_code','city','zone','payment_method','cuisine_type','delivery_time_mins']
    df = pd.DataFrame(data, columns=cols)
    path = f"{OUTPUT_DIR}/stg_orders_raw.csv"
    buf = io.StringIO(); df.to_csv(buf, index=False)
    lines = buf.getvalue().split('\n')
    with open(path, 'w', encoding='utf-8', newline='') as f:
        f.write(lines[0] + '\n')
        for line in lines[1:]:
            if not line: continue
            if chance(RATES['schema_drift_orders']):
                line = f"{line},SCHEMA_DRIFT_{random.randint(1000, 9999)}"
            f.write(line + '\n')
    print(f"Saved: {len(df):,} rows -> {path}")
    SUMMARIES.append(tr.summary('stg_orders_raw', len(df)))
    del df, data; gc.collect()
    return tr.clean_ids()

# --------------------------------------------------------------------------------------
# CHILDREN
# --------------------------------------------------------------------------------------
def generate_order_items(n, order_pool):
    print(f"\n Generating {n:,} order items...")
    R = RATES['items']; tr = Tracker(); data = []
    for i in range(n):
        cid = row_id("OI", i, 7); dirty = False
        k = pick_kind(R['order'])
        if   k == 'null':    order_id = None; dirty = True
        elif k == 'phantom': order_id = phantom("ORD", N_ORD, 7); dirty = True
        else:                order_id = random.choice(order_pool)
        product_id = f"PRD_{random.randint(1, 5000):06d}"; product_name = fake.word().capitalize()
        if chance(R['qty_neg']):   quantity = random.randint(-5, -1); dirty = True
        else:                      quantity = random.randint(1, 10)
        if chance(R['price_neg']): unit_price = round(random.uniform(-100, -1), 2); dirty = True
        else:                      unit_price = round(random.uniform(50, 300), 2)
        total_price = round(quantity * unit_price, 2)
        if chance(R['discount_neg']): discount = round(random.uniform(-50, -1), 2); dirty = True
        else:                         discount = round(random.uniform(0, 50), 2)
        row = [pad(cid), order_id, product_id, product_name, quantity, unit_price, total_price, discount]
        def variant(r_):
            r_[3] = fake.word().capitalize(); return r_
        emit(data, tr, cid, row, dirty, variant)
    cols = ['order_item_id','order_id','product_id','product_name','quantity','unit_price','total_price','discount']
    df = pd.DataFrame(data, columns=cols)
    path = f"{OUTPUT_DIR}/stg_order_items_raw.csv"; df.to_csv(path, index=False)
    print(f"Saved: {len(df):,} rows -> {path}")
    SUMMARIES.append(tr.summary('stg_order_items_raw', len(df)))
    del df, data; gc.collect()

def generate_payments(n, order_pool):
    print(f"\n Generating {n:,} payments...")
    R = RATES['payments']; tr = Tracker(); data = []
    for i in range(n):
        cid = row_id("PAY", i, 7); dirty = False
        k = pick_kind(R['order'])
        if   k == 'null':    order_id = None; dirty = True
        elif k == 'phantom': order_id = phantom("ORD", N_ORD, 7); dirty = True
        else:                order_id = random.choice(order_pool)
        if chance(R['amount_neg']): amount = round(random.uniform(-100, -1), 2); dirty = True
        else:                       amount = round(random.uniform(50, 1500), 2)
        if chance(R['method_invalid']): method = random.choice(INVALID_PAYMENT); dirty = True
        else:                           method = random.choice(PAYMENT_METHODS)
        gateway = random.choice(GATEWAYS)
        k = pick_kind(R['status'])
        if   k == 'null':    status = None; dirty = True
        elif k == 'invalid': status = random.choice(['processing', 'initiated', 'held']); dirty = True
        else:                status = random.choice(['success', 'failed', 'pending'])
        if chance(R['future_ts']):
            ts = fake.date_time_between(start_date='+1d', end_date='+30d'); dirty = True
        else:
            ts = fake.date_time_between(start_date='-365d', end_date='now')
        cashback = round(random.uniform(0, 100), 2) if chance(0.5) else 0.0
        row = {'payment_id': pad(cid), 'order_id': order_id, 'amount': amount, 'payment_method': method,
               'gateway': gateway, 'status': status, 'payment_ts': ts.isoformat(), 'cashback': cashback}
        def variant(r_):
            r_['gateway'] = random.choice(GATEWAYS); r_['cashback'] = round(random.uniform(0, 100), 2); return r_
        emit(data, tr, cid, row, dirty, variant)
    path = f"{OUTPUT_DIR}/stg_payments_raw.json"
    with open(path, 'w') as f: json.dump(data, f, default=str)
    print(f"Saved: {len(data):,} rows -> {path}")
    SUMMARIES.append(tr.summary('stg_payments_raw', len(data)))
    del data; gc.collect()

def generate_complaints(n, order_pool, cust_pool):
    print(f"\n Generating {n:,} complaints...")
    R = RATES['complaints']; tr = Tracker(); data = []
    for i in range(n):
        cid = row_id("COM", i, 7); dirty = False
        k = pick_kind(R['order'])
        if   k == 'null':    order_id = None; dirty = True
        elif k == 'phantom': order_id = phantom("ORD", N_ORD, 7); dirty = True
        else:                order_id = random.choice(order_pool)
        k = pick_kind(R['customer'])
        if   k == 'null':    customer_id = None; dirty = True
        elif k == 'phantom': customer_id = phantom("CUS", N_CUST, 6); dirty = True
        else:                customer_id = random.choice(cust_pool)
        k = pick_kind(R['category'])
        if   k == 'null':    category = None; dirty = True
        elif k == 'invalid': category = random.choice(INVALID_COMPLAINT); dirty = True
        else:                category = random.choice(COMPLAINT_CATEGORIES)
        subcat = random.choice(COMPLAINT_SUBCATS[category]) if category in COMPLAINT_SUBCATS else 'General'
        k = pick_kind(R['description'])
        if   k == 'null':  description = None; dirty = True
        elif k == 'empty': description = '';   dirty = True
        else:              description = fake.sentence(nb_words=8)
        resolution = round(random.uniform(0.5, 48.0), 1) if chance(0.70) else None
        compensation = round(random.uniform(0, 200), 2) if chance(0.40) else 0.0
        if chance(R['future_ts']):
            ts = fake.date_time_between(start_date='+1d', end_date='+30d'); dirty = True
        else:
            ts = fake.date_time_between(start_date='-365d', end_date='now')
        if chance(R['resolved_null']): is_resolved = None; dirty = True
        else:                          is_resolved = random.choice([True, False])
        row = {'complaint_id': pad(cid), 'order_id': order_id, 'customer_id': customer_id, 'complaint_category': category,
               'complaint_subcategory': subcat, 'description': description, 'resolution_time_hrs': resolution,
               'compensation_amount': compensation, 'complaint_ts': ts.isoformat(), 'is_resolved': is_resolved}
        def variant(r_):
            r_['resolution_time_hrs'] = round(random.uniform(0.5, 48.0), 1); r_['compensation_amount'] = round(random.uniform(0, 200), 2); return r_
        emit(data, tr, cid, row, dirty, variant)
    path = f"{OUTPUT_DIR}/stg_complaints_raw.json"
    with open(path, 'w') as f: json.dump(data, f, default=str)
    print(f"Saved: {len(data):,} rows -> {path}")
    SUMMARIES.append(tr.summary('stg_complaints_raw', len(data)))
    del data; gc.collect()

# --------------------------------------------------------------------------------------
def print_expected():
    print("\n" + "=" * 78)
    print("  EXPECTED CLEANER RESULT  (compare with the cleaner's final table)")
    print("=" * 78)
    print(f"{'table':24}{'read':>10}{'dupes':>10}{'good':>10}{'bad':>10}")
    T = {'read': 0, 'dupes': 0, 'good': 0.0, 'bad': 0.0}
    for s in SUMMARIES:
        good = s['good'] + s['mixed'] / 2; bad = s['bad'] + s['mixed'] / 2   # mixed = PK-collision pair, 1 clean + 1 dirty
        print(f"{s['table']:24}{s['read']:>10,}{s['dupes']:>10,}{good:>10,.0f}{bad:>10,.0f}")
        T['read'] += s['read']; T['dupes'] += s['dupes']; T['good'] += good; T['bad'] += bad
    print("-" * 64)
    print(f"{'TOTAL':24}{T['read']:>10,}{T['dupes']:>10,}{T['good']:>10,.0f}{T['bad']:>10,.0f}")
    print(f"{'PERCENT':24}{'':>10}{T['dupes']/T['read']:>10.1%}{T['good']/T['read']:>10.1%}{T['bad']/T['read']:>10.1%}")
    with open(f"{OUTPUT_DIR}/expected_summary.json", 'w') as f:
        json.dump(SUMMARIES, f, indent=2)

if __name__ == "__main__":
    print("=" * 60); print("  BYTETRACK - DATA GENERATOR (clean-pool)"); print("=" * 60)
    rest_pool  = generate_restaurants(N_REST)
    cust_pool  = generate_customers(N_CUST)
    agent_pool = generate_agents(N_AGENT)
    order_pool = generate_orders(N_ORD, cust_pool, rest_pool, agent_pool)
    generate_order_items(N_ITEM, order_pool)
    generate_payments(N_PAY, order_pool)
    generate_complaints(N_COMP, order_pool, cust_pool)
    print_expected()
    print("\n" + "=" * 60); print("  ALL FILES GENERATED SUCCESSFULLY"); print("=" * 60)