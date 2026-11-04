#!/usr/bin/env python3
"""Générateur de transactions SahlPay (portefeuille mobile).

Un événement JSON est produit par changement d'état d'une transaction :
INITIATED -> AUTHORIZED -> COMPLETED | REJECTED (+ REVERSED occasionnel).

Cas réels simulés volontairement :
  * ~5 % d'événements en retard (1 à 15 min, téléphone hors réseau) ;
  * ~2 % de doublons (même tx_id et même status) ;
  * scénarios de fraude injectés (vérité terrain écrite dans data/fraud_ground_truth.json) :
      - appareils partagés par de nombreux comptes,
      - transferts circulaires A -> B -> C -> A,
      - comptes collecteurs recevant de nombreux petits virements puis dépensant chez un commerçant.

Modes :
  python producer.py --mode file  --n 50000            -> data/transactions_history.jsonl
  python producer.py --mode kafka --rate 20            -> topic Kafka "transactions" (Ctrl-C pour arrêter)
"""
import argparse
import heapq
import json
import math
import os
import random
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "data"

CITY_WEIGHTS = {
    "Casablanca": 0.28, "Rabat": 0.14, "Marrakech": 0.12, "Fes": 0.10, "Tanger": 0.10,
    "Agadir": 0.08, "Meknes": 0.06, "Oujda": 0.05, "Kenitra": 0.04, "Tetouan": 0.03,
}

# catégorie -> (montant médian MAD, sigma log-normale)
AMOUNT_PROFILE = {
    "epicerie": (70, 0.7), "restauration": (90, 0.6), "carburant": (300, 0.5),
    "telecom": (40, 0.6), "pharmacie": (120, 0.7), "e-commerce": (380, 0.9),
    "habillement": (350, 0.7), "transport": (50, 0.6),
}
P2P_PROFILE = (300, 1.0)

N_WALLETS = 5000
COMMUNITY_SIZE = 20     # les virements P2P se font surtout dans un cercle social
N_CONTACTS = 5
LATE_RATE = 0.05
LATE_MAX_S = 15 * 60
DUP_RATE = 0.02

FILE_START = datetime(2026, 11, 8, tzinfo=timezone.utc)
FILE_SPAN_DAYS = 7
# profil horaire d'activité (0h..23h)
HOURLY = [1, 0.5, 0.3, 0.2, 0.2, 0.4, 1, 2.5, 4, 5, 5.5, 6, 7, 6.5, 5.5, 5, 5.5, 6.5, 7, 7, 6, 4.5, 3, 2]


def iso(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def lognormal(rng, median, sigma, lo=5.0, hi=20000.0):
    return round(min(hi, max(lo, rng.lognormvariate(math.log(median), sigma))), 2)


def load_merchants(path):
    if not Path(path).exists():
        sys.exit(f"{path} introuvable : lancer d'abord python producer/generate_merchants.py")
    with open(path, encoding="utf-8") as f:
        return [json.loads(line) for line in f if line.strip()]


class Population:
    """Comptes, appareils, cercles sociaux et commerçants — déterministes pour une graine donnée."""

    def __init__(self, rng, merchants):
        self.rng = rng
        self.merchants = merchants
        self.merchant_category = {m["merchant_id"]: m["category"] for m in merchants}
        self.merchants_by_city = {}
        for m in merchants:
            self.merchants_by_city.setdefault(m["city"], []).append(m["merchant_id"])
        self.ecommerce = [m["merchant_id"] for m in merchants if m["category"] == "e-commerce"]

        self.wallets = [f"W-{10000 + i}" for i in range(N_WALLETS)]
        cities, weights = list(CITY_WEIGHTS), list(CITY_WEIGHTS.values())
        self.home_city = {w: rng.choices(cities, weights)[0] for w in self.wallets}

        self._used_devices = set()
        self.devices = {}
        for w in self.wallets:
            self.devices[w] = [self.new_device()]
            if rng.random() < 0.15:                       # second téléphone
                self.devices[w].append(self.new_device())

        shuffled = self.wallets[:]
        rng.shuffle(shuffled)
        self.contacts = {}
        for i in range(0, len(shuffled), COMMUNITY_SIZE):
            community = shuffled[i:i + COMMUNITY_SIZE]
            for w in community:
                others = [o for o in community if o != w]
                self.contacts[w] = rng.sample(others, min(N_CONTACTS, len(others)))
                if rng.random() < 0.03:                   # appareil familial partagé (cas légitime)
                    self.devices[w].append(self.devices[rng.choice(others)][0])

    def new_device(self):
        while True:
            d = "D-%06x" % self.rng.getrandbits(24)
            if d not in self._used_devices:
                self._used_devices.add(d)
                return d

    def p2m_amount(self, rng, merchant_id):
        median, sigma = AMOUNT_PROFILE.get(self.merchant_category.get(merchant_id, "epicerie"), (100, 0.8))
        return lognormal(rng, median, sigma)


def make_tx(type_, payer, payee, device, city, channel, amount, forced=False):
    return {"type": type_, "payer_wallet": payer, "payee_id": payee, "device_id": device,
            "city": city, "channel": channel, "amount_mad": amount, "forced": forced}


def random_tx(rng, pop):
    payer = rng.choice(pop.wallets)
    device = pop.devices[payer][0] if rng.random() < 0.8 else rng.choice(pop.devices[payer])
    home = pop.home_city[payer]
    if rng.random() < 0.70:
        if rng.random() < 0.002:                          # commerçant absent du référentiel
            merchant, city = f"M-{rng.randint(901, 999)}", home
        else:
            city = home if rng.random() < 0.85 else rng.choices(list(CITY_WEIGHTS), list(CITY_WEIGHTS.values()))[0]
            merchant = rng.choice(pop.merchants_by_city.get(city) or [m["merchant_id"] for m in pop.merchants])
        channel = rng.choices(["QR", "APP", "USSD"], [0.70, 0.25, 0.05])[0]
        return make_tx("P2M", payer, merchant, device, city, channel, pop.p2m_amount(rng, merchant))
    payee = rng.choice(pop.contacts[payer]) if rng.random() < 0.99 else rng.choice(pop.wallets)
    channel = rng.choices(["APP", "USSD"], [0.75, 0.25])[0]
    return make_tx("P2P", payer, payee, device, home, channel, lognormal(rng, *P2P_PROFILE))


def build_scenarios(rng, pop):
    """Scénarios de fraude injectés + vérité terrain."""
    txs = []
    truth = {"shared_devices": [], "cycles": [], "collectors": []}
    reserved = set()

    def pick(n):
        chosen = rng.sample([w for w in pop.wallets if w not in reserved], n)
        reserved.update(chosen)
        return chosen

    # 1) appareils partagés par de nombreux comptes
    for n_accounts in (25, 12):
        device = pop.new_device()
        wallets = pick(n_accounts)
        truth["shared_devices"].append({"device_id": device, "wallets": sorted(wallets)})
        for w in wallets:
            for _ in range(rng.randint(1, 3)):
                city = pop.home_city[w]
                if rng.random() < 0.6:
                    m = rng.choice(pop.merchants_by_city.get(city) or pop.ecommerce)
                    txs.append(make_tx("P2M", w, m, device, city, "APP", pop.p2m_amount(rng, m), True))
                else:
                    payee = rng.choice(pop.wallets)
                    txs.append(make_tx("P2P", w, payee, device, city, "APP", round(rng.uniform(200, 1500), 2), True))

    # 2) transferts circulaires A -> B -> C -> A (répétés deux fois, montants proches)
    for _ in range(4):
        a, b, c = pick(3)
        truth["cycles"].append([a, b, c])
        base = rng.uniform(3000, 9000)
        for _rep in range(2):
            for s, d in ((a, b), (b, c), (c, a)):
                amount = round(base * rng.uniform(0.96, 1.0), 2)
                txs.append(make_tx("P2P", s, d, pop.devices[s][0], pop.home_city[s], "APP", amount, True))

    # 3) comptes collecteurs : nombreux petits virements entrants puis dépense e-commerce
    for _ in range(3):
        (collector,) = pick(1)
        senders = rng.sample([w for w in pop.wallets if w != collector], rng.randint(40, 60))
        total = 0.0
        for s in senders:
            amount = round(rng.uniform(10, 49), 2)
            total += amount
            channel = rng.choice(["USSD", "APP"])
            txs.append(make_tx("P2P", s, collector, pop.devices[s][0], pop.home_city[s], channel, amount, True))
        merchant = rng.choice(pop.ecommerce)
        for _ in range(2):
            txs.append(make_tx("P2M", collector, merchant, pop.devices[collector][0],
                               pop.home_city[collector], "APP", round(total * 0.45, 2), True))
        truth["collectors"].append({"wallet": collector, "n_senders": len(senders), "cash_out_merchant": merchant})

    rng.shuffle(txs)
    truth["n_scenario_transactions"] = len(txs)
    return txs, truth


def lifecycle(rng, tx):
    """Liste de (décalage en secondes depuis l'initiation, status)."""
    steps = [(0.0, "INITIATED")]
    t = rng.uniform(0.5, 3)
    reject_auth = 0.04 + (0.04 if tx["channel"] == "USSD" else 0) + (0.03 if tx["amount_mad"] > 5000 else 0)
    if not tx["forced"] and rng.random() < reject_auth:
        return steps + [(t, "REJECTED")]
    steps.append((t, "AUTHORIZED"))
    t += rng.uniform(0.5, 4)
    if not tx["forced"] and rng.random() < 0.03:
        return steps + [(t, "REJECTED")]
    steps.append((t, "COMPLETED"))
    if not tx["forced"] and rng.random() < 0.01:
        steps.append((t + rng.uniform(60, 1800), "REVERSED"))
    return steps


def event(tx_id, tx, status, event_time):
    return {"tx_id": tx_id, "type": tx["type"], "payer_wallet": tx["payer_wallet"], "payee_id": tx["payee_id"],
            "device_id": tx["device_id"], "city": tx["city"], "channel": tx["channel"], "status": status,
            "amount_mad": tx["amount_mad"], "event_time": iso(event_time)}


def deliveries(rng, steps):
    """Pour chaque étape : (décalage événement, délai d'arrivée, doublon?)."""
    out = []
    for offset, status in steps:
        late = rng.random() < LATE_RATE
        delay = rng.uniform(60, LATE_MAX_S) if late else rng.uniform(0, 1.5)
        out.append((offset, status, delay, late, False))
        if rng.random() < DUP_RATE:
            out.append((offset, status, delay + rng.uniform(0.5, 30), late, True))
    return out


def write_truth(truth, seed):
    DATA.mkdir(parents=True, exist_ok=True)
    path = DATA / "fraud_ground_truth.json"
    with path.open("w", encoding="utf-8") as f:
        json.dump({"seed": seed, **truth}, f, indent=2)
    print(f"Verite terrain des scenarios : {path}")


# ---------------------------------------------------------------------------------------------- file

def run_file(args, pop, scenarios, flow_rng):
    span = FILE_SPAN_DAYS * 86400

    def start_time(max_fraction=1.0):
        day = flow_rng.randrange(int(FILE_SPAN_DAYS * max_fraction))
        hour = flow_rng.choices(range(24), HOURLY)[0]
        return FILE_START + timedelta(days=day, hours=hour, seconds=flow_rng.uniform(0, 3600))

    records = []   # (arrivée, seq, événement)
    counter = 1

    def emit(tx, t0):
        nonlocal counter
        tx_id = f"TX-{counter:08d}"
        counter += 1
        for offset, status, delay, _late, _dup in deliveries(flow_rng, lifecycle(flow_rng, tx)):
            et = t0 + timedelta(seconds=offset)
            records.append((et + timedelta(seconds=delay), len(records), event(tx_id, tx, status, et)))

    for tx in scenarios:                       # scénarios hors des dernières heures pour ne pas être tronqués
        emit(tx, start_time(0.9))
    while len(records) < args.n:
        emit(random_tx(flow_rng, pop), start_time())

    records.sort(key=lambda r: (r[0], r[1]))
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", encoding="utf-8") as f:
        for _, _, e in records[:args.n]:
            f.write(json.dumps(e) + "\n")
    late = sum(1 for arr, _, e in records[:args.n]
               if (arr - datetime.strptime(e["event_time"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)).total_seconds() > 60)
    print(f"{args.n} evenements ecrits dans {out} ({counter - 1} transactions, ~{late} en retard, "
          f"periode {iso(FILE_START)} + {span // 86400} jours)")


# --------------------------------------------------------------------------------------------- kafka

def next_tx_counter():
    state = DATA / ".tx_counter"
    try:
        return max(50_000_000, int(state.read_text().strip()) + 1)
    except (OSError, ValueError):
        return 50_000_000


def save_tx_counter(value):
    try:
        (DATA / ".tx_counter").write_text(str(value))
    except OSError:
        pass


def run_kafka(args, pop, scenarios, flow_rng):
    from kafka import KafkaProducer

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        linger_ms=10,
    )
    late_log = (DATA / "producer_late_events.log").open("a", encoding="utf-8")
    queue = list(scenarios)
    tx_rate = args.rate / 3.0           # ~3 événements par transaction en moyenne
    counter = next_tx_counter()
    heap = []
    seq = 0
    start = next_tx = last_stats = time.monotonic()
    sent = late = dups = 0
    print(f"Envoi vers {args.bootstrap} topic={args.topic} a ~{args.rate} evt/s (Ctrl-C pour arreter)")
    try:
        while True:
            now = time.monotonic()
            if args.duration and now - start >= args.duration:
                break
            wall = datetime.now(timezone.utc)
            while next_tx <= now:
                if queue and flow_rng.random() < 0.08:
                    tx = queue.pop()
                    if not queue:
                        print(f"[SCENARIOS] {len(scenarios)} transactions de scenarios injectees")
                else:
                    tx = random_tx(flow_rng, pop)
                tx_id = f"TX-{counter:08d}"
                counter += 1
                t0 = wall - timedelta(seconds=now - next_tx)
                for offset, status, delay, is_late, is_dup in deliveries(flow_rng, lifecycle(flow_rng, tx)):
                    e = event(tx_id, tx, status, t0 + timedelta(seconds=offset))
                    heapq.heappush(heap, (next_tx + offset + delay, seq, e, is_late, is_dup, delay))
                    seq += 1
                next_tx += flow_rng.expovariate(tx_rate)

            while heap and heap[0][0] <= now:
                _, _, e, is_late, is_dup, delay = heapq.heappop(heap)
                producer.send(args.topic, key=e["tx_id"], value=e)
                sent += 1
                dups += is_dup
                if is_late:
                    late += 1
                    line = (f"[LATE] tx_id={e['tx_id']} status={e['status']} city={e['city']} "
                            f"event_time={e['event_time']} arrivee={iso(datetime.now(timezone.utc))} "
                            f"retard={delay / 60:.1f} min")
                    late_log.write(line + "\n")
                    if delay > 600:
                        print(line)

            if now - last_stats >= 10:
                elapsed = now - start
                print(f"[STATS] {sent} evts envoyes ({sent / elapsed:.1f}/s), retard={late}, doublons={dups}, "
                      f"scenarios restants={len(queue)}, en attente={len(heap)}")
                last_stats = now
                late_log.flush()
                save_tx_counter(counter)
            time.sleep(0.02)
    except KeyboardInterrupt:
        pass
    finally:
        producer.flush()
        producer.close()
        late_log.close()
        save_tx_counter(counter)
        print(f"Arret : {sent} evenements envoyes ({late} en retard, {dups} doublons)")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--mode", choices=["file", "kafka"], required=True)
    ap.add_argument("--n", type=int, default=50000, help="mode file : nombre d'evenements")
    ap.add_argument("--out", default=str(DATA / "transactions_history.jsonl"))
    ap.add_argument("--rate", type=float, default=20, help="mode kafka : evenements par seconde")
    ap.add_argument("--duration", type=float, default=0, help="mode kafka : duree en secondes (0 = infini)")
    ap.add_argument("--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP", "localhost:9094"))
    ap.add_argument("--topic", default="transactions")
    ap.add_argument("--merchants", default=str(DATA / "merchants.json"))
    ap.add_argument("--seed", type=int, default=2026)
    args = ap.parse_args()

    pop_rng = random.Random(args.seed)        # population + scénarios : identiques dans les deux modes
    pop = Population(pop_rng, load_merchants(args.merchants))
    scenarios, truth = build_scenarios(pop_rng, pop)
    write_truth(truth, args.seed)

    flow_rng = random.Random(args.seed + 1 if args.mode == "file" else time.time_ns())
    if args.mode == "file":
        run_file(args, pop, scenarios, flow_rng)
    else:
        run_kafka(args, pop, scenarios, flow_rng)


if __name__ == "__main__":
    main()
