#!/usr/bin/env python3
"""Envoie dans Kafka UN paiement de test reconnaissable (INITIATED -> AUTHORIZED -> COMPLETED)
pour vérifier qu'il traverse toute la chaîne. Suivi ensuite avec src/track_payment.py.

docker compose exec jupyter python3 producer/send_test_payment.py --city Rabat --amount 1234.56
"""
import argparse
import json
import os
import time
from datetime import datetime, timedelta, timezone

from kafka import KafkaProducer


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--tx-id", default=f"TX-TEST-{datetime.now(timezone.utc):%H%M%S}")
    ap.add_argument("--type", choices=["P2M", "P2P"], default="P2M")
    ap.add_argument("--payer", default="W-10001")
    ap.add_argument("--payee", default="M-087", help="M-xxx pour P2M, W-xxxxx pour P2P")
    ap.add_argument("--city", default="Rabat")
    ap.add_argument("--amount", type=float, default=1234.56)
    ap.add_argument("--status", choices=["COMPLETED", "REJECTED"], default="COMPLETED")
    ap.add_argument("--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP", "localhost:9094"))
    args = ap.parse_args()

    producer = KafkaProducer(bootstrap_servers=args.bootstrap, acks="all",
                             key_serializer=str.encode, value_serializer=lambda v: json.dumps(v).encode())
    t0 = datetime.now(timezone.utc)
    steps = ["INITIATED", "AUTHORIZED", args.status] if args.status == "COMPLETED" else ["INITIATED", "REJECTED"]
    for i, status in enumerate(steps):
        event = {"tx_id": args.tx_id, "type": args.type, "payer_wallet": args.payer, "payee_id": args.payee,
                 "device_id": "D-test01", "city": args.city, "channel": "QR" if args.type == "P2M" else "APP",
                 "status": status, "amount_mad": args.amount,
                 "event_time": (t0 + timedelta(seconds=i)).strftime("%Y-%m-%dT%H:%M:%SZ")}
        meta = producer.send("transactions", key=args.tx_id, value=event).get(timeout=10)
        print(f"envoye {status:<10} -> partition {meta.partition}, offset {meta.offset}")
        time.sleep(1)
    producer.flush()
    print(f"\nPaiement de test : {args.tx_id} ({args.amount} MAD, {args.city}) a {t0:%H:%M:%S} UTC")
    print(f"Suivi : docker compose exec jupyter spark-submit src/track_payment.py {args.tx_id}")


if __name__ == "__main__":
    main()
