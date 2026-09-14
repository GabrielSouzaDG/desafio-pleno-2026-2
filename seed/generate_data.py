#!/usr/bin/env python3
"""
Gerador de dados sintéticos do desafio de Engenharia de Dados.

Produz:
  mock-api/data/events.jsonl        -> consumido pela Mock API
  seed/postgres/02_customers.sql    -> carga inicial (batch 1, roda no init do Postgres)
  seed/postgres/03_batch2.sql       -> alterações do batch 2 (aplicado via `make batch2`)
  seed/GABARITO.md                  -> números esperados (uso interno do avaliador)

As anomalias abaixo são PLANTADAS de propósito:
  - duplicatas exatas de eventos
  - correções (mesmo event_id com updated_at posterior e valores diferentes)
  - late arrival (occurred_at até 3 dias antes do updated_at)
  - schema drift (campo novo a partir de uma data)
  - customer_id nulo e customer_id órfão (não existe na dimensão)
  - mudança de plano de clientes entre o batch 1 e o batch 2

Determinístico: mesma SEED => mesmo dataset.
"""

from __future__ import annotations

import json
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

SEED = 20260911
random.seed(SEED)

ROOT = Path(__file__).resolve().parent.parent

# ---------------------------------------------------------------- parâmetros
DATA_END = datetime(2026, 8, 31, 23, 59, 0, tzinfo=timezone.utc)
DATA_START = DATA_END - timedelta(days=120)
BATCH1_CUTOFF = DATA_END - timedelta(days=10)      # tudo com updated_at <= aqui é batch 1
DRIFT_DATE = DATA_END - timedelta(days=6)          # a partir daqui aparece campo novo

N_CUSTOMERS = 400
N_TICKETS = 2600
N_USAGE_EVENTS = 9000

DUP_RATE = 0.015          # duplicatas exatas
NULL_CUSTOMER_RATE = 0.004
ORPHAN_CUSTOMER_RATE = 0.004
N_CORRECTIONS = 180       # eventos do batch 1 corrigidos no batch 2
N_LATE_ARRIVALS = 220     # eventos antigos que só chegam no batch 2

PLANS = ["free", "pro", "enterprise"]
PLAN_W = [0.45, 0.40, 0.15]
SEGMENTS = ["SMB", "MidMarket", "Enterprise", "Education", "Government"]
COUNTRIES = ["BR", "BR", "BR", "BR", "PT", "AR", "MX", "US"]
CHANNELS = ["email", "chat", "phone", "in_app"]
PRIORITIES = ["low", "medium", "high", "urgent"]
USAGE_TYPES = ["login", "feature_used", "export_generated", "report_viewed"]
SOURCE_APPS = ["web", "mobile_ios", "mobile_android", "api"]

PREFIX = ["Nova", "Alfa", "Vertex", "Solar", "Prime", "Atlas", "Delta", "Norte",
          "Vale", "Horizonte", "Aurora", "Centro", "Forte", "Lumen", "Terra"]
SUFFIX = ["Tecnologia", "Servicos", "Industria", "Logistica", "Consultoria",
          "Sistemas", "Alimentos", "Energia", "Saude", "Educacao", "Varejo"]


def iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def rand_dt(start: datetime, end: datetime) -> datetime:
    delta = int((end - start).total_seconds())
    return start + timedelta(seconds=random.randint(0, max(delta, 1)))


# ---------------------------------------------------------------- customers
def build_customers():
    """
    ~55% dos clientes entram antes da janela de eventos (base instalada) e ~45%
    durante a janela. Sem isso não existe coorte observável dentro do dataset.
    """
    customers = []
    for i in range(1, N_CUSTOMERS + 1):
        if random.random() < 0.55:
            signup = rand_dt(DATA_START - timedelta(days=600), DATA_START)
        else:
            signup = rand_dt(DATA_START, DATA_END - timedelta(days=5))
        plan = random.choices(PLANS, weights=PLAN_W)[0]
        customers.append({
            "customer_id": f"c_{i:04d}",
            "company_name": f"{random.choice(PREFIX)} {random.choice(SUFFIX)}",
            "plan": plan,
            "segment": random.choice(SEGMENTS),
            "signup_date": signup.date().isoformat(),
            "country": random.choice(COUNTRIES),
            "is_active": random.random() > 0.08,
            "updated_at": iso(signup),
            "_signup_dt": signup,
        })
    return customers


def build_customer_changes(customers):
    """Mudanças aplicadas no batch 2: upgrades/downgrades de plano, churn e clientes novos."""
    changes = []
    pool = [c for c in customers if c["_signup_dt"] < BATCH1_CUTOFF - timedelta(days=30)]
    for c in random.sample(pool, 55):
        old = c["plan"]
        new = random.choice([p for p in PLANS if p != old])
        changes.append({
            "type": "plan_change",
            "customer_id": c["customer_id"],
            "old_plan": old,
            "new_plan": new,
            "updated_at": iso(rand_dt(BATCH1_CUTOFF, DATA_END)),
        })
    for c in random.sample(pool, 12):
        changes.append({
            "type": "churn",
            "customer_id": c["customer_id"],
            "updated_at": iso(rand_dt(BATCH1_CUTOFF, DATA_END)),
        })
    new_customers = []
    for i in range(N_CUSTOMERS + 1, N_CUSTOMERS + 19):
        signup = rand_dt(BATCH1_CUTOFF, DATA_END)
        new_customers.append({
            "customer_id": f"c_{i:04d}",
            "company_name": f"{random.choice(PREFIX)} {random.choice(SUFFIX)}",
            "plan": random.choices(PLANS, weights=PLAN_W)[0],
            "segment": random.choice(SEGMENTS),
            "signup_date": signup.date().isoformat(),
            "country": random.choice(COUNTRIES),
            "is_active": True,
            "updated_at": iso(signup),
            "_signup_dt": signup,
        })
    return changes, new_customers


# ------------------------------------------------------------------- events
_eid = 0
_tid = 0


def new_event_id() -> str:
    global _eid
    _eid += 1
    return f"e_{_eid:07d}"


def new_ticket_id() -> str:
    """Sequencial: ticket_id repetido misturaria dois tickets na métrica de primeira resposta."""
    global _tid
    _tid += 1
    return f"t_{_tid:06d}"


# peso de atividade: enterprise usa mais o produto que free, e cada cliente tem
# um fator próprio. Sem isso, o "Top 10 clientes" vira ruído estatístico.
ACTIVITY_BY_PLAN = {"free": 1.0, "pro": 1.8, "enterprise": 3.2}


def build_activity_weights(customers):
    return [ACTIVITY_BY_PLAN[c["plan"]] * random.lognormvariate(0, 0.55) for c in customers]


def pick_customer(customers, weights):
    """Retorna (customer_id, signup_dt). signup_dt=None para nulo/órfão."""
    r = random.random()
    if r < NULL_CUSTOMER_RATE:
        return None, None
    if r < NULL_CUSTOMER_RATE + ORPHAN_CUSTOMER_RATE:
        return f"c_9{random.randint(100, 999)}", None
    c = random.choices(customers, weights=weights)[0]
    return c["customer_id"], c["_signup_dt"]


def event_window(signup_dt):
    """Um cliente não gera evento antes de existir."""
    start = DATA_START if signup_dt is None else max(DATA_START, signup_dt)
    return start, DATA_END


def make_event(customer_id, event_type, occurred_at, properties, updated_at=None):
    ev = {
        "event_id": new_event_id(),
        "customer_id": customer_id,
        "event_type": event_type,
        "occurred_at": iso(occurred_at),
        "updated_at": iso(updated_at or occurred_at),
        "channel": random.choice(CHANNELS),
        "properties": properties,
    }
    # schema drift: campo novo passa a existir a partir da DRIFT_DATE
    if occurred_at >= DRIFT_DATE:
        ev["source_app"] = random.choice(SOURCE_APPS)
        if event_type.startswith("ticket_"):
            ev["properties"]["sla_minutes"] = random.choice([60, 120, 240, 480])
    return ev


def first_response_minutes(plan: str) -> float:
    """Enterprise responde mais rápido — o desafio pede essa métrica por plano."""
    base = {"enterprise": 25, "pro": 90, "free": 260}[plan]
    return max(3.0, random.lognormvariate(0, 0.7) * base)


def build_events(customers):
    plan_by_id = {c["customer_id"]: c["plan"] for c in customers}
    weights = build_activity_weights(customers)
    events = []

    # --- ciclo de vida de tickets
    for _ in range(N_TICKETS):
        cid, signup = pick_customer(customers, weights)
        plan = plan_by_id.get(cid, "free")
        ticket_id = new_ticket_id()
        start, end = event_window(signup)
        if start >= end - timedelta(hours=6):
            continue
        opened = rand_dt(start, end - timedelta(hours=6))
        priority = random.choices(PRIORITIES, weights=[0.3, 0.4, 0.22, 0.08])[0]
        agent = f"a_{random.randint(1, 24):02d}"

        events.append(make_event(cid, "ticket_opened", opened, {
            "ticket_id": ticket_id,
            "priority": priority,
            "subject_len": random.randint(15, 180),
        }))

        if random.random() < 0.86:                       # 14% sem resposta, de propósito
            replied = opened + timedelta(minutes=first_response_minutes(plan))
            if replied <= DATA_END:
                events.append(make_event(cid, "ticket_replied", replied, {
                    "ticket_id": ticket_id,
                    "agent_id": agent,
                    "is_first_response": True,
                }))
                # respostas seguintes, para o candidato ter que pegar a PRIMEIRA
                for _ in range(random.randint(0, 3)):
                    replied = replied + timedelta(minutes=random.randint(30, 3000))
                    if replied > DATA_END:
                        break
                    events.append(make_event(cid, "ticket_replied", replied, {
                        "ticket_id": ticket_id,
                        "agent_id": agent,
                        "is_first_response": False,
                    }))
                if random.random() < 0.75:
                    closed = replied + timedelta(hours=random.randint(1, 96))
                    if closed <= DATA_END:
                        events.append(make_event(cid, "ticket_closed", closed, {
                            "ticket_id": ticket_id,
                            "resolution": random.choice(["solved", "duplicate", "wont_fix"]),
                        }))

    # --- eventos de uso do produto
    for _ in range(N_USAGE_EVENTS):
        cid, signup = pick_customer(customers, weights)
        start, end = event_window(signup)
        if start >= end:
            continue
        occurred = rand_dt(start, end)
        etype = random.choice(USAGE_TYPES)
        props = {"session_id": f"s_{random.randint(10**6, 10**7)}"}
        if etype == "feature_used":
            props["feature"] = random.choice(
                ["dashboard", "export", "search", "automation", "api_key", "webhook"])
        if etype == "export_generated":
            props["rows"] = random.randint(10, 50000)
            props["format"] = random.choice(["csv", "xlsx", "json"])
        events.append(make_event(cid, etype, occurred, props))

    return events


def apply_anomalies(events):
    """Duplicatas, correções e late arrivals. Retorna a lista final com o campo _batch."""
    batch1 = [e for e in events if e["updated_at"] <= iso(BATCH1_CUTOFF)]
    final = list(events)

    # --- correções: mesmo event_id reaparece no batch 2 com updated_at posterior
    corrections = 0
    for ev in random.sample(batch1, min(N_CORRECTIONS, len(batch1))):
        fixed = json.loads(json.dumps(ev))
        fixed["updated_at"] = iso(rand_dt(BATCH1_CUTOFF, DATA_END))
        if fixed["event_type"] == "ticket_opened":
            fixed["properties"]["priority"] = random.choice(PRIORITIES)
        else:
            fixed["channel"] = random.choice(CHANNELS)
        fixed["properties"]["corrected"] = True
        final.append(fixed)
        corrections += 1

    # --- late arrival: occurred_at antigo, updated_at no batch 2
    late = 0
    for ev in final[:]:
        if late >= N_LATE_ARRIVALS:
            break
        occurred = datetime.strptime(ev["occurred_at"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        if BATCH1_CUTOFF - timedelta(days=3) <= occurred <= BATCH1_CUTOFF and random.random() < 0.25:
            ev["updated_at"] = iso(rand_dt(BATCH1_CUTOFF, BATCH1_CUTOFF + timedelta(days=3)))
            late += 1

    # --- duplicatas exatas
    dups = 0
    for ev in random.sample(final, int(len(final) * DUP_RATE)):
        final.append(json.loads(json.dumps(ev)))
        dups += 1

    # --- marca o batch de visibilidade
    cutoff = iso(BATCH1_CUTOFF)
    for ev in final:
        ev["_batch"] = 1 if ev["updated_at"] <= cutoff else 2

    final.sort(key=lambda e: (e["updated_at"], e["event_id"]))
    return final, {"corrections": corrections, "late_arrivals": late, "duplicates": dups}


# --------------------------------------------------------------------- SQL
def sql_escape(v: str) -> str:
    return v.replace("'", "''")


def write_postgres_sql(customers, changes, new_customers):
    init = ROOT / "seed" / "postgres" / "02_customers.sql"
    lines = ["-- Gerado por seed/generate_data.py. Carga inicial (batch 1).", "BEGIN;"]
    for c in customers:
        lines.append(
            "INSERT INTO crm.customers "
            "(customer_id, company_name, plan, segment, signup_date, country, is_active, updated_at) VALUES "
            f"('{c['customer_id']}', '{sql_escape(c['company_name'])}', '{c['plan']}', '{c['segment']}', "
            f"DATE '{c['signup_date']}', '{c['country']}', {str(c['is_active']).lower()}, "
            f"TIMESTAMPTZ '{c['updated_at']}');"
        )
    lines += ["COMMIT;", ""]
    init.write_text("\n".join(lines), encoding="utf-8")

    b2 = ROOT / "seed" / "postgres" / "03_batch2.sql"
    lines = ["-- Batch 2: aplicado por `make batch2`. NAO roda no init do Postgres.", "BEGIN;"]
    for ch in changes:
        if ch["type"] == "plan_change":
            lines.append(
                f"UPDATE crm.customers SET plan = '{ch['new_plan']}', "
                f"updated_at = TIMESTAMPTZ '{ch['updated_at']}' "
                f"WHERE customer_id = '{ch['customer_id']}';"
            )
        else:
            lines.append(
                f"UPDATE crm.customers SET is_active = false, "
                f"updated_at = TIMESTAMPTZ '{ch['updated_at']}' "
                f"WHERE customer_id = '{ch['customer_id']}';"
            )
    for c in new_customers:
        lines.append(
            "INSERT INTO crm.customers "
            "(customer_id, company_name, plan, segment, signup_date, country, is_active, updated_at) VALUES "
            f"('{c['customer_id']}', '{sql_escape(c['company_name'])}', '{c['plan']}', '{c['segment']}', "
            f"DATE '{c['signup_date']}', '{c['country']}', true, TIMESTAMPTZ '{c['updated_at']}') "
            "ON CONFLICT (customer_id) DO NOTHING;"
        )
    lines += ["COMMIT;", ""]
    b2.write_text("\n".join(lines), encoding="utf-8")


# -------------------------------------------------------------------- main
def main():
    customers = build_customers()
    changes, new_customers = build_customer_changes(customers)
    events = build_events(customers)
    events, stats = apply_anomalies(events)

    out = ROOT / "mock-api" / "data" / "events.jsonl"
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", encoding="utf-8") as fh:
        for ev in events:
            fh.write(json.dumps(ev, ensure_ascii=False) + "\n")

    write_postgres_sql(customers, changes, new_customers)

    b1 = sum(1 for e in events if e["_batch"] == 1)
    b2 = len(events) - b1
    unique_ids = len({e["event_id"] for e in events})
    drift = sum(1 for e in events if "source_app" in e)
    orphan = sum(1 for e in events if e["customer_id"] and not e["customer_id"].startswith("c_0")
                 and e["customer_id"] not in {c["customer_id"] for c in customers})
    nulls = sum(1 for e in events if e["customer_id"] is None)

    gab = ROOT / "seed" / "GABARITO.md"
    gab.write_text(
        "# Gabarito do dataset (uso interno — não enviar ao candidato)\n\n"
        f"- Seed: `{SEED}` (determinístico)\n"
        f"- Janela: {DATA_START.date()} a {DATA_END.date()}\n"
        f"- Corte batch 1: `updated_at <= {iso(BATCH1_CUTOFF)}`\n"
        f"- Schema drift a partir de: `occurred_at >= {iso(DRIFT_DATE)}` (campo `source_app`)\n\n"
        "## Volumetria\n\n"
        f"| Métrica | Valor |\n|---|---|\n"
        f"| Linhas totais no arquivo | {len(events)} |\n"
        f"| **event_id distintos (silver correta)** | **{unique_ids}** |\n"
        f"| Visíveis no batch 1 | {b1} |\n"
        f"| Visíveis só no batch 2 | {b2} |\n"
        f"| Duplicatas exatas plantadas | {stats['duplicates']} |\n"
        f"| Correções (mesmo id, updated_at maior) | {stats['corrections']} |\n"
        f"| Late arrivals | {stats['late_arrivals']} |\n"
        f"| Eventos com `source_app` (drift) | {drift} |\n"
        f"| customer_id nulo | {nulls} |\n"
        f"| customer_id órfão | {orphan} |\n"
        f"| Clientes (batch 1) | {len(customers)} |\n"
        f"| Mudanças de plano no batch 2 | {sum(1 for c in changes if c['type'] == 'plan_change')} |\n"
        f"| Churns no batch 2 | {sum(1 for c in changes if c['type'] == 'churn')} |\n"
        f"| Clientes novos no batch 2 | {len(new_customers)} |\n\n"
        "## Como conferir a entrega\n\n"
        "```sql\n"
        "-- na silver, depois de rodar batch 1 + batch 2 (e depois de rodar DE NOVO):\n"
        f"SELECT count(*) FROM lakehouse.silver.events;          -- esperado: {unique_ids}\n"
        "SELECT count(*) FROM (SELECT event_id FROM lakehouse.silver.events\n"
        "                      GROUP BY 1 HAVING count(*) > 1);  -- esperado: 0\n"
        "```\n\n"
        "Se o candidato retornar mais que isso, ele não deduplicou. "
        "Se retornar menos, provavelmente descartou os eventos com `customer_id` nulo/órfão "
        "com um INNER JOIN silencioso — pergunte sobre isso no code review.\n",
        encoding="utf-8")

    print(f"events.jsonl          : {len(events)} linhas ({unique_ids} event_id distintos)")
    print(f"  batch 1 / batch 2   : {b1} / {b2}")
    print(f"  dups/correcoes/late : {stats['duplicates']} / {stats['corrections']} / {stats['late_arrivals']}")
    print(f"  drift / null / orfao: {drift} / {nulls} / {orphan}")
    print(f"customers             : {len(customers)} (+{len(new_customers)} no batch 2)")


if __name__ == "__main__":
    main()
