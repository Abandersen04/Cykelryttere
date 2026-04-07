"""
main.py – Hoved-pipeline for danske cykelryttere.

Kørsel:
  python main.py                  # kun Wikidata
  python main.py --enrich-pcs     # Wikidata + PCS profil + top-3 resultater (alle)
  python main.py --enrich-pcs 50  # Wikidata + PCS for de første 50 ryttere
"""

import asyncio
import os
import sys
from datetime import datetime, timezone

import browser as br
from wikidata import fetch_cyclists
from pcs_enricher import enrich_from_pcs, fetch_top3_results
from database import init_db, upsert_cyclist, get_all, insert_results, get_cyclist_id
from export import export_all
from config import DB_PATH, OUTPUT_DIR


async def main_async(enrich_pcs: bool = False, pcs_limit: int | None = None) -> None:
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    conn = init_db(DB_PATH)
    print(f"Database initialiseret: {DB_PATH}\n")

    # --- Trin 1: Wikidata ---
    print("Henter grunddata fra Wikidata...")
    cyclists = fetch_cyclists()
    print(f"Fandt {len(cyclists)} danske cykelryttere i Wikidata.")

    with_pcs = [c for c in cyclists if c.get("pcs_id")]
    print(f"  Med PCS-ID:  {len(with_pcs)}")
    print(f"  Uden PCS-ID: {len(cyclists) - len(with_pcs)}\n")

    now = datetime.now(timezone.utc).isoformat()
    for c in cyclists:
        upsert_cyclist(conn, {**c, "weight_kg": None, "height_m": None,
                               "total_wins": 0, "current_team": None,
                               "fetched_at": now,
                               "wikipedia_url": c.get("wikipedia_url")})
    print(f"Wikidata-data gemt for alle {len(cyclists)} ryttere.\n")

    # --- Trin 2: PCS-berigelse (valgfri) ---
    if enrich_pcs and with_pcs:
        to_enrich = with_pcs[:pcs_limit] if pcs_limit else with_pcs
        print(f"Beriger {len(to_enrich)} ryttere med PCS-profil + top-3 resultater...")
        total_results = 0

        for i, cyclist in enumerate(to_enrich, start=1):
            pcs_id = cyclist["pcs_id"]
            print(f"  [{i}/{len(to_enrich)}] {cyclist['name']} (id={pcs_id})", end=" ... ", flush=True)

            # Profil (vægt, højde, hold)
            pcs_data = await enrich_from_pcs(pcs_id)
            upsert_cyclist(conn, {**cyclist, **pcs_data,
                                   "total_wins": 0,
                                   "wikipedia_url": cyclist.get("wikipedia_url"),
                                   "fetched_at": datetime.now(timezone.utc).isoformat()})

            # Top-3 resultater
            results = await fetch_top3_results(pcs_id)
            if results:
                cyclist_id = get_cyclist_id(conn, pcs_id)
                if cyclist_id:
                    insert_results(conn, cyclist_id, results)
                    total_results += len(results)

            wins = sum(1 for r in results if r.get("rank") == 1)
            print(f"{len(results)} resultater, {wins} sejre")

        print(f"\nGemt {total_results} resultater i alt.\n")

    # --- Trin 3: Eksport ---
    print("Eksporterer data...")
    export_all(conn)

    all_cyclists = get_all(conn)
    total_res = conn.execute("SELECT COUNT(*) FROM results").fetchone()[0]
    print(f"\n--- Afslutningsstatistik ---")
    print(f"  Ryttere i alt:         {len(all_cyclists)}")
    print(f"  Kvinder:               {sum(1 for r in all_cyclists if r.get('gender') == 'kvinde')}")
    print(f"  Mænd:                  {sum(1 for r in all_cyclists if r.get('gender') == 'mand')}")
    print(f"  Med fødested:          {sum(1 for r in all_cyclists if r.get('birthplace'))}")
    print(f"  Med koordinater:       {sum(1 for r in all_cyclists if r.get('latitude'))}")
    print(f"  Med PCS-ID:            {sum(1 for r in all_cyclists if r.get('pcs_id'))}")
    print(f"  Med vægt/højde:        {sum(1 for r in all_cyclists if r.get('weight_kg'))}")
    print(f"  Resultater i alt:      {total_res}")

    conn.close()
    if enrich_pcs:
        br.stop_browser()


def main() -> None:
    args = sys.argv[1:]
    enrich_pcs = "--enrich-pcs" in args
    pcs_limit = None
    if enrich_pcs:
        idx = args.index("--enrich-pcs")
        if idx + 1 < len(args) and args[idx + 1].isdigit():
            pcs_limit = int(args[idx + 1])

    asyncio.run(main_async(enrich_pcs=enrich_pcs, pcs_limit=pcs_limit))


if __name__ == "__main__":
    main()
