"""
wikidata.py – Henter grunddata om danske cykelryttere fra Wikidata via SPARQL.
"""

import requests
from config import WIKIDATA_SPARQL

SPARQL_QUERY = """
SELECT ?rytter ?rytterLabel ?fødestedLabel ?fødselsdato ?koordinater ?pcsId ?kønLabel ?wikipediaArtikel
WHERE {
  ?rytter wdt:P31 wd:Q5 .
  ?rytter wdt:P106 wd:Q2309784 .
  VALUES ?nationalitet { wd:Q35 wd:Q756617 }
  ?rytter wdt:P27 ?nationalitet .

  # Kræv mindst én Wikipedia-artikel
  ?wikipediaArtikel schema:about ?rytter .
  FILTER(CONTAINS(STR(?wikipediaArtikel), "wikipedia.org"))

  OPTIONAL {
    ?rytter wdt:P19 ?fødested .
    OPTIONAL { ?fødested wdt:P625 ?koordinater . }
  }
  OPTIONAL { ?rytter wdt:P569 ?fødselsdato . }
  OPTIONAL { ?rytter wdt:P1663 ?pcsId . }
  OPTIONAL { ?rytter wdt:P21 ?køn . }
  SERVICE wikibase:label {
    bd:serviceParam wikibase:language "da,en".
  }
}
ORDER BY ?rytterLabel
"""


def fetch_cyclists() -> list[dict]:
    """
    Kører SPARQL-forespørgsel mod Wikidata og returnerer alle danske cykelryttere.

    Returns:
        Liste af dicts med felterne: wikidata_id, name, birthplace, birthdate,
        coordinates, pcs_id, gender.
    """
    headers = {
        "Accept": "application/sparql-results+json",
        "User-Agent": "DanishCyclistsProject/1.0 (educational data project)",
    }
    response = requests.get(
        WIKIDATA_SPARQL,
        params={"query": SPARQL_QUERY, "format": "json"},
        headers=headers,
        timeout=30,
    )
    response.raise_for_status()
    bindings = response.json()["results"]["bindings"]

    seen: set[str] = set()
    cyclists: list[dict] = []

    for row in bindings:
        wikidata_id = row["rytter"]["value"].split("/")[-1]

        # Dedupliker – tag første forekomst per rytter
        if wikidata_id in seen:
            continue
        seen.add(wikidata_id)

        fødselsdato_raw = row.get("fødselsdato", {}).get("value", "")
        # Wikidata returnerer ISO-datetime – behold kun dato
        fødselsdato = fødselsdato_raw[:10] if fødselsdato_raw else None

        koordinater_raw = row.get("koordinater", {}).get("value", "")
        lat, lon = _parse_coordinates(koordinater_raw)

        # Foretruk dansk Wikipedia, ellers tag første artikel
        wiki_url = row.get("wikipediaArtikel", {}).get("value")

        cyclists.append({
            "wikidata_id": wikidata_id,
            "name": row.get("rytterLabel", {}).get("value", ""),
            "birthplace": row.get("fødestedLabel", {}).get("value"),
            "birthdate": fødselsdato,
            "latitude": lat,
            "longitude": lon,
            "pcs_id": row.get("pcsId", {}).get("value"),
            "gender": row.get("kønLabel", {}).get("value"),
            "wikipedia_url": wiki_url,
        })

    return cyclists


def _parse_coordinates(coords_str: str) -> tuple[float | None, float | None]:
    """Parser Wikidata-koordinatstreng 'Point(lon lat)' til (lat, lon)."""
    if not coords_str or not coords_str.startswith("Point("):
        return None, None
    try:
        inner = coords_str[6:-1]  # fjern "Point(" og ")"
        lon_str, lat_str = inner.split()
        return float(lat_str), float(lon_str)
    except (ValueError, AttributeError):
        return None, None
