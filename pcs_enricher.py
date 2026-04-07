"""
pcs_enricher.py – Beriger rytterdata med oplysninger fra ProCyclingStats.
"""

import logging
from bs4 import BeautifulSoup

from browser import fetch_html
from config import PCS_BASE_URL

logger = logging.getLogger(__name__)

# URL til top-3 resultater på HC-niveau og over (WorldTour, Pro Series osv.)
_RESULTS_URL = (
    "{base}/rider.php?id={pcs_id}"
    "&p=results&sort=date"
    "&topx=3&ptopx=smallerorequal"
    "&level=hc"
    "&pro_win=0&pro_win=1"
    "&filter=Filter"
)


async def enrich_from_pcs(pcs_id: str) -> dict:
    """
    Henter supplerende profildata fra PCS for én rytter.

    Returns:
        Dict med felterne: weight_kg, height_m, current_team.
    """
    result = {"weight_kg": None, "height_m": None, "current_team": None}
    url = f"{PCS_BASE_URL}/rider.php?id={pcs_id}"

    try:
        html = await fetch_html(url, wait=5.0)
        soup = BeautifulSoup(html, "lxml")
        result["weight_kg"], result["height_m"] = _parse_weight_height(soup)
        result["current_team"] = _parse_team(soup)
    except Exception as e:
        logger.error(f"Fejl ved PCS-profil for id={pcs_id}: {e}")

    return result


async def fetch_top3_results(pcs_id: str) -> list[dict]:
    """
    Henter top-3 resultater (HC-niveau+) for én rytter fra PCS.

    Returns:
        Liste af dicts med: date, rank, race_name, race_url, race_class, pcs_points.
    """
    url = _RESULTS_URL.format(base=PCS_BASE_URL, pcs_id=pcs_id)
    try:
        html = await fetch_html(url, wait=5.0)
        soup = BeautifulSoup(html, "lxml")
        return _parse_results_table(soup)
    except Exception as e:
        logger.error(f"Fejl ved PCS-resultater for id={pcs_id}: {e}")
        return []


def _parse_results_table(soup: BeautifulSoup) -> list[dict]:
    """Parser resultattabellen og returnerer liste af resultater."""
    results = []
    table = soup.find("table", class_="basic")
    if not table:
        return results

    for row in table.select("tbody tr"):
        cells = row.find_all("td")
        if len(cells) < 5:
            continue
        try:
            rank_text = cells[2].get_text(strip=True)
            rank = int(rank_text) if rank_text.isdigit() else None
        except (ValueError, IndexError):
            rank = None

        race_cell = cells[3]
        race_link = race_cell.find("a")
        race_name = race_cell.get_text(strip=True)
        race_url = race_link["href"] if race_link else None

        try:
            pcs_points = int(cells[6].get_text(strip=True))
        except (ValueError, IndexError):
            pcs_points = None

        results.append({
            "date": cells[1].get_text(strip=True),
            "rank": rank,
            "race_name": race_name,
            "race_url": race_url,
            "race_class": cells[4].get_text(strip=True),
            "pcs_points": pcs_points,
        })

    return results


def _parse_weight_height(soup: BeautifulSoup) -> tuple[float | None, float | None]:
    weight_kg = None
    height_m = None
    for ul in soup.find_all("ul", class_="list"):
        li = ul.find("li")
        if li and "Weight" in li.get_text():
            texts = [d.get_text(strip=True) for d in li.find_all("div")]
            try:
                weight_kg = float(texts[texts.index("kg") - 1])
            except (ValueError, IndexError):
                pass
            try:
                height_m = float(texts[texts.index("m") - 1])
            except (ValueError, IndexError):
                pass
            break
    return weight_kg, height_m


def _parse_team(soup: BeautifulSoup) -> str | None:
    for table in soup.find_all("table", class_="basic"):
        rows = table.select("tbody tr")
        if rows:
            cells = rows[0].find_all("td")
            if len(cells) >= 2:
                team_link = cells[1].find("a")
                if team_link:
                    return team_link.get_text(strip=True)
    return None
