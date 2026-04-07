"""
browser.py – Delt browser-session via nodriver (omgår Cloudflare på PCS).
Genstarter automatisk hvis browseren crasher.
"""

import asyncio
import logging
import nodriver as uc

logger = logging.getLogger(__name__)

_browser: uc.Browser | None = None


async def _start_browser() -> uc.Browser:
    """Starter en ny browser-instans."""
    global _browser
    _browser = await uc.start(headless=False)
    return _browser


async def get_browser() -> uc.Browser:
    """Returnerer en delt browser-instans (starter/genstarter hvis nødvendigt)."""
    global _browser
    if _browser is None:
        await _start_browser()
    return _browser


async def fetch_html(url: str, wait: float = 5.0, retries: int = 2) -> str:
    """
    Henter HTML fra en URL med den delte browser.
    Genstarter browseren automatisk ved fejl og prøver igen.

    Args:
        url: Absolut URL.
        wait: Sekunder at vente på Cloudflare-challenge og sideindhold.
        retries: Antal genforsøg ved browserfejl.

    Returns:
        HTML-indhold som string.
    """
    global _browser
    last_error = None

    for attempt in range(retries + 1):
        try:
            browser = await get_browser()
            page = await browser.get(url)
            await asyncio.sleep(wait)
            return await page.get_content()
        except Exception as e:
            last_error = e
            logger.warning(f"Browser-fejl (forsøg {attempt + 1}/{retries + 1}) for {url}: {e}")
            # Dræb den gamle browser og start en ny
            try:
                if _browser is not None:
                    _browser.stop()
            except Exception:
                pass
            _browser = None
            await asyncio.sleep(2)

    raise RuntimeError(f"Kunne ikke hente {url} efter {retries + 1} forsøg: {last_error}")


def stop_browser() -> None:
    """Lukker browser-instansen."""
    global _browser
    if _browser is not None:
        try:
            _browser.stop()
        except Exception:
            pass
        _browser = None
