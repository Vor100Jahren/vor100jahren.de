"""
VOR 100 JAHREN – Step 1: Automatische Themengewinnung
=====================================================
Sammelt Zeitungsinhalte aus 13 Quellen, gewichtet sie und generiert
per Claude API 20 Themenvorschläge für einen historischen Tag.

Nutzung:
    python step1_pipeline.py 1926-02-13
    python step1_pipeline.py 1926-02-13 --output ./mein_ordner
    python step1_pipeline.py 1926-02-13 --skip ddb bne
    python step1_pipeline.py 1926-02-13 --verbose
"""

import argparse
import json
import os
import re
import sys
import time
import xml.etree.ElementTree as ET
import datetime as _dt
from datetime import datetime
from pathlib import Path

import requests
from dotenv import load_dotenv

load_dotenv()

# ─────────────────────────────────────────────
# API-Keys und Konfiguration
# ─────────────────────────────────────────────

DDB_API_KEY = os.environ.get("DDB_API_KEY", "")
NYT_API_KEY = os.environ.get("NYT_API_KEY", "")
TROVE_API_KEY = os.environ.get("TROVE_API_KEY", "")
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")

OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output")

HEADERS_DDB = {
    "Authorization": f"OAuth oauth_consumer_key={DDB_API_KEY}",
    "Accept": "application/xml",
}

HEADERS_GALLICA = {
    "User-Agent": "Vor100Jahren-Bot/1.0 (historical research)",
}

# ─────────────────────────────────────────────
# Zeitungskonfiguration
# ─────────────────────────────────────────────

GERMAN_PAPERS_SEARCH = {
    # PRIMÄR (immer abfragen):
    "Vorwärts": "Vorwärts",
    "Berliner Tageblatt": "Berliner Tageblatt",
    "Deutsche Allgemeine Zeitung": "Deutsche Allgemeine Zeitung",
    "Kölnische Zeitung": "Kölnische Zeitung",
    "Deutscher Reichsanzeiger": "Reichsanzeiger",
    # BACKUP (nachrücken bei Lücken):
    "Badische Presse": "Badische Presse",
    "Sächsische Staatszeitung": "Sächsische Staatszeitung",
    "Harburger Tageblatt": "Harburger Tageblatt",
    "Westfälischer Merkur": "Westfälischer Merkur",
    "Hamburger Echo": "Hamburger Echo",
    # ENTFERNT: Berliner Börsen-Zeitung (33% OCR, überwiegend Finanztabellen)
}

GALLICA_SERIES = {
    "Le Figaro": "cb34355551z",
    "Le Temps": "cb34431794k",
}

LOC_NEWSPAPERS = {
    # PRIMÄR:
    "sn84026749": ("The Washington Times", "The Washington Times (Washington, D.C.)", 20),
    # BACKUP (nur wenn Washington Post + Washington Times leer):
    "sn83045462": ("Evening Star", "Evening Star (Washington, D.C.)", 20),
}
LOC_BACKUP = {"sn83045462"}  # LCCNs, die nur als Fallback abgerufen werden

# Maximale Zeichen pro Quelle für den Claude-Korpus
MAX_CHARS_PER_SOURCE = 5000
MAX_CHARS_RETROSPECTIVE = 3000

# ─────────────────────────────────────────────
# Hilfsfunktionen: ALTO-XML-Parsing
# ─────────────────────────────────────────────

ALTO_NAMESPACES = [
    "http://www.loc.gov/standards/alto/ns-v2#",
    "http://www.loc.gov/standards/alto/ns-v3#",
    "http://bibnum.bnf.fr/ns/alto_prod",
    "http://schema.ccs-gmbh.com/ALTO",
]


def extract_alto_text(xml_text):
    """Extrahiert Fließtext aus ALTO-XML (alle Namespace-Varianten)."""
    if not xml_text or len(xml_text) < 50:
        return ""
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return ""

    # Namespace erkennen
    ns = ""
    for candidate in ALTO_NAMESPACES:
        if root.find(f".//{{{candidate}}}TextBlock") is not None:
            ns = candidate
            break

    if not ns:
        # Versuch ohne Namespace
        blocks = root.findall(".//TextBlock")
        if not blocks:
            return ""
        ns = None

    paragraphs = []
    if ns:
        blocks = root.findall(f".//{{{ns}}}TextBlock")
    else:
        blocks = root.findall(".//TextBlock")

    for block in blocks:
        lines = []
        if ns:
            text_lines = block.findall(f".//{{{ns}}}TextLine")
        else:
            text_lines = block.findall(".//TextLine")
        for line in text_lines:
            if ns:
                strings = line.findall(f"{{{ns}}}String")
            else:
                strings = line.findall("String")
            words = [s.get("CONTENT", "") for s in strings if s.get("CONTENT")]
            if words:
                lines.append(" ".join(words))
        if lines:
            paragraphs.append(" ".join(lines))

    return "\n\n".join(paragraphs)


# ─────────────────────────────────────────────
# Textaufbereitung: Fraktur + Segmentierung
# ─────────────────────────────────────────────


def normalize_fraktur(text):
    """Fraktur-Sonderzeichen normalisieren für Keyword-Matching.

    OCR erkennt Fraktur-Zeichen korrekt (ſ, ⸗, ꝛ), aber moderne
    Keyword-Suche scheitert daran. Ersetzungen:
      ſ → s  (langes s, häufigste Korrektur)
      ⸗ → -  (Fraktur-Trennstrich)
      ꝛ → r  (R rotunda, selten)
    """
    if not text:
        return text
    count = text.count('ſ') + text.count('⸗') + text.count('ꝛ')
    text = text.replace('ſ', 's')
    text = text.replace('⸗', '-')
    text = text.replace('ꝛ', 'r')
    if count > 0:
        print(f"    Fraktur-Normalisierung: {count} Korrekturen")
    return text


def resegment_paragraphs(text, min_len=200, max_len=3000):
    """Absätze auf sinnvolle Größe normalisieren (200-3000 Zeichen).

    Schritt 1: Monster-Absätze (>max_len) an Satzgrenzen aufbrechen.
    Schritt 2: Mikro-Absätze (<min_len) mit Nachbarn zusammenfassen.
    Seitenmarker ('--- Seite N ---') werden beibehalten.
    """
    if not text:
        return text

    paragraphs = text.split('\n\n')
    splits = 0
    merges = 0

    # Schritt 1: Monster-Absätze aufbrechen
    split_paras = []
    for para in paragraphs:
        if para.strip().startswith('--- Seite') and para.strip().endswith('---'):
            split_paras.append(para)
            continue
        if len(para) <= max_len:
            split_paras.append(para)
            continue

        remaining = para
        while len(remaining) > max_len:
            best_pos = -1
            for match in re.finditer(r'[.!?]\s+(?=[A-ZÄÖÜ])', remaining[:max_len]):
                best_pos = match.end()
            if best_pos > min_len:
                split_paras.append(remaining[:best_pos].strip())
                remaining = remaining[best_pos:].strip()
                splits += 1
            else:
                space_pos = remaining[:max_len].rfind(' ')
                if space_pos > min_len:
                    split_paras.append(remaining[:space_pos].strip())
                    remaining = remaining[space_pos:].strip()
                    splits += 1
                else:
                    split_paras.append(remaining[:max_len].strip())
                    remaining = remaining[max_len:].strip()
                    splits += 1
        if remaining.strip():
            split_paras.append(remaining.strip())

    # Schritt 2: Mikro-Absätze zusammenfassen
    merged = []
    buffer = ""
    for para in split_paras:
        if para.strip().startswith('--- Seite') and para.strip().endswith('---'):
            if buffer:
                merged.append(buffer)
                buffer = ""
            merged.append(para)
            continue
        if not buffer:
            buffer = para
        elif len(buffer) < min_len:
            buffer = buffer + " " + para
            merges += 1
        else:
            merged.append(buffer)
            buffer = para
    if buffer:
        merged.append(buffer)

    if splits > 0 or merges > 0:
        print(f"    Neusegmentierung: {splits} Splits, {merges} Merges → {len(merged)} Absätze")

    return '\n\n'.join(merged)


# ─────────────────────────────────────────────
# DDB: Deutsche Digitale Bibliothek
# ─────────────────────────────────────────────


def search_ddb_newspapers(date_str):
    """Sucht alle verfügbaren Zeitungsausgaben für ein Datum in der DDB."""
    url = "https://api.deutsche-digitale-bibliothek.de/search/index/newspaper-issues/select"
    results = {}

    for display_name, search_term in GERMAN_PAPERS_SEARCH.items():
        params = {
            "q": f'paper_title:"{search_term}" AND publication_date:[{date_str}T00:00:00Z TO {date_str}T23:59:59Z]',
            "rows": 5,
            "fl": "id,paper_title,publication_date",
        }
        try:
            r = requests.get(url, params=params, headers=HEADERS_DDB, timeout=45)
            if r.status_code == 200:
                data = r.json()
                docs = data.get("response", {}).get("docs", [])
                if docs:
                    results[display_name] = docs[0]["id"]
            time.sleep(0.3)
        except Exception as e:
            print(f"    Warnung: DDB-Suche für {display_name} fehlgeschlagen: {e}")

    return results


def fetch_mets_record(item_id, paper_name):
    """Holt den METS/MODS-Record einer DDB-Zeitungsausgabe."""
    base_id = re.sub(r"[^a-zA-Z0-9]", "", item_id)[:32]
    url = f"https://api.deutsche-digitale-bibliothek.de/items/{base_id}/source/record"
    try:
        r = requests.get(url, headers=HEADERS_DDB, timeout=45)
        if r.status_code == 200:
            return r.text
    except Exception as e:
        print(f"    Warnung: METS-Abruf für {paper_name} fehlgeschlagen: {e}")
    return ""


def extract_alto_urls_from_mets(mets_xml):
    """Extrahiert ALTO-XML-URLs aus einem METS-Record."""
    if not mets_xml:
        return []
    try:
        root = ET.fromstring(mets_xml)
    except ET.ParseError:
        return []

    ns_mets = "http://www.loc.gov/METS/"
    urls = []

    for file_grp in root.findall(f".//{{{ns_mets}}}fileGrp"):
        use = file_grp.get("USE", "")
        if "FULLTEXT" in use.upper():
            for f in file_grp.findall(f".//{{{ns_mets}}}file"):
                flocat = f.find(f"{{{ns_mets}}}FLocat")
                if flocat is not None:
                    href = flocat.get("{http://www.w3.org/1999/xlink}href", "")
                    if href and href.startswith("http"):
                        urls.append(href)

    return urls


def fetch_alto_page(url, paper_name, page_num):
    """Lädt eine einzelne ALTO-XML-Seite und extrahiert den Text."""
    try:
        r = requests.get(url, headers=HEADERS_DDB, timeout=45)
        if r.status_code == 200:
            return extract_alto_text(r.text)
    except Exception as e:
        if page_num == 1:
            print(f"    Warnung: Seite {page_num} von {paper_name}: {e}")
    return ""


def fetch_ddb_newspaper(item_id, paper_name, max_pages=3):
    """Holt Text einer DDB-Zeitung (erste N Seiten)."""
    mets_xml = fetch_mets_record(item_id, paper_name)
    if not mets_xml:
        return ""

    alto_urls = extract_alto_urls_from_mets(mets_xml)
    if not alto_urls:
        print(f"    {paper_name}: Keine ALTO-URLs gefunden")
        return ""

    if max_pages:
        alto_urls = alto_urls[:max_pages]

    all_text = []
    for i, url in enumerate(alto_urls):
        text = fetch_alto_page(url, paper_name, i + 1)
        if text:
            all_text.append(f"--- Seite {i + 1} ---\n\n{text}")
        time.sleep(0.5)

    return "\n\n".join(all_text)


def fetch_all_ddb(date_str, verbose=False):
    """Holt alle DDB-Zeitungen für ein Datum."""
    print("\n  DDB: Suche Zeitungsausgaben ...")
    paper_ids = search_ddb_newspapers(date_str)
    print(f"  DDB: {len(paper_ids)} Zeitungen gefunden")

    results = {}
    for name, item_id in paper_ids.items():
        if verbose:
            print(f"    Lade {name} ...")
        text = fetch_ddb_newspaper(item_id, name, max_pages=3)
        if text:
            results[name] = text
            print(f"    ✓ {name}: {len(text):,} Zeichen")
        else:
            print(f"    ✗ {name}: kein Text erhalten")

    return results


# ─────────────────────────────────────────────
# Gallica: Bibliothèque nationale de France
# ─────────────────────────────────────────────


def _gallica_get(url, headers, max_retries=3):
    """HTTP-GET mit Retry bei 429 (Too Many Requests)."""
    for attempt in range(max_retries):
        try:
            r = requests.get(url, headers=headers, timeout=30)
            if r.status_code == 429:
                wait = 15 * (attempt + 1)  # 15s, 30s, 45s
                print(f"    ⏳ Gallica 429 – warte {wait}s (Versuch {attempt + 1}/{max_retries})")
                time.sleep(wait)
                continue
            return r
        except Exception as e:
            if attempt < max_retries - 1:
                time.sleep(10)
                continue
            raise
    return r  # Letzten Response zurückgeben (vermutlich 429)


def fetch_gallica(series_ark, name, date_str, max_pages=20):
    """Holt eine Gallica-Zeitung (Le Figaro, Le Temps) per ALTO-XML."""
    date_compact = date_str.replace("-", "")
    lookup_url = f"https://gallica.bnf.fr/ark:/12148/{series_ark}/date{date_compact}.item"

    try:
        r = _gallica_get(lookup_url, HEADERS_GALLICA)
        if r.status_code != 200:
            print(f"    {name}: Lookup HTTP {r.status_code}")
            return ""
    except Exception as e:
        print(f"    Warnung: Gallica-Lookup für {name}: {e}")
        return ""

    ark_match = re.search(r"(bpt6k[0-9a-z]+)", r.text)
    if not ark_match:
        print(f"    {name}: Keine ARK-ID für {date_str} gefunden")
        return ""

    ark_id = ark_match.group(1)
    all_text = []

    for page in range(1, max_pages + 1):
        alto_url = f"https://gallica.bnf.fr/RequestDigitalElement?O={ark_id}&E=ALTO&Deb={page}"
        try:
            r = _gallica_get(alto_url, HEADERS_GALLICA)
            if r.status_code == 429:
                print(f"    {name}: Rate-Limit bei Seite {page}, breche ab")
                break
            if r.status_code != 200 or len(r.content) < 200:
                break
            text = extract_alto_text(r.text)
            if text and len(text) > 50:
                all_text.append(f"--- Seite {page} ---\n\n{text}")
            else:
                break
        except Exception:
            break
        time.sleep(5)  # 5s statt 2.5s – Gallica braucht mehr Abstand

    return "\n\n".join(all_text)


def fetch_all_gallica(date_str, verbose=False):
    """Holt alle Gallica-Zeitungen für ein Datum.

    Bei leeren Ergebnissen wird ein Retry nach 10 Sekunden versucht,
    da Gallica bei aufeinanderfolgenden Anfragen Rate-Limiting anwendet.
    """
    results = {}
    for name, ark in GALLICA_SERIES.items():
        if verbose:
            print(f"    Lade {name} ...")
        text = fetch_gallica(ark, name, date_str)
        if not text:
            # Retry nach Pause (Gallica Rate-Limiting)
            print(f"    ⏳ {name}: erster Versuch leer – Retry in 10s ...")
            time.sleep(10)
            text = fetch_gallica(ark, name, date_str)
        if text:
            results[name] = text
            print(f"    ✓ {name}: {len(text):,} Zeichen")
        else:
            print(f"    ✗ {name}: nicht verfügbar")
        # Pause zwischen Zeitungen gegen Rate-Limiting
        time.sleep(5)
    return results


# ─────────────────────────────────────────────
# LoC: Library of Congress – Chronicling America
# ─────────────────────────────────────────────


def merge_ocr_lines(raw_text):
    """Merged LoC-OCR-Zeilen zu Absätzen."""
    if not raw_text:
        return ""
    lines = raw_text.split("\n")
    paragraphs = []
    current = []

    for line in lines:
        stripped = line.strip()
        if not stripped:
            if current:
                paragraphs.append(" ".join(current))
                current = []
        elif len(stripped) < 15 and stripped == stripped.upper():
            if current:
                paragraphs.append(" ".join(current))
                current = []
            paragraphs.append(stripped)
        else:
            current.append(stripped)

    if current:
        paragraphs.append(" ".join(current))
    return "\n\n".join(paragraphs)


def fetch_loc_newspaper(lccn, name, date_str, max_pages=20):
    """Holt eine US-Zeitung von der Library of Congress."""
    all_text = []

    for page in range(1, max_pages + 1):
        url = f"https://www.loc.gov/resource/{lccn}/{date_str}/ed-1/?sp={page}&st=text&fo=json"
        try:
            r = requests.get(url, timeout=30)
            if r.status_code != 200:
                break
            data = r.json()

            # Volltext-Service-URL extrahieren
            fulltext_url = None
            if "segments" in data:
                for seg in data.get("segments", []):
                    if "fulltext_service" in seg:
                        fulltext_url = seg["fulltext_service"]
                        break
            if not fulltext_url and "fulltext_service" in data:
                fulltext_url = data["fulltext_service"]

            if fulltext_url:
                ft_resp = requests.get(fulltext_url, timeout=30)
                if ft_resp.status_code == 200:
                    try:
                        ft_data = ft_resp.json()
                        raw = ""
                        for key, val in ft_data.items():
                            if isinstance(val, dict) and "full_text" in val:
                                raw = val["full_text"]
                                break
                        if not raw and isinstance(ft_data, str):
                            raw = ft_data
                    except (json.JSONDecodeError, ValueError):
                        raw = ft_resp.text

                    text = merge_ocr_lines(raw)
                    if text and len(text) > 50:
                        all_text.append(f"--- Seite {page} ---\n\n{text}")
        except Exception as e:
            if page == 1:
                print(f"    Warnung: {name} Seite {page}: {e}")
            break
        time.sleep(1.0)

    return "\n\n".join(all_text)


def fetch_all_loc(date_str, verbose=False, need_backup=False):
    """Holt LoC-Zeitungen für ein Datum.

    Backup-Zeitungen (LOC_BACKUP) werden nur abgerufen, wenn need_backup=True.
    """
    results = {}
    for lccn, (short_name, full_name, max_p) in LOC_NEWSPAPERS.items():
        if lccn in LOC_BACKUP and not need_backup:
            if verbose:
                print(f"    ⏭ {full_name}: Backup – übersprungen")
            continue
        if verbose:
            print(f"    Lade {full_name} ...")
        text = fetch_loc_newspaper(lccn, full_name, date_str, max_p)
        if text:
            results[full_name] = text
            print(f"    ✓ {full_name}: {len(text):,} Zeichen")
        else:
            print(f"    ✗ {full_name}: nicht verfügbar")
    return results


# ─────────────────────────────────────────────
# NYT: New York Times Archive API
# ─────────────────────────────────────────────


def fetch_nyt_articles(date_str):
    """Holt NYT-Metadaten (Headlines, Abstracts) für ein Datum."""
    year, month, day = date_str.split("-")
    url = f"https://api.nytimes.com/svc/archive/v1/{year}/{int(month)}.json"

    try:
        r = requests.get(url, params={"api-key": NYT_API_KEY}, timeout=60)
        if r.status_code != 200:
            return "", []
    except Exception as e:
        print(f"    Warnung: NYT-Abruf fehlgeschlagen: {e}")
        return "", []

    data = r.json()
    docs = data.get("response", {}).get("docs", [])

    # Filtern nach Zieldatum
    target = f"{year}-{month}-{day}"
    day_docs = [d for d in docs if d.get("pub_date", "").startswith(target)]

    lines = []
    metadata = []
    for doc in day_docs[:80]:  # Max 80 Artikel
        headline = doc.get("headline", {}).get("main", "")
        abstract = doc.get("abstract", "")
        page = doc.get("print_page", "?")
        mat_type = doc.get("type_of_material", "")
        keywords = [kw.get("value", "") for kw in doc.get("keywords", [])]

        if headline:
            line = f"[S.{page}] [{mat_type}] {headline}"
            if abstract:
                line += f"\n  → {abstract}"
            if keywords:
                line += f"\n  Schlagwörter: {', '.join(keywords[:5])}"
            lines.append(line)
            metadata.append({
                "headline": headline,
                "abstract": abstract,
                "page": page,
                "type": mat_type,
                "keywords": keywords[:5],
            })

    return "\n\n".join(lines), metadata


# ─────────────────────────────────────────────
# BNE: Biblioteca Nacional de España – El Sol
# ─────────────────────────────────────────────


# Fester Parent-UUID für "El Sol (Madrid. 1917)" in der BNE Hemeroteca Digital
BNE_EL_SOL_PARENT = "0312db23-f8f4-4c11-98ef-fcaa99442e30"
# Erste Ausgabe: 29/11/1917 → Offset 0 in der Paginierung
BNE_EL_SOL_START = _dt.date(1917, 11, 29)


def _bne_find_issue_uuid(date_str, verbose=False):
    """
    Findet die Issue-UUID für El Sol an einem bestimmten Datum.
    Nutzt Offset-Paginierung über den parent-Parameter.
    """
    target = _dt.date.fromisoformat(date_str)
    # El Sol erschien fast täglich (~6.4 Ausgaben/Woche inkl. Sonderausgaben)
    delta_days = (target - BNE_EL_SOL_START).days
    if delta_days < 0:
        return None
    estimated_offset = max(0, int(delta_days * 0.92) - 5)

    base_url = (
        f"https://hemerotecadigital.bne.es/hd/es/results"
        f"?parent={BNE_EL_SOL_PARENT}&t=alt-asc"
    )
    target_fmt = f"{target.day}/{target.month}/{target.year}"

    # Iterative Anpassung um das Zieldatum zu finden
    for attempt in range(8):
        url = f"{base_url}&s={estimated_offset}"
        try:
            r = requests.get(url, timeout=90)
            if r.status_code != 200:
                return None
        except Exception as e:
            if verbose:
                print(f"    BNE-Anfrage fehlgeschlagen: {e}")
            return None

        name_parts = re.findall(
            r'<span class="name-part">(.*?)</span>', r.text
        )
        viewer_links = re.findall(
            r'viewer\?id=([a-f0-9-]+)', r.text
        )
        unique_viewers = list(dict.fromkeys(viewer_links))

        # name_parts Struktur: [header, title, date, pages, title, date, pages, ...]
        # Erstes Element ist der Header-Titel, dann 3er-Gruppen
        dates_found = []
        for i in range(2, len(name_parts), 3):
            dates_found.append(name_parts[i])

        if not dates_found:
            return None

        # Zieldatum in den Ergebnissen suchen
        for idx, d in enumerate(dates_found):
            if d == target_fmt and idx < len(unique_viewers):
                if verbose:
                    print(f"    El Sol: Gefunden bei Offset {estimated_offset + idx}")
                return unique_viewers[idx]

        # Datum nicht gefunden → Offset anpassen
        first_date_str = dates_found[0]
        last_date_str = dates_found[-1] if dates_found else first_date_str
        try:
            d, m, y = map(int, first_date_str.split("/"))
            first_date = _dt.date(y, m, d)
            diff = (target - first_date).days
            if diff > 10:
                estimated_offset += max(5, int(diff * 0.92))
            elif diff < -10:
                estimated_offset = max(0, estimated_offset - max(5, int(-diff * 0.92)))
            elif diff > 0:
                estimated_offset += max(1, diff - 1)
            elif diff < 0:
                estimated_offset = max(0, estimated_offset + diff - 1)
            else:
                # Erstes Datum stimmt, aber Ziel nicht in den 10 Ergebnissen
                estimated_offset += 5
        except (ValueError, IndexError):
            estimated_offset += 10

        if verbose:
            print(f"    BNE: Versuch {attempt+1}, Offset → {estimated_offset} "
                  f"(erste Ausgabe: {first_date_str})")

    return None


def fetch_bne_el_sol(date_str, max_pages=10, verbose=False):
    """
    Holt El Sol von der BNE Hemeroteca Digital.
    Neuer Ansatz: Parent-UUID → Offset-Paginierung → Issue-UUID → Text.
    """
    from bs4 import BeautifulSoup

    # Schritt 1: Issue-UUID für das Datum finden
    issue_uuid = _bne_find_issue_uuid(date_str, verbose=verbose)
    if not issue_uuid:
        print(f"    El Sol: Keine Ausgabe für {date_str} gefunden")
        return ""

    # Schritt 2: Seiten-UUIDs der Ausgabe auflösen
    # Die Issue-Seite listet Bild-UUIDs pro Seite
    issue_url = (
        f"https://hemerotecadigital.bne.es/hd/es/results"
        f"?parent={issue_uuid}&t=alt-asc"
    )
    try:
        r = requests.get(issue_url, timeout=90)
        if r.status_code != 200:
            # Fallback: Issue-UUID direkt als Text abrufen
            text_url = f"https://hemerotecadigital.bne.es/hd/es/text?id={issue_uuid}"
            r2 = requests.get(text_url, timeout=90)
            if r2.status_code == 200 and len(r2.text) > 100:
                text = BeautifulSoup(r2.text, "html.parser").get_text(separator="\n")
                return re.sub(r"\n{3,}", "\n\n", text).strip()
            return ""
    except Exception as e:
        if verbose:
            print(f"    BNE-Seiten-Anfrage fehlgeschlagen: {e}")
        return ""

    # Bild-UUIDs extrahieren (für OCR-Text-Abruf)
    page_uuids = re.findall(r'/hd/es/low\?id=([a-f0-9-]+)', r.text)
    page_uuids = list(dict.fromkeys(page_uuids))

    if not page_uuids:
        # Fallback: ganzen Issue-Text abrufen
        text_url = f"https://hemerotecadigital.bne.es/hd/es/text?id={issue_uuid}"
        try:
            r2 = requests.get(text_url, timeout=90)
            if r2.status_code == 200 and len(r2.text) > 100:
                text = BeautifulSoup(r2.text, "html.parser").get_text(separator="\n")
                return re.sub(r"\n{3,}", "\n\n", text).strip()
        except Exception:
            pass
        return ""

    if verbose:
        print(f"    El Sol: {len(page_uuids)} Seiten gefunden")

    # Schritt 3: OCR-Text pro Seite abrufen
    all_text = []
    for i, page_uuid in enumerate(page_uuids[:max_pages]):
        text_url = f"https://hemerotecadigital.bne.es/hd/es/text?id={page_uuid}"
        try:
            r = requests.get(text_url, timeout=90)
            if r.status_code == 200 and len(r.text) > 100:
                text = BeautifulSoup(r.text, "html.parser").get_text(separator="\n")
                text = re.sub(r"\n{3,}", "\n\n", text).strip()
                if text:
                    all_text.append(f"--- Seite {i + 1} ---\n\n{text}")
        except Exception:
            pass
        time.sleep(1.0)

    return "\n\n".join(all_text)


# ─────────────────────────────────────────────
# Trove (National Library of Australia)
# ─────────────────────────────────────────────

TROVE_NEWSPAPERS = {
    # Große australische Tageszeitungen mit guter OCR für die 1920er
    "The Sydney Morning Herald": 35,      # NSW, seit 1842
    "The Argus": 13,                       # Melbourne, VIC, 1848–1957
    "The Brisbane Courier": 68,            # QLD, seit 1864
}


def fetch_trove_newspapers(date_str, verbose=False):
    """
    Holt australische Zeitungstexte von Trove (NLA) für ein Datum.
    Nutzt die Trove API v3 (newspaper/article endpoint).
    Rückgabe: Dict {Zeitungsname: Text}
    """
    if not TROVE_API_KEY:
        print("    ⚠ TROVE_API_KEY nicht gesetzt – Trove wird übersprungen")
        return {}

    year, month, day = date_str.split("-")
    results = {}

    for paper_name, title_id in TROVE_NEWSPAPERS.items():
        try:
            # Trove API v3: Suche nach Datum + Zeitungstitel
            search_url = (
                f"https://api.trove.nla.gov.au/v3/result"
                f"?key={TROVE_API_KEY}"
                f"&category=newspaper"
                f"&l-title={title_id}"
                f"&date=[{date_str}TO{date_str}]"
                f"&encoding=json"
                f"&n=20"
                f"&reclevel=full"
                f"&include=articleText"
            )

            r = requests.get(search_url, timeout=30, headers={
                "User-Agent": "Vor100Jahren-Bot/1.0 (historical research)"
            })

            if r.status_code == 403:
                print(f"    ⚠ Trove API-Key ungültig (HTTP 403)")
                return {}
            if r.status_code != 200:
                if verbose:
                    print(f"    {paper_name}: HTTP {r.status_code}")
                continue

            data = r.json()

            # Artikel aus der Antwortstruktur extrahieren
            articles = []
            try:
                category = data.get("category", [{}])
                if isinstance(category, list):
                    for cat in category:
                        records = cat.get("records", {})
                        article_list = records.get("article", [])
                        articles.extend(article_list)
                elif isinstance(category, dict):
                    records = category.get("records", {})
                    articles = records.get("article", [])
            except (KeyError, TypeError):
                pass

            if not articles:
                if verbose:
                    print(f"    {paper_name}: Keine Artikel für {date_str}")
                continue

            # Artikeltexte zusammenbauen
            page_texts = []
            for i, article in enumerate(articles[:20]):
                title = article.get("heading", "")
                text = article.get("articleText", "")
                if text:
                    # HTML-Tags entfernen
                    text = re.sub(r"<[^>]+>", " ", text)
                    text = re.sub(r"\s+", " ", text).strip()
                    if len(text) > 50:
                        page_texts.append(f"[{title}]\n{text}")

            if page_texts:
                results[paper_name] = "\n\n".join(page_texts)
                if verbose:
                    print(f"    {paper_name}: {len(page_texts)} Artikel, {len(results[paper_name]):,} Zeichen")

        except Exception as e:
            print(f"    {paper_name}: Fehler – {e}")

        time.sleep(1.0)  # Rate-Limiting beachten

    return results


# ─────────────────────────────────────────────
# Pravda (archive.org)
# ─────────────────────────────────────────────

# Russische Monatsnamen (Genitiv, wie im Pravda-Header verwendet)
_RUSSIAN_MONTHS_GENITIVE = {
    1: "января", 2: "февраля", 3: "марта", 4: "апреля",
    5: "мая", 6: "июня", 7: "июля", 8: "августа",
    9: "сентября", 10: "октября", 11: "ноября", 12: "декабря",
}


def _estimate_pravda_issue_number(date_str):
    """
    Schätzt die Pravda-Ausgabenummer für ein Datum (1926).
    Kalibriert an bekannten Ankerpunkten:
      Nr. 1  = 01.01.1926
      Nr. 48 = 27.02.1926
      Nr. 50 = 02.03.1926
      Nr. 54 = 06.03.1926
    Rückgabe: geschätzte Nummer.
    """
    year, month, day = [int(x) for x in date_str.split("-")]
    target = _dt.date(year, month, day)
    # Ankerpunkt: Nr. 48 = 27.02.1926 (verifiziert)
    anchor_date = _dt.date(1926, 2, 27)
    anchor_nr = 48

    delta_days = (target - anchor_date).days
    # Pravda erschien 6x/Woche (kein Montag in dieser Periode)
    estimate = anchor_nr + int(delta_days * 6 / 7)
    return max(1, estimate)


def _pravda_find_djvutxt_filename(identifier, verbose=False):
    """
    Findet den tatsächlichen DjVuTXT-Dateinamen via archive.org Metadata-API.
    Pravda-Dateien haben kyrillische Dateinamen wie 'Правда, 1926 , № 48_djvu.txt'.
    """
    meta_url = f"https://archive.org/metadata/{identifier}/files"
    try:
        r = requests.get(meta_url, timeout=30, headers={
            "User-Agent": "Vor100Jahren-Bot/1.0 (historical research)"
        })
        if r.status_code != 200:
            return None
        files = r.json().get("result", [])
        for f in files:
            name = f.get("name", "")
            if name.endswith("_djvu.txt"):
                if verbose:
                    print(f"    DjVuTXT-Datei: {name}")
                return name
    except Exception as e:
        if verbose:
            print(f"    Metadata-Fehler: {e}")
    return None


def fetch_pravda(date_str, verbose=False):
    """
    Holt die Pravda-Ausgabe für ein Datum von archive.org.

    Strategie:
    1. Geschätzte Ausgabenummer berechnen (kalibriert)
    2. ±3 Kandidaten auf archive.org suchen
    3. DjVuTXT-Dateinamen per Metadata-API auflösen (kyrillisch!)
    4. Text herunterladen, optionale Datumsvalidierung

    Rückgabe: Text oder ""
    """
    year, month, day = [int(x) for x in date_str.split("-")]
    month_name = _RUSSIAN_MONTHS_GENITIVE.get(month, "")
    date_pattern = f"{day} {month_name} {year}"

    estimate = _estimate_pravda_issue_number(date_str)

    # ±3 Kandidaten durchsuchen (engerer Radius dank besserer Schätzung)
    candidates = list(range(max(1, estimate - 3), estimate + 4))

    # Beste Treffer sammeln (falls kein exakter Datumsmatch)
    best_text = ""
    best_nr = None

    for nr in candidates:
        try:
            query = f'collection:pravda-newspaper AND title:"Правда, {year}" AND title:"№ {nr}"'
            search_url = (
                f"https://archive.org/advancedsearch.php"
                f"?q={requests.utils.quote(query)}"
                f"&fl[]=identifier,title"
                f"&rows=5"
                f"&output=json"
            )

            r = requests.get(search_url, timeout=30, headers={
                "User-Agent": "Vor100Jahren-Bot/1.0 (historical research)"
            })
            if r.status_code != 200:
                continue

            data = r.json()
            docs = data.get("response", {}).get("docs", [])

            if not docs:
                continue

            identifier = docs[0]["identifier"]
            title = docs[0].get("title", "")

            if verbose:
                print(f"    Pravda Nr. {nr}: {identifier} – {title}")

            # DjVuTXT-Dateinamen per Metadata-API auflösen
            djvu_filename = _pravda_find_djvutxt_filename(identifier, verbose=verbose)
            if not djvu_filename:
                # Fallback: identifier-basierten Namen versuchen
                djvu_filename = f"{identifier}_djvu.txt"

            txt_url = f"https://archive.org/download/{identifier}/{requests.utils.quote(djvu_filename)}"
            r_txt = requests.get(txt_url, timeout=60, headers={
                "User-Agent": "Vor100Jahren-Bot/1.0 (historical research)"
            })

            if r_txt.status_code != 200:
                if verbose:
                    print(f"    Pravda Nr. {nr}: DjVuTXT nicht verfügbar ({r_txt.status_code})")
                continue

            text = r_txt.text
            if len(text) < 200:
                continue

            # Datum im Text validieren (Best-Effort, OCR oft zu schlecht)
            if date_pattern in text:
                if verbose:
                    print(f"    ✓ Pravda Nr. {nr}: Datum {date_pattern} bestätigt, {len(text):,} Zeichen")
                return text[:80000]

            # Falls kein Datumsmatch: Nr. == estimate als besten Treffer merken
            if nr == estimate and not best_text:
                best_text = text[:80000]
                best_nr = nr

        except Exception as e:
            if verbose:
                print(f"    Pravda Nr. {nr}: Fehler – {e}")

        time.sleep(1.5)

    # Fallback: Geschätzte Ausgabe verwenden (OCR-Datum nicht verifizierbar)
    if best_text:
        if verbose:
            print(f"    ⚠ Pravda Nr. {best_nr}: Datum nicht im OCR, verwende Schätzung ({len(best_text):,} Zeichen)")
        return best_text

    return ""


# ─────────────────────────────────────────────
# Washington Post (archive.org)
# ─────────────────────────────────────────────


def fetch_washington_post(date_str, verbose=False):
    """
    Holt die Washington Post für ein Datum von archive.org.
    Identifier-Muster: per_washington-post_YYYY-MM-DD_NNNNN
    DjVuTXT: {identifier}_djvu.txt
    Rückgabe: Text oder ""
    """
    # Suche nach exaktem Datum im Titel
    query = f'title:"Washington Post" AND title:"{date_str}"'
    search_url = (
        f"https://archive.org/advancedsearch.php"
        f"?q={requests.utils.quote(query)}"
        f"&fl[]=identifier,title"
        f"&rows=5"
        f"&output=json"
    )

    try:
        r = requests.get(search_url, timeout=30, headers={
            "User-Agent": "Vor100Jahren-Bot/1.0 (historical research)"
        })
        if r.status_code != 200:
            return ""

        data = r.json()
        docs = data.get("response", {}).get("docs", [])
        if not docs:
            if verbose:
                print(f"    Washington Post: Keine Ausgabe für {date_str}")
            return ""

        identifier = docs[0]["identifier"]
        if verbose:
            print(f"    Washington Post: {identifier}")

        # DjVuTXT herunterladen
        txt_url = f"https://archive.org/download/{identifier}/{identifier}_djvu.txt"
        r_txt = requests.get(txt_url, timeout=60, headers={
            "User-Agent": "Vor100Jahren-Bot/1.0 (historical research)"
        })

        if r_txt.status_code != 200:
            if verbose:
                print(f"    Washington Post: DjVuTXT nicht verfügbar")
            return ""

        text = r_txt.text
        if len(text) > 80000:
            text = text[:80000]

        return text

    except Exception as e:
        if verbose:
            print(f"    Washington Post: Fehler – {e}")
        return ""


# ─────────────────────────────────────────────
# North China Herald (archive.org)
# ─────────────────────────────────────────────


def fetch_north_china_herald(date_str, verbose=False):
    """
    Holt den North China Herald für ein Datum von archive.org.
    Wochenzeitung (samstags) – sucht exaktes Datum und ±7 Tage.
    Identifier-Muster: north-china-herald-YYYY.MM.DD
    DjVuTXT: YYYY.MM.DD_djvu.txt
    Rückgabe: (Text, tatsächliches Datum) oder ("", "")
    """
    from datetime import timedelta

    year, month, day = [int(x) for x in date_str.split("-")]
    target = datetime(year, month, day)

    # Kandidaten: exaktes Datum + ±7 Tage (Wochenblatt)
    candidates = [target + timedelta(days=d) for d in [0, -1, 1, -2, 2, -3, 3, -4, 4, -5, 5, -6, 6, -7, 7]]

    for candidate in candidates:
        date_dotted = candidate.strftime("%Y.%m.%d")
        identifier = f"north-china-herald-{date_dotted}"

        try:
            # Direkt versuchen – die Identifier sind deterministisch
            txt_url = f"https://archive.org/download/{identifier}/{date_dotted}_djvu.txt"
            r = requests.get(txt_url, timeout=60, headers={
                "User-Agent": "Vor100Jahren-Bot/1.0 (historical research)"
            })

            if r.status_code == 200 and len(r.text) > 500:
                text = r.text
                actual_date = candidate.strftime("%Y-%m-%d")
                if verbose:
                    print(f"    NCH: {identifier} ({len(text):,} Zeichen)")
                if actual_date != date_str:
                    print(f"    ℹ North China Herald: nächste Ausgabe {actual_date} (Wochenblatt)")

                if len(text) > 80000:
                    text = text[:80000]
                return text

        except Exception:
            pass

        time.sleep(0.5)

    if verbose:
        print(f"    North China Herald: Keine Ausgabe nahe {date_str}")
    return ""


# ─────────────────────────────────────────────
# ANNO – Austrian Newspapers Online (ÖNB)
# ─────────────────────────────────────────────

# Kürzel → (Anzeigename, max_pages)
ANNO_NEWSPAPERS = {
    # PRIMÄR:
    "nfp": ("Neue Freie Presse", 20),
    # BACKUP (nur wenn NFP leer):
    "wrz": ("Wiener Zeitung", 12),
}
ANNO_BACKUP = {"wrz"}  # Kürzel, die nur als Fallback abgerufen werden


def fetch_anno_newspaper(aid, date_str, max_pages=20, verbose=False):
    """
    Holt Zeitungstext von ANNO (ÖNB) für ein Datum.
    API: https://anno.onb.ac.at/cgi-content/annoshow?text={aid}|{datum}|{seite}
    Rückgabe: Text oder ""
    """
    date_compact = date_str.replace("-", "")
    all_text = []

    for page in range(1, max_pages + 1):
        url = f"https://anno.onb.ac.at/cgi-content/annoshow?text={aid}|{date_compact}|{page}"
        try:
            # Kein expliziter User-Agent – ANNO blockiert Bot-Kennungen (403)
            r = requests.get(url, timeout=30)
            if r.status_code != 200 or len(r.text) < 100:
                break
            all_text.append(f"--- Seite {page} ---\n\n{r.text}")
        except Exception as e:
            if verbose:
                print(f"    ANNO {aid} Seite {page}: {e}")
            break
        time.sleep(0.5)

    if not all_text:
        return ""

    text = "\n\n".join(all_text)
    if len(text) > 80000:
        text = text[:80000]
    return text


def fetch_all_anno(date_str, verbose=False, need_backup=False):
    """Holt alle ANNO-Zeitungen. Backup-Titel nur wenn need_backup=True."""
    results = {}
    for aid, (name, max_pages) in ANNO_NEWSPAPERS.items():
        if aid in ANNO_BACKUP and not need_backup:
            if verbose:
                print(f"    ⏭ {name}: Backup – übersprungen")
            continue
        text = fetch_anno_newspaper(aid, date_str, max_pages, verbose)
        if text:
            results[name] = text
            print(f"    ✓ {name}: {len(text):,} Zeichen, {text.count('--- Seite')} Seiten")
        else:
            print(f"    ✗ {name}: nicht verfügbar für {date_str}")
    return results


# ─────────────────────────────────────────────
# Delpher – Koninklijke Bibliotheek (NL)
# ─────────────────────────────────────────────

DELPHER_NEWSPAPERS = {
    "Nieuwe Rotterdamsche Courant": "Nieuwe Rotterdamsche Courant",
}


def fetch_delpher_newspaper(paper_title, date_str, max_articles=30, verbose=False):
    """
    Holt Zeitungsartikel von Delpher (KB.nl) via SRU-API.

    1. SRU-Suche nach Datum + Zeitungstitel → Artikel-URNs
    2. OCR-Text pro Artikel via resolver.kb.nl

    Rückgabe: Text oder ""
    """
    # SRU-Suche
    sru_query = f'date={date_str} AND papertitle exact "{paper_title}"'
    sru_url = (
        f"https://jsru.kb.nl/sru/sru"
        f"?version=1.2"
        f"&operation=searchRetrieve"
        f"&x-collection=DDD_artikel"
        f"&query={requests.utils.quote(sru_query)}"
        f"&maximumRecords={max_articles}"
        f"&recordSchema=dcx"
    )

    try:
        r = requests.get(sru_url, timeout=30, headers={
            "User-Agent": "Vor100Jahren-Bot/1.0 (historical research)"
        })
        if r.status_code != 200:
            if verbose:
                print(f"    Delpher SRU: HTTP {r.status_code}")
            return ""
    except Exception as e:
        if verbose:
            print(f"    Delpher SRU: {e}")
        return ""

    # URNs extrahieren (Format: http://resolver.kb.nl/resolve?urn=...:ocr)
    urns = re.findall(r'<dc:identifier>(http://resolver\.kb\.nl/resolve\?urn=[^<]+:ocr)</dc:identifier>', r.text)
    titles = re.findall(r'<dc:title>([^<]+)</dc:title>', r.text)

    if not urns:
        if verbose:
            print(f"    Delpher: Keine Artikel für {paper_title} am {date_str}")
        return ""

    # OCR-Text pro Artikel abrufen
    all_text = []
    for i, urn_url in enumerate(urns[:max_articles]):
        try:
            r_ocr = requests.get(urn_url, timeout=30, headers={
                "User-Agent": "Vor100Jahren-Bot/1.0 (historical research)"
            })
            if r_ocr.status_code == 200 and len(r_ocr.text) > 50:
                # XML-Tags entfernen, Text extrahieren
                text = r_ocr.text
                # <title>...</title> und <p>...</p> extrahieren
                article_title = ""
                title_match = re.search(r'<title>([^<]+)</title>', text)
                if title_match:
                    article_title = title_match.group(1)
                paragraphs = re.findall(r'<p>([^<]+)</p>', text)
                if paragraphs:
                    article_text = "\n".join(paragraphs)
                    if len(article_text) > 50:
                        header = f"[{article_title}]" if article_title else ""
                        all_text.append(f"{header}\n{article_text}" if header else article_text)
        except Exception:
            pass
        time.sleep(0.3)  # Rate-Limiting

    if not all_text:
        return ""

    text = "\n\n".join(all_text)
    if len(text) > 80000:
        text = text[:80000]
    return text


def fetch_all_delpher(date_str, verbose=False):
    """Holt alle Delpher-Zeitungen für ein Datum."""
    results = {}
    for display_name, search_title in DELPHER_NEWSPAPERS.items():
        text = fetch_delpher_newspaper(search_title, date_str, verbose=verbose)
        if text:
            results[display_name] = text
            print(f"    ✓ {display_name}: {len(text):,} Zeichen")
        else:
            print(f"    ✗ {display_name}: nicht verfügbar für {date_str}")
    return results


# ─────────────────────────────────────────────
# Wikipedia OnThisDay API
# ─────────────────────────────────────────────


def fetch_wikipedia_onthisday(date_str):
    """Holt historische Ereignisse von Wikipedia OnThisDay (deutsch)."""
    _, month, day = date_str.split("-")
    url = f"https://api.wikimedia.org/feed/v1/wikipedia/de/onthisday/all/{month}/{day}"

    try:
        r = requests.get(url, headers={"User-Agent": "Vor100Jahren-Bot/1.0"}, timeout=45)
        if r.status_code != 200:
            return ""
    except Exception as e:
        print(f"    Warnung: Wikipedia OnThisDay: {e}")
        return ""

    data = r.json()
    target_year = int(date_str[:4])
    lines = []

    for section in ["events", "births", "deaths"]:
        entries = data.get(section, [])
        for entry in entries:
            year = entry.get("year", 9999)
            if year and year <= target_year:
                text = entry.get("text", "")
                if text:
                    label = {"events": "Ereignis", "births": "Geburt", "deaths": "Tod"}
                    lines.append(f"[{label[section]} {year}] {text}")

    return "\n".join(lines)


# ─────────────────────────────────────────────
# LeMO Chronik (Deutsches Historisches Museum)
# ─────────────────────────────────────────────


def fetch_lemo_chronik(date_str):
    """Holt Monatseinträge aus der LeMO-Jahreschronik."""
    from bs4 import BeautifulSoup

    year = date_str[:4]
    month_num = int(date_str[5:7])
    month_names = [
        "", "Januar", "Februar", "März", "April", "Mai", "Juni",
        "Juli", "August", "September", "Oktober", "November", "Dezember",
    ]
    target_month = month_names[month_num]

    url = f"https://www.dhm.de/lemo/jahreschronik/{year}"
    try:
        r = requests.get(url, timeout=20)
        if r.status_code != 200:
            return ""
    except Exception as e:
        print(f"    Warnung: LeMO-Abruf: {e}")
        return ""

    soup = BeautifulSoup(r.text, "html.parser")
    text = soup.get_text(separator="\n")

    # Monatsabschnitt extrahieren
    lines = text.split("\n")
    in_section = False
    section_lines = []

    for line in lines:
        stripped = line.strip()
        if target_month.lower() in stripped.lower() and len(stripped) < 40:
            in_section = True
            section_lines.append(stripped)
            continue
        if in_section:
            # Nächster Monatsname = Ende
            is_next_month = False
            for m in month_names[1:]:
                if m != target_month and m.lower() in stripped.lower() and len(stripped) < 40:
                    is_next_month = True
                    break
            if is_next_month:
                break
            if stripped:
                section_lines.append(stripped)

    return "\n".join(section_lines)


# ─────────────────────────────────────────────
# Phase 1: Alle Quellen abrufen
# ─────────────────────────────────────────────


def fetch_all_sources(date_str, skip_sources=None, verbose=False):
    """
    Orchestriert den Abruf aller Quellen.
    Gibt ein Dictionary zurück: {source_name: text}
    """
    skip = set(skip_sources or [])
    sources = {}
    failed = []

    # --- DDB ---
    if "ddb" not in skip:
        print("\n[ 1/13] DDB – Deutsche Digitale Bibliothek")
        try:
            ddb_results = fetch_all_ddb(date_str, verbose)
            for name, text in ddb_results.items():
                sources[f"ddb:{name}"] = text
        except Exception as e:
            print(f"  FEHLER: DDB gesamt: {e}")
            failed.append("ddb")
    else:
        print("\n[ 1/13] DDB – übersprungen")

    # --- Gallica ---
    if "gallica" not in skip:
        print("\n[ 2/13] Gallica – Bibliothèque nationale de France")
        try:
            gallica_results = fetch_all_gallica(date_str, verbose)
            for name, text in gallica_results.items():
                sources[f"gallica:{name}"] = text
        except Exception as e:
            print(f"  FEHLER: Gallica gesamt: {e}")
            failed.append("gallica")
    else:
        print("\n[ 2/13] Gallica – übersprungen")

    # --- LoC (Washington Times primär, Evening Star Backup) ---
    if "loc" not in skip:
        print("\n[ 3/13] LoC – Library of Congress (Washington Times)")
        try:
            loc_results = fetch_all_loc(date_str, verbose, need_backup=False)
            for name, text in loc_results.items():
                sources[f"loc:{name}"] = text
            # Falls Washington Times leer → Evening Star als Backup
            if not any(k.startswith("loc:") for k in sources):
                print("    ↳ Washington Times leer – aktiviere Evening Star als Backup ...")
                try:
                    backup_results = fetch_all_loc(date_str, verbose, need_backup=True)
                    for name, text in backup_results.items():
                        sources[f"loc:{name}"] = text
                except Exception:
                    pass
        except Exception as e:
            print(f"  FEHLER: LoC gesamt: {e}")
            failed.append("loc")
    else:
        print("\n[ 3/13] LoC – übersprungen")

    # --- NYT ---
    if "nyt" not in skip:
        print("\n[ 4/13] NYT – New York Times Archive")
        try:
            if not NYT_API_KEY:
                print("  Warnung: NYT_API_KEY nicht gesetzt")
                failed.append("nyt")
            else:
                nyt_text, _ = fetch_nyt_articles(date_str)
                if nyt_text:
                    sources["nyt:New York Times"] = nyt_text
                    print(f"    ✓ NYT: {len(nyt_text):,} Zeichen")
                else:
                    print(f"    ✗ NYT: keine Artikel für {date_str}")
        except Exception as e:
            print(f"  FEHLER: NYT: {e}")
            failed.append("nyt")
    else:
        print("\n[ 4/13] NYT – übersprungen")

    # --- Washington Post (archive.org) ---
    if "wapo" not in skip:
        print("\n[ 5/13] Washington Post – archive.org")
        try:
            wapo_text = fetch_washington_post(date_str, verbose)
            if wapo_text:
                sources["wapo:Washington Post"] = wapo_text
                print(f"    ✓ Washington Post: {len(wapo_text):,} Zeichen")
            else:
                print(f"    ✗ Washington Post: nicht verfügbar für {date_str}")
                # Evening Star als Backup aktivieren
                if "loc" not in skip and not any(k.startswith("loc:") for k in sources):
                    print("    ↳ Aktiviere Evening Star als Backup ...")
                    try:
                        backup_results = fetch_all_loc(date_str, verbose, need_backup=True)
                        for name, text in backup_results.items():
                            if name not in [v for k, v in sources.items() if k.startswith("loc:")]:
                                sources[f"loc:{name}"] = text
                    except Exception:
                        pass
        except Exception as e:
            print(f"  FEHLER: Washington Post: {e}")
            failed.append("wapo")
    else:
        print("\n[ 5/13] Washington Post – übersprungen")

    # --- BNE ---
    if "bne" not in skip:
        print("\n[ 6/13] BNE – Hemeroteca Digital (El Sol)")
        try:
            bne_text = fetch_bne_el_sol(date_str)
            if bne_text:
                sources["bne:El Sol"] = bne_text
                print(f"    ✓ El Sol: {len(bne_text):,} Zeichen")
            else:
                print(f"    ✗ El Sol: nicht verfügbar für {date_str}")
        except Exception as e:
            print(f"  FEHLER: BNE: {e}")
            failed.append("bne")
    else:
        print("\n[ 6/13] BNE – übersprungen")

    # --- ANNO (Neue Freie Presse primär, Wiener Zeitung Backup) ---
    if "anno" not in skip:
        print("\n[ 7/13] ANNO – Österreichische Nationalbibliothek")
        try:
            anno_results = fetch_all_anno(date_str, verbose, need_backup=False)
            for name, text in anno_results.items():
                sources[f"anno:{name}"] = text
            if not anno_results:
                # Wiener Zeitung als Backup
                print("    ↳ NFP leer – aktiviere Wiener Zeitung als Backup ...")
                anno_backup = fetch_all_anno(date_str, verbose, need_backup=True)
                for name, text in anno_backup.items():
                    sources[f"anno:{name}"] = text
        except Exception as e:
            print(f"  FEHLER: ANNO: {e}")
            failed.append("anno")
    else:
        print("\n[ 7/13] ANNO – übersprungen")

    # --- Delpher (NRC) ---
    if "delpher" not in skip:
        print("\n[ 8/13] Delpher – Koninklijke Bibliotheek (NL)")
        try:
            delpher_results = fetch_all_delpher(date_str, verbose)
            for name, text in delpher_results.items():
                sources[f"delpher:{name}"] = text
        except Exception as e:
            print(f"  FEHLER: Delpher: {e}")
            failed.append("delpher")
    else:
        print("\n[ 8/13] Delpher – übersprungen")

    # --- Trove ---
    if "trove" not in skip:
        print("\n[ 9/13] Trove – National Library of Australia")
        try:
            trove_results = fetch_trove_newspapers(date_str, verbose)
            for name, text in trove_results.items():
                sources[f"trove:{name}"] = text
            if trove_results:
                total_chars = sum(len(t) for t in trove_results.values())
                print(f"    ✓ Trove: {len(trove_results)} Zeitungen, {total_chars:,} Zeichen")
            else:
                print(f"    ✗ Trove: keine Ergebnisse für {date_str}")
        except Exception as e:
            print(f"  FEHLER: Trove: {e}")
            failed.append("trove")
    else:
        print("\n[ 9/13] Trove – übersprungen")

    # --- North China Herald (archive.org) ---
    if "nch" not in skip:
        print("\n[10/13] North China Herald – archive.org")
        try:
            nch_text = fetch_north_china_herald(date_str, verbose)
            if nch_text:
                sources["nch:North China Herald"] = nch_text
                print(f"    ✓ North China Herald: {len(nch_text):,} Zeichen")
            else:
                print(f"    ✗ North China Herald: nicht verfügbar nahe {date_str}")
        except Exception as e:
            print(f"  FEHLER: North China Herald: {e}")
            failed.append("nch")
    else:
        print("\n[10/13] North China Herald – übersprungen")

    # --- Pravda ---
    if "pravda" not in skip:
        print("\n[11/13] Pravda – archive.org")
        try:
            pravda_text = fetch_pravda(date_str, verbose)
            if pravda_text:
                sources["pravda:Pravda"] = pravda_text
                print(f"    ✓ Pravda: {len(pravda_text):,} Zeichen")
            else:
                print(f"    ✗ Pravda: nicht verfügbar für {date_str}")
        except Exception as e:
            print(f"  FEHLER: Pravda: {e}")
            failed.append("pravda")
    else:
        print("\n[11/13] Pravda – übersprungen")

    # --- Wikipedia ---
    if "wikipedia" not in skip:
        print("\n[12/13] Wikipedia OnThisDay")
        try:
            wiki_text = fetch_wikipedia_onthisday(date_str)
            if wiki_text:
                sources["retro:Wikipedia OnThisDay"] = wiki_text
                print(f"    ✓ Wikipedia: {len(wiki_text):,} Zeichen")
            else:
                print(f"    ✗ Wikipedia: keine Einträge")
        except Exception as e:
            print(f"  FEHLER: Wikipedia: {e}")
            failed.append("wikipedia")
    else:
        print("\n[12/13] Wikipedia – übersprungen")

    # --- LeMO ---
    if "lemo" not in skip:
        print("\n[13/13] LeMO Chronik")
        try:
            lemo_text = fetch_lemo_chronik(date_str)
            if lemo_text:
                sources["retro:LeMO Chronik"] = lemo_text
                print(f"    ✓ LeMO: {len(lemo_text):,} Zeichen")
            else:
                print(f"    ✗ LeMO: keine Einträge für den Monat")
        except Exception as e:
            print(f"  FEHLER: LeMO: {e}")
            failed.append("lemo")
    else:
        print("\n[13/13] LeMO – übersprungen")

    return sources, failed


# ─────────────────────────────────────────────
# Phase 2: Korpus-Aufbereitung mit Gewichtung
# ─────────────────────────────────────────────


def truncate_text(text, max_chars):
    """Kürzt Text auf max_chars, bevorzugt Anfang (Titelseite)."""
    if len(text) <= max_chars:
        return text
    # Am letzten Absatzende vor dem Limit schneiden
    truncated = text[:max_chars]
    last_para = truncated.rfind("\n\n")
    if last_para > max_chars * 0.7:
        return truncated[:last_para] + "\n\n[... gekürzt]"
    return truncated + "\n\n[... gekürzt]"


def sample_text_distributed(text, max_chars=8000):
    """Extrahiert eine repräsentative Stichprobe aus dem vollen Zeitungstext.

    Statt nur die ersten N Zeichen (Titelseite) zu nehmen, wird über die
    gesamte Ausgabe gestreut: Absatzanfänge und markante Textpassagen
    aus allen Teilen der Zeitung werden erfasst.

    Strategie:
    1. Erste 3000 Zeichen (Titelseite, wichtigste Nachrichten)
    2. Absatzanfänge aus dem Rest (je 200 Zeichen pro Absatz, gleichmäßig verteilt)
    3. Seitenüberschriften (--- Seite N ---) werden erhalten

    So werden auch Kultur-, Wissenschafts- und Sportmeldungen von Seite 3+
    in die Themenidentifikation einbezogen.
    """
    if len(text) <= max_chars:
        return text

    parts = []

    # 1. Titelseite: erste 3000 Zeichen
    front_page_budget = min(3000, max_chars // 3)
    front = text[:front_page_budget]
    last_para = front.rfind("\n\n")
    if last_para > front_page_budget * 0.7:
        front = front[:last_para]
    parts.append(front)
    chars_used = len(front)

    # 2. Rest des Textes in Absätze aufteilen
    remainder = text[front_page_budget:]
    paragraphs = [p.strip() for p in remainder.split("\n\n") if p.strip()]

    # Seitenmarker separat erfassen
    page_markers = [p for p in paragraphs if p.startswith("--- Seite")]

    # Substanzielle Absätze (>80 Zeichen, keine reinen Seitenmarker)
    content_paragraphs = [p for p in paragraphs
                          if len(p) > 80 and not p.startswith("--- Seite")]

    if not content_paragraphs:
        return front + "\n\n[... gekürzt]"

    # 3. Gleichmäßig verteilt Absatzanfänge sampeln
    remaining_budget = max_chars - chars_used - 100  # Reserve für Marker
    chars_per_sample = 200  # Anfang jedes Absatzes
    max_samples = remaining_budget // chars_per_sample

    # Gleichmäßig über den Text verteilt sampeln
    step = max(1, len(content_paragraphs) // max_samples)
    sampled = content_paragraphs[::step][:max_samples]

    for para in sampled:
        snippet = para[:chars_per_sample]
        # Am Wortende schneiden
        if len(para) > chars_per_sample:
            last_space = snippet.rfind(" ")
            if last_space > chars_per_sample * 0.7:
                snippet = snippet[:last_space] + " …"
            else:
                snippet += " …"
        parts.append(snippet)

    result = "\n\n".join(parts)
    return result[:max_chars]


def build_corpus_for_claude(sources, date_str):
    """
    Baut den gewichteten Korpus für den Claude-Prompt auf.
    Primäre Zeitungsquellen werden hervorgehoben, retrospektive herabgestuft.
    """
    primary_sections = []
    nyt_section = ""
    retro_sections = []
    stats = {
        "primary_sources": 0,
        "primary_chars": 0,
        "nyt_chars": 0,
        "retrospective_sources": 0,
        "retrospective_chars": 0,
    }

    for source_key, text in sorted(sources.items()):
        prefix, name = source_key.split(":", 1)

        # Textaufbereitung: Fraktur-Normalisierung + Absatz-Neusegmentierung
        # "primary" ist der generische Prefix für Quellen aus dem gespeicherten Corpus
        if prefix in ("ddb", "gallica", "loc", "bne", "trove", "pravda", "wapo", "nch", "anno", "delpher", "primary"):
            text = normalize_fraktur(text)
            text = resegment_paragraphs(text)
            # Verteilte Stichprobe statt einfacher Kürzung auf Titelseite
            sampled = sample_text_distributed(text, max_chars=8000)
            primary_sections.append(f"\n### {name}\n{sampled}")
            stats["primary_sources"] += 1
            stats["primary_chars"] += len(sampled)

        elif prefix == "nyt":
            nyt_section = truncate_text(text, MAX_CHARS_PER_SOURCE)
            stats["nyt_chars"] = len(nyt_section)

        elif prefix == "retro":
            sampled = sample_text_distributed(text, max_chars=MAX_CHARS_RETROSPECTIVE)
            retro_sections.append(f"\n### {name}\n{sampled}")
            stats["retrospective_sources"] += 1
            stats["retrospective_chars"] += len(sampled)

    # Korpus zusammenbauen
    corpus_parts = []

    corpus_parts.append(
        "=== PRIMÄRE ZEITUNGSQUELLEN (HAUPTGEWICHT) ===\n"
        "Die folgenden Texte stammen direkt aus Zeitungsausgaben des Tages.\n"
        "Diese Quellen haben das höchste Gewicht bei der Themenauswahl.\n"
    )
    corpus_parts.extend(primary_sections)

    if nyt_section:
        corpus_parts.append(
            "\n\n=== NYT-METADATEN (SEKUNDÄR) ===\n"
            "Nur Schlagzeilen und Zusammenfassungen, kein Volltext.\n"
        )
        corpus_parts.append(nyt_section)

    if retro_sections:
        corpus_parts.append(
            "\n\n=== RETROSPEKTIVE QUELLEN (NUR ERGÄNZUNG) ===\n"
            "Diese Quellen dienen nur zur Kontextualisierung.\n"
            "Sie sollen NICHT als Thementreiber verwendet werden.\n"
        )
        corpus_parts.extend(retro_sections)

    corpus_text = "\n".join(corpus_parts)
    stats["total_chars"] = len(corpus_text)
    return corpus_text, stats


# ─────────────────────────────────────────────
# Phase 3: Claude API – Themenvorschläge
# ─────────────────────────────────────────────

SYSTEM_PROMPT = """Du bist ein erfahrener historischer Zeitungsanalyst, spezialisiert auf die 1920er Jahre.
Deine Aufgabe ist es, aus einem Tageskorpus historischer Zeitungsquellen die wichtigsten Themen
zu identifizieren und als Vorschläge für eine moderne Nachrichtenausgabe im Stil der 1920er aufzubereiten.

Regeln:
1. Themen, die in mehreren PRIMÄREN Zeitungsquellen erscheinen, haben höchste Priorität.
2. Retrospektive Quellen (Wikipedia, LeMO) dienen NUR zur Kontextualisierung, nicht als Thementreiber.
3. Jeder Vorschlag braucht mindestens eine primäre Zeitungsquelle als Beleg.
4. Die Vorschläge sollen den journalistischen Stil der Weimarer Republik widerspiegeln.
5. Antworte AUSSCHLIESSLICH mit einem validen JSON-Objekt, kein weiterer Text.

MEHRSPRACHIGER KORPUS:
Der Quellenkorpus enthält Zeitungstexte in SECHS Sprachen: Deutsch, Französisch, Englisch,
Spanisch, Russisch und Niederländisch. Die Texte liegen als OCR-Rohdaten vor und können
Zeichenfehler enthalten. Du MUSST alle Sprachen aktiv auswerten:
- Lies die französischen Quellen (Le Figaro, Le Temps) und identifiziere deren Themen.
- Lies die englischen Quellen (The Washington Times, Washington Post, North China Herald) und identifiziere deren Themen.
- Lies die spanischen (El Sol), russischen (Pravda) und niederländischen (Nieuwe Rotterdamsche Courant) Quellen.
- Themen, die SPRACHÜBERGREIFEND in mehreren Quellen auftauchen, sind besonders relevant.
- Themen, die NUR in fremdsprachigen Quellen erscheinen (z.B. ein Ereignis nur in Le Figaro
  und The Washington Times, nicht in deutschen Zeitungen), sind trotzdem gültige Vorschläge.
- Benenne im "sources"-Feld ALLE Quellen, in denen ein Thema erscheint, unabhängig von der Sprache."""

USER_PROMPT_TEMPLATE = """Historisches Datum: {date_historical} ({weekday})
Modernes Datum: {date_modern}

Quellenübersicht:
- Primäre Zeitungsquellen: {primary_count} Zeitungen ({primary_chars:,} Zeichen)
- NYT-Metadaten: {nyt_chars:,} Zeichen
- Retrospektive Quellen: {retro_count} ({retro_chars:,} Zeichen)

{corpus}

───────────────────────────────────────
AUFGABE: Erstelle genau 30 Themenvorschläge.

RANKING-REGELN:
- Themen in 5+ Primärquellen → höchster Rang
- Themen in 3-4 Primärquellen → mittlerer Rang
- Themen in 1-2 Primärquellen → niedrigerer Rang
- Themen NUR aus retrospektiven Quellen → niedrigster Rang (nur als Ergänzung)

ARTIKELTYPEN:
- "Hauptartikel": Große Tagesgeschichte, 800-1200 Wörter (2-4 pro Tag)
- "Artikel": Reguläre Nachricht, 400-700 Wörter (8-10 pro Tag)
- "Kurzbeitrag": Kurzmeldung, 150-300 Wörter (6-8 pro Tag)

KATEGORIEN (14 Kategorien):
- Diplomatie: Völkerbund, internationale Verträge, Konferenzen, Abrüstungsverhandlungen
- Innenpolitik: Regierung, Parlament, Parteien, Verfassungsfragen
- International: Auslandsnachrichten, die keine Diplomatie sind (Bürgerkriege, Unruhen, Wahlen im Ausland)
- Wirtschaft: NUR nachrichtenrelevante Wirtschaftsthemen (Handelsabkommen, Industriepolitik,
  Reparationen, Zollfragen, Wirtschaftskrisen). KEINE reinen Börsenberichte, Kursnotizen,
  Devisentabellen oder Handelsstatistiken – diese sind NICHT als Themen geeignet.
- Arbeit & Soziales: Gewerkschaften, Streiks, Lohnkämpfe, Arbeitslosigkeit, Sozialpolitik, Wohnungsnot
- Justiz & Kriminalität: Gerichtsprozesse, Kriminalfälle, Gesetzgebung
- Feuilleton: Theater, Musik, Kunst, Film, Literatur, Ausstellungen, Premieren, Kritiken
- Wissenschaft & Technik: Erfindungen, Entdeckungen, Medizin, Luftfahrt, Radio/Rundfunk, Expeditionen
- Sport: Fußball, Boxen, Leichtathletik, Turnen, Pferderennen, Radsport, Olympia
- Gesellschaft: Alltagsleben, Mode, gesellschaftliche Debatten, Prominente
- Militär & Sicherheit: Reichswehr, Abrüstung, Polizei, Sicherheitspolitik
- Kolonien & Übersee: Mandatsgebiete, Kolonialpolitik, Übersee-Nachrichten
- Religion & Kirche: Kirchenpolitik, Vatikan, Kirchenkonflikte, religiöse Debatten
- Vermischtes: Kuriositäten, Unglücke, Wetter, Rekorde, Allerlei

PFLICHT-MINDESTQUOTEN (30 Vorschläge):
- Feuilleton: mindestens 3 Vorschläge
- Wissenschaft & Technik: mindestens 3 Vorschläge
- Sport: mindestens 2 Vorschläge
- Arbeit & Soziales: mindestens 2 Vorschläge
Durchsuche den Korpus GEZIELT nach Meldungen aus den hinteren Zeitungsseiten
(Feuilleton, Sport, Wissenschaft), die über die Titelseiten-Nachrichten hinausgehen.
Die restlichen 20 Vorschläge verteilen sich frei nach Quellenlage.

SEARCH_KEYWORDS:
Für jedes Thema MUSST du 15-25 mehrsprachige Suchbegriffe liefern, die im
OCR-Zeitungskorpus vorkommen könnten. Diese werden für die Kontextextraktion
in Step 2 verwendet. Der Korpus enthält Zeitungen in SECHS Sprachen – für jede
Sprache MÜSSEN relevante Begriffe enthalten sein! Berücksichtige:
- Deutsche Begriffe (z.B. "Völkerbund", "Stresemann", "Reichstag")
- Französische Begriffe (z.B. "Société des Nations", "Chambre", "confiance")
- Englische Begriffe (z.B. "League of Nations", "coal", "Parliament")
- Russische Begriffe in Kyrillisch (z.B. "Лига Наций", "Германия", "рейхстаг")
- Niederländische Begriffe (z.B. "Volkenbond", "kabinet", "verkiezingen")
- Spanische Begriffe (z.B. "Sociedad de Naciones", "gobierno", "canciller")
- Ortsnamen, Personennamen, OCR-typische Varianten (z.B. "Volkerbund" ohne Umlaut)
- Verwandte Begriffe (z.B. "Trianon" und "Revision" für Ungarn-Themen)

Antworte mit diesem JSON-Format:
{{
  "proposals": [
    {{
      "rank": 1,
      "topic": "Kurzer Thementitel",
      "headline_suggestion": "Vorgeschlagene Schlagzeile im 1920er-Stil",
      "category": "Kategorie",
      "source_count": 8,
      "sources": ["Vorwärts", "Berliner Tageblatt", ...],
      "snippet": "Kurzer Beleg-Auszug aus dem Korpus (max 100 Wörter)",
      "suggested_type": "Hauptartikel",
      "estimated_words": "800-1200",
      "search_keywords": ["Keyword_DE", "mot_français", "english_term", "русский_термин", "NL_woord", "término_ES", ...],
      "rationale": "Kurze Begründung für Ranking und Relevanz"
    }}
  ]
}}

WICHTIG – JSON-Validität:
- Deine Antwort MUSS strikt valides JSON sein, das direkt mit json.loads() parsbar ist.
- Achte besonders auf korrekte Kommas zwischen Array-Elementen und Objekt-Feldern.
- Strings mit Anführungszeichen, Zeilenumbrüchen oder Backslashes MÜSSEN korrekt escaped werden (\\", \\n, \\\\).
- Kein Text vor oder nach dem JSON-Objekt. Keine Markdown-Code-Blöcke.
- Kein Trailing Comma nach dem letzten Element in Arrays oder Objekten."""


def _repair_json(raw_text):
    """Versucht häufige JSON-Fehler aus LLM-Antworten zu reparieren.

    Repariert (in dieser Reihenfolge):
    - Text vor/nach dem JSON-Objekt entfernen
    - Trailing Commas vor } oder ]
    - Fehlende Kommas zwischen }{, ][, }[, etc.
    - Unescapte Steuerzeichen in Strings (Newlines, Tabs)
    - Abgeschnittenes JSON (offene Klammern/Strings schließen)

    Returns: (parsed_dict, repair_notes) oder (None, error_msg)
    """
    import re as _re

    text = raw_text.strip()
    repairs = []

    # 1. JSON-Objekt extrahieren falls umgebender Text vorhanden
    if not text.startswith("{"):
        start = text.find("{")
        if start > 0:
            repairs.append(f"Text vor JSON entfernt ({start} Zeichen)")
            text = text[start:]
        elif start < 0:
            return None, "Kein JSON-Objekt gefunden"

    # Überschüssigen Text nach dem JSON-Block entfernen
    depth = 0
    in_string = False
    escape_next = False
    for i, ch in enumerate(text):
        if escape_next:
            escape_next = False
            continue
        if ch == '\\' and in_string:
            escape_next = True
            continue
        if ch == '"' and not escape_next:
            in_string = not in_string
            continue
        if in_string:
            continue
        if ch == '{':
            depth += 1
        elif ch == '}':
            depth -= 1
            if depth == 0:
                if i < len(text) - 1:
                    trailing = text[i+1:].strip()
                    if trailing:
                        repairs.append(f"Text nach JSON entfernt ({len(trailing)} Zeichen)")
                    text = text[:i+1]
                break

    # 2. Trailing Commas entfernen (z.B. ,} oder ,])
    fixed = _re.sub(r',\s*([}\]])', r'\1', text)
    if fixed != text:
        repairs.append("Trailing Commas entfernt")
        text = fixed

    # 3. Fehlende Kommas einfügen (häufigster LLM-Fehler!)
    #    z.B. }{ → },{  oder ][ → ],[  oder }" → },"  oder ]" → ],"
    patterns = [
        (r'}\s*{', '},{'),           # }{  → },{
        (r']\s*\[', '],['),          # ][  → ],[
        (r'}\s*\[', '},['),          # }[  → },[
        (r']\s*{', '],{'),           # ]{  → ],{
        (r'}\s*"', '},"'),           # }"  → },"
        (r']\s*"', '],"'),           # ]"  → ],"
        (r'"\s*"', '","'),           # ""  → ","  (Achtung: nur außerhalb von Strings)
        (r'(\d)\s*"', r'\1,"'),      # 8"  → 8,"
        (r'"\s*{', '",{'),           # "{  → ",{
        (r'"\s*\[', '",['),          # "[  → ",[
        (r'(true|false|null)\s*"', r'\1,"'),    # true" → true,"
        (r'(true|false|null)\s*{', r'\1,{'),    # true{ → true,{
        (r'(\d)\s*{', r'\1,{'),      # 8{  → 8,{
    ]
    # Diese Reparaturen nur AUSSERHALB von Strings anwenden
    # Einfacher Ansatz: Strings temporär ersetzen
    string_pattern = _re.compile(r'"(?:[^"\\]|\\.)*"', _re.DOTALL)
    strings = []
    def save_string(m):
        strings.append(m.group(0))
        return f'__STR{len(strings)-1}__'
    text_no_strings = string_pattern.sub(save_string, text)

    for pat, repl in patterns:
        new_text = _re.sub(pat, repl, text_no_strings)
        if new_text != text_no_strings:
            text_no_strings = new_text
            if "Fehlende Kommas eingefügt" not in repairs:
                repairs.append("Fehlende Kommas eingefügt")

    # Strings wiederherstellen
    def restore_string(m):
        idx = int(m.group(0).replace('__STR', '').replace('__', ''))
        return strings[idx]
    text = _re.sub(r'__STR(\d+)__', restore_string, text_no_strings)

    # 4. Unescapte Steuerzeichen in Strings reparieren
    def fix_string_content(match):
        s = match.group(0)
        inner = s[1:-1]
        original = inner
        inner = inner.replace('\n', '\\n').replace('\r', '\\r').replace('\t', '\\t')
        if inner != original:
            return '"' + inner + '"'
        return s

    fixed = _re.sub(r'"(?:[^"\\]|\\.)*"', fix_string_content, text, flags=_re.DOTALL)
    if fixed != text:
        repairs.append("Unescapte Steuerzeichen in Strings repariert")
        text = fixed

    # 5. Versuch parsen
    try:
        result = json.loads(text)
        return result, repairs
    except json.JSONDecodeError:
        pass

    # 6. Abgeschnittenes JSON: Letztes vollständiges Proposal finden und Rest abschneiden
    #    Statt offene Klammern zu schließen (fragil), schneiden wir den unvollständigen
    #    Teil ab und behalten nur die bereits vollständigen Proposals.
    depth_brace = text.count('{') - text.count('}')
    depth_bracket = text.count('[') - text.count(']')
    if depth_brace > 0 or depth_bracket > 0:
        # Strategie: Finde die letzte Position eines vollständig geschlossenen
        # Proposal-Objekts und schneide danach ab
        last_complete = -1
        search_start = 0
        # Suche nach dem Pattern }, das ein vollständiges Objekt in einem Array beendet
        while True:
            pos = text.find('},', search_start)
            if pos < 0:
                break
            last_complete = pos + 1  # inkl. }
            search_start = pos + 1

        # Auch } gefolgt von ] prüfen (letztes Objekt im Array)
        pos = text.rfind('}]')
        if pos > last_complete:
            last_complete = pos + 2  # inkl. }]

        if last_complete > 0:
            truncated = text[:last_complete]
            # Offene Klammern schließen
            remaining_brace = truncated.count('{') - truncated.count('}')
            remaining_bracket = truncated.count('[') - truncated.count(']')
            truncated += ']' * remaining_bracket + '}' * remaining_brace
            truncated = _re.sub(r',\s*([}\]])', r'\1', truncated)

            try:
                result = json.loads(truncated)
                orig_count = text.count('"rank"')
                saved_count = len(result.get("proposals", []))
                repairs.append(f"Abgeschnittenes JSON: {saved_count} von ~{orig_count} Vorschlägen gerettet")
                return result, repairs
            except json.JSONDecodeError:
                pass

        # Fallback: Einfaches Klammern-Schließen
        quote_count = len(_re.findall(r'(?<!\\)"', text))
        if quote_count % 2 == 1:
            text += '"'
            repairs.append("Offenen String geschlossen")

        text = text.rstrip()
        if text.endswith(','):
            text = text[:-1]

        text += ']' * depth_bracket + '}' * depth_brace
        text = _re.sub(r',\s*([}\]])', r'\1', text)

        try:
            result = json.loads(text)
            repairs.append(f"Abgeschnittenes JSON vervollständigt")
            return result, repairs
        except json.JSONDecodeError as e:
            return None, f"Reparatur fehlgeschlagen: {e}"

    return None, "JSON nicht reparierbar"


def _save_raw_response(raw_text, date_str, output_dir):
    """Speichert die Roh-Antwort der API für spätere Fehleranalyse."""
    os.makedirs(output_dir, exist_ok=True)
    date_compact = date_str.replace("-", "")
    filepath = os.path.join(output_dir, f"themenvorschlag_{date_compact}.raw.txt")
    with open(filepath, "w", encoding="utf-8") as f:
        f.write(raw_text)
    print(f"  Rohtext gesichert: {filepath}")
    return filepath


def generate_topic_proposals(corpus_text, stats, date_str):
    """Ruft Claude API auf und generiert 30 Themenvorschläge."""
    from anthropic import Anthropic

    # Wochentag berechnen
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    weekdays_de = ["Montag", "Dienstag", "Mittwoch", "Donnerstag", "Freitag", "Samstag", "Sonntag"]
    weekday = weekdays_de[dt.weekday()]

    # Modernes Datum (+ 100 Jahre)
    modern_date = dt.replace(year=dt.year + 100).strftime("%Y-%m-%d")

    user_prompt = USER_PROMPT_TEMPLATE.format(
        date_historical=date_str,
        weekday=weekday,
        date_modern=modern_date,
        primary_count=stats["primary_sources"],
        primary_chars=stats["primary_chars"],
        nyt_chars=stats["nyt_chars"],
        retro_count=stats["retrospective_sources"],
        retro_chars=stats["retrospective_chars"],
        corpus=corpus_text,
    )

    client = Anthropic(api_key=ANTHROPIC_API_KEY)

    print("  Claude API wird aufgerufen ...")
    print(f"  Korpus-Größe: {len(user_prompt):,} Zeichen (~{len(user_prompt) // 4:,} Tokens)")

    raw_text = ""
    max_retries = 2
    for attempt in range(max_retries + 1):
        try:
            response = client.messages.create(
                model="claude-sonnet-4-20250514",
                max_tokens=16384,
                system=SYSTEM_PROMPT,
                messages=[{"role": "user", "content": user_prompt}],
            )

            # Diagnostik
            stop_reason = response.stop_reason
            print(f"  API-Antwort: stop_reason={stop_reason}, "
                  f"content_blocks={len(response.content)}, "
                  f"usage={response.usage.input_tokens}in/{response.usage.output_tokens}out")

            if not response.content:
                print(f"  WARNUNG: Leere Antwort (content=[])")
                if attempt < max_retries:
                    print(f"  Retry {attempt + 1}/{max_retries} ...")
                    time.sleep(10)
                    continue
                raw_text = ""
                break

            raw_text = response.content[0].text.strip()

            if not raw_text:
                print(f"  WARNUNG: Leerer Textblock (stop_reason={stop_reason})")
                if attempt < max_retries:
                    print(f"  Retry {attempt + 1}/{max_retries} ...")
                    time.sleep(10)
                    continue
                break

            if stop_reason == "max_tokens":
                print(f"  WARNUNG: Antwort bei max_tokens abgeschnitten ({len(raw_text):,} Zeichen)")

            # Erfolgreiche Antwort
            break

        except Exception as e:
            print(f"  API-Fehler: {e}")
            if attempt < max_retries:
                print(f"  Retry {attempt + 1}/{max_retries} in 15s ...")
                time.sleep(15)
            else:
                raw_text = ""

    # JSON extrahieren (falls in Code-Block)
    json_match = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", raw_text, re.DOTALL)
    if json_match:
        raw_text = json_match.group(1)

    # Phase A: Direktes Parsing
    try:
        proposals = json.loads(raw_text)
    except json.JSONDecodeError as e:
        print(f"  WARNUNG: JSON-Parsing fehlgeschlagen: {e}")
        print(f"  Rohtext ({len(raw_text)} Zeichen): {raw_text[:500]}")

        # Rohtext sichern für spätere Analyse
        _save_raw_response(raw_text, date_str, OUTPUT_DIR)

        # Phase B: Automatische Reparatur versuchen
        print("  Versuche automatische JSON-Reparatur ...")
        repaired, notes = _repair_json(raw_text)

        if repaired is not None and isinstance(repaired, dict):
            proposals = repaired
            repair_note = ", ".join(notes) if isinstance(notes, list) else str(notes)
            print(f"  ✓ JSON-Reparatur erfolgreich: {repair_note}")
            num_rescued = len(proposals.get("proposals", []))
            print(f"  ✓ {num_rescued} Vorschläge gerettet")
        else:
            print(f"  ✗ Reparatur fehlgeschlagen: {notes}")
            proposals = {"raw_response": raw_text, "proposals": []}

    # Ränge deterministisch 1–N durchnummerieren
    # (Claude liefert manchmal Relevanz-Stufen statt fortlaufender Nummern)
    items = proposals.get("proposals", [])
    if items:
        old_ranks = [p.get("rank") for p in items]
        for i, p in enumerate(items, start=1):
            p["rank"] = i
        new_ranks = [p["rank"] for p in items]
        if old_ranks != new_ranks:
            print(f"  Rang-Korrektur: {len(items)} Vorschläge auf 1–{len(items)} durchnummeriert")

    return proposals


# ─────────────────────────────────────────────
# Phase 4 & 5: JSON-Export und Zusammenfassung
# ─────────────────────────────────────────────


def write_output(proposals, stats, failed_sources, date_str, output_dir):
    """Schreibt das Ergebnis als JSON-Datei."""
    os.makedirs(output_dir, exist_ok=True)

    dt = datetime.strptime(date_str, "%Y-%m-%d")
    date_compact = date_str.replace("-", "")
    filename = f"themenvorschlag_{date_compact}.json"
    filepath = os.path.join(output_dir, filename)

    output = {
        "schema_version": "1.0",
        "project": "Vor 100 Jahren",
        "date_historical": date_str,
        "date_modern": dt.replace(year=dt.year + 100).strftime("%Y-%m-%d"),
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "corpus_stats": {
            **stats,
            "sources_failed": failed_sources,
        },
        "proposals": proposals.get("proposals", []),
        "selection": {
            "status": "pending",
            "selected_indices": [],
            "note": "Bitte 3–15 Artikel aus den Vorschlägen auswählen (Empfehlung: 7). Richtwert: 1-2 Hauptartikel, 3-4 Artikel, 1-2 Kurzbeiträge",
        },
    }

    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    return filepath


def print_summary(proposals, filepath):
    """Gibt eine Zusammenfassung der Vorschläge aus."""
    items = proposals.get("proposals", [])
    if not items:
        print("\n  Keine Vorschläge generiert.")
        return

    print(f"\n{'=' * 60}")
    print(f"  {len(items)} THEMENVORSCHLÄGE")
    print(f"{'=' * 60}")

    for p in items:
        rank = p.get("rank", "?")
        topic = p.get("topic", "?")
        art_type = p.get("suggested_type", "?")
        sources = p.get("source_count", 0)
        category = p.get("category", "?")
        print(f"  {rank:>2}. [{art_type:<14}] {topic}")
        print(f"      Kategorie: {category} | Quellen: {sources}")

    print(f"\n{'=' * 60}")
    print(f"  Gespeichert: {filepath}")
    print(f"\n  NÄCHSTER SCHRITT:")
    print(f"  Öffne die JSON-Datei und trage die Nummern deiner")
    print(f"  ausgewählten Artikel (3–15) in 'selected_indices' ein.")
    print(f"{'=' * 60}")


# ─────────────────────────────────────────────
# Hauptprogramm
# ─────────────────────────────────────────────


def main():
    parser = argparse.ArgumentParser(
        description="Vor 100 Jahren – Step 1: Automatische Themengewinnung",
        epilog="Beispiel: python step1_pipeline.py 1926-02-13",
    )
    parser.add_argument(
        "date",
        help="Historisches Datum im Format YYYY-MM-DD",
    )
    parser.add_argument(
        "--output",
        default=OUTPUT_DIR,
        help=f"Ausgabeverzeichnis (Standard: {OUTPUT_DIR})",
    )
    parser.add_argument(
        "--skip",
        nargs="+",
        choices=["ddb", "gallica", "loc", "nyt", "wapo", "bne", "anno", "delpher", "trove", "nch", "pravda", "wikipedia", "lemo"],
        default=[],
        help="Quellen überspringen (z.B. --skip bne lemo)",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Ausführliche Ausgabe",
    )

    args = parser.parse_args()

    # Datum validieren
    try:
        target_date = datetime.strptime(args.date, "%Y-%m-%d")
    except ValueError:
        print("FEHLER: Ungültiges Datumsformat. Bitte YYYY-MM-DD verwenden.", file=sys.stderr)
        sys.exit(1)

    weekdays = ["Montag", "Dienstag", "Mittwoch", "Donnerstag", "Freitag", "Samstag", "Sonntag"]
    weekday = weekdays[target_date.weekday()]

    print("=" * 60)
    print("  VOR 100 JAHREN – STEP 1: THEMENGEWINNUNG")
    print("=" * 60)
    print(f"  Datum: {args.date} ({weekday})")
    print(f"  Ausgabe: {args.output}")
    if args.skip:
        print(f"  Übersprungen: {', '.join(args.skip)}")

    # Prüfe API-Keys
    if not DDB_API_KEY and "ddb" not in args.skip:
        print("\n  WARNUNG: DDB_API_KEY nicht gesetzt – DDB wird übersprungen")
        args.skip.append("ddb")
    if not ANTHROPIC_API_KEY:
        print("\n  FEHLER: ANTHROPIC_API_KEY nicht gesetzt!", file=sys.stderr)
        sys.exit(1)

    start_time = time.time()

    # Phase 1: Quellen abrufen (oder vorhandenen Corpus laden)
    print(f"\n{'─' * 60}")
    print("  PHASE 1: QUELLENABFRAGE")
    print(f"{'─' * 60}")

    corpus_file = os.path.join(args.output, f"corpus_{args.date}.json")

    if os.path.exists(corpus_file) and not args.verbose:
        # Vorhandenen Corpus wiederverwenden
        print(f"  ✓ Vorhandener Corpus gefunden: {corpus_file}")
        with open(corpus_file, "r", encoding="utf-8") as f:
            full_corpus = json.load(f)
        total_chars = sum(len(t) for t in full_corpus.values())
        print(f"  Geladen: {len(full_corpus)} Quellen, {total_chars:,} Zeichen")
        # Sources-Dict für build_corpus_for_claude erstellen
        # Primäre Zeitungsquellen mit ddb:-Prefix etc. rekonstruieren
        sources = {}
        for name, text in full_corpus.items():
            if name in ("NYT (Metadaten)",):
                sources[f"nyt:{name}"] = text
            elif name.startswith("Wikipedia") or name.startswith("LEMO"):
                sources[f"retro:{name}"] = text
            else:
                sources[f"primary:{name}"] = text
        failed = []
    else:
        # Quellen frisch abrufen
        sources, failed = fetch_all_sources(args.date, args.skip, args.verbose)

        primary_count = sum(1 for k in sources if not k.startswith("retro:"))
        print(f"\n  Ergebnis: {len(sources)} Quellen erfolgreich, {len(failed)} fehlgeschlagen")
        print(f"  Primärquellen: {primary_count}")

        if primary_count < 1:
            print("\n  FEHLER: Keine Primärquellen verfügbar. Abbruch.", file=sys.stderr)
            sys.exit(1)

        # Vollen Corpus speichern (für Step 2 Wiederverwendung)
        print(f"\n  Speichere vollen Corpus: {corpus_file}")
        full_corpus = {}
        for source_key, text in sources.items():
            # Prefix entfernen für Speicherung (ddb:Name → Name)
            if ":" in source_key:
                _prefix, name = source_key.split(":", 1)
            else:
                name = source_key
            full_corpus[name] = text
        os.makedirs(args.output, exist_ok=True)
        with open(corpus_file, "w", encoding="utf-8") as f:
            json.dump(full_corpus, f, ensure_ascii=False, indent=2)
        total_chars = sum(len(t) for t in full_corpus.values())
        print(f"  ✓ Corpus gespeichert: {len(full_corpus)} Quellen, {total_chars:,} Zeichen")

    # Phase 2: Korpus aufbereiten (gekürzte Version für Claude-Prompt)
    print(f"\n{'─' * 60}")
    print("  PHASE 2: KORPUS-AUFBEREITUNG")
    print(f"{'─' * 60}")

    corpus_text, stats = build_corpus_for_claude(sources, args.date)
    print(f"  Gesamtkorpus für Claude: {stats['total_chars']:,} Zeichen")
    print(f"  Primär: {stats['primary_chars']:,} | NYT: {stats['nyt_chars']:,} | Retro: {stats['retrospective_chars']:,}")

    # Phase 3: Claude API
    print(f"\n{'─' * 60}")
    print("  PHASE 3: THEMENVORSCHLÄGE GENERIEREN")
    print(f"{'─' * 60}")

    proposals = generate_topic_proposals(corpus_text, stats, args.date)

    num_proposals = len(proposals.get("proposals", []))
    print(f"  ✓ {num_proposals} Vorschläge generiert")

    # Phase 4: JSON speichern
    filepath = write_output(proposals, stats, failed, args.date, args.output)

    # Phase 5: Zusammenfassung
    print_summary(proposals, filepath)

    elapsed = time.time() - start_time
    print(f"\n  Laufzeit: {elapsed:.0f} Sekunden ({elapsed / 60:.1f} Minuten)")


if __name__ == "__main__":
    main()
