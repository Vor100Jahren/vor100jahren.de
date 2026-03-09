#!/usr/bin/env python3
"""
VOR 100 JAHREN – Step-2-Pipeline: Artikelproduktion
End-to-End-Testlauf für den 13. Februar 1926

Stufen:
  0. Download: Alle Seiten aller Quellzeitungen (DDB + Gallica + LoC + NYT)
  1. Artikelauswahl: 5 Artikel aus dem Top-30-Ranking
  2. Kontextextraktion: Keyword-Suche im Gesamtkorpus
  3. Artikelgenerierung: Artikel im 1920er-Zeitungsstil
  4. Anreicherung: Wikipedia-Links, Wikimedia-Bilder, Quellenapparat
  5. Export: JSON + DOCX
"""

import argparse
import requests
import json
import xml.etree.ElementTree as ET
import re
import sys
import time
import os
import datetime as _dt
from datetime import datetime
from urllib.parse import quote as url_quote
from dotenv import load_dotenv

# .env-Datei laden (API-Keys)
load_dotenv()

# ============ KONFIGURATION ============
DDB_API_KEY = os.environ.get("DDB_API_KEY", "")
NYT_API_KEY = os.environ.get("NYT_API_KEY", "")
TROVE_API_KEY = os.environ.get("TROVE_API_KEY", "")
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
TARGET_DATE = "1926-02-13"  # Wird in main() dynamisch überschrieben
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output")
# Verzeichnis für lokal gehostete Bilder (vermeidet Wikimedia-Hotlinking / 429-Fehler)
IMAGES_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "images")

HEADERS_DDB = {
    "Authorization": f"OAuth oauth_consumer_key={DDB_API_KEY}",
    "Accept": "application/xml"
}
HEADERS_GALLICA = {
    "User-Agent": "Vor100Jahren-Bot/1.0 (historical research)"
}

# Deutsche Zeitungen (DDB) – Suchbegriffe für paper_title-Suche
# Manche Titel brauchen alternative Schreibweisen
GERMAN_PAPERS_SEARCH = {
    # Anzeigename: Suchbegriff für DDB-API
    # PRIMÄR (immer abfragen):
    "Vorwärts": "Vorwärts",
    "Berliner Tageblatt": "Berliner Tageblatt",
    "Deutsche Allgemeine Zeitung": "allgemeine Zeitung",
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

# Französische Zeitungen (Gallica)
GALLICA_SERIES = {
    "Le Figaro": "cb34355551z",
    "Le Temps": "cb34431794k",
}

# US-Zeitungen (Library of Congress – Chronicling America)
# LCCN-Code: (Kurzname, Anzeigename mit Ort, max_pages)
LOC_NEWSPAPERS = {
    # PRIMÄR:
    "sn84026749": ("The Washington Times", "The Washington Times (Washington, D.C.)", 20),
    # BACKUP (nur wenn Washington Post + Washington Times leer):
    "sn83045462": ("Evening Star", "Evening Star (Washington, D.C.)", 20),
}
LOC_BACKUP = {"sn83045462"}  # LCCNs, die nur als Fallback abgerufen werden

# ============ QUELLEN-REGISTRY ============
# Zentrale Zuordnung: Korpus-Name → Archiv-Metadaten
# Wird von build_source_apparatus() verwendet, um aus Kontext-Treffern
# automatisch den Quellenapparat zu erzeugen.

def build_source_registry(date_str, ddb_item_ids=None, trove_urls=None,
                          bne_issue_url=None, delpher_urls=None):
    """Quellen-Registry dynamisch aus Konfiguration aufbauen.

    Liefert ein Dict: corpus_name → {newspaper, url, archive, license}
    Alle URLs werden mit dem aktuellen Datum parametrisiert.

    Optionale Parameter für spezifische URLs:
      ddb_item_ids:  Dict {display_name: item_id} aus find_newspapers/select_key_papers
      trove_urls:    Dict {paper_name: trove_article_url} aus fetch_trove_newspapers
      bne_issue_url: String – spezifische BNE-Ausgabe-URL aus fetch_bne_el_sol
      delpher_urls:  Dict {paper_name: resolver_url} aus fetch_delpher_newspaper
    """
    date_compact = date_str.replace("-", "")
    registry = {}

    # Deutsche Zeitungen (DDB) – spezifische Ausgabe-URLs wenn Item-IDs bekannt
    ddb_base_url = "https://www.deutsche-digitale-bibliothek.de/newspaper/"
    _ddb_ids = ddb_item_ids or {}
    for display_name in GERMAN_PAPERS_SEARCH.keys():
        item_id = _ddb_ids.get(display_name)
        url = (f"https://www.deutsche-digitale-bibliothek.de/newspaper/{item_id}"
               if item_id else ddb_base_url)
        registry[display_name] = {
            "newspaper": display_name,
            "url": url,
            "archive": "Deutsche Digitale Bibliothek",
            "license": "Gemeinfrei (Originalwerk) / Nutzungsbedingungen DDB"
        }
    
    # Französische Zeitungen (Gallica)
    for name, ark in GALLICA_SERIES.items():
        registry[name] = {
            "newspaper": name,
            "url": f"https://gallica.bnf.fr/ark:/12148/{ark}/date{date_compact}",
            "archive": "Gallica (BnF)",
            "license": "Public Domain"
        }
    
    # US-Zeitungen (Library of Congress)
    for lccn, (name, display_name, _) in LOC_NEWSPAPERS.items():
        registry[name] = {
            "newspaper": display_name,
            "url": f"https://www.loc.gov/resource/{lccn}/{date_str}/ed-1/",
            "archive": "Library of Congress \u2013 Chronicling America",
            "license": "Public Domain"
        }
    
    # NYT (Metadaten)
    registry["NYT (Metadaten)"] = {
        "newspaper": "The New York Times (Metadaten)",
        "url": "https://www.nytimes.com/",
        "archive": "NYT Archive API",
        "license": "NYT Terms of Service"
    }

    # Australische Zeitungen (Trove) – spezifische URLs wenn verfügbar
    trove_papers = {
        "The Sydney Morning Herald": 35,
        "The Argus": 13,
        "The Brisbane Courier": 68,
    }
    _trove_urls = trove_urls or {}
    for name in trove_papers.keys():
        specific_url = _trove_urls.get(name)
        registry[name] = {
            "newspaper": name,
            "url": specific_url if specific_url else "https://trove.nla.gov.au/newspaper/",
            "archive": "Trove (National Library of Australia)",
            "license": "Public Domain / Out of Copyright"
        }

    # Österreichische Zeitungen (ANNO)
    for aid, (name, _) in ANNO_NEWSPAPERS.items():
        registry[name] = {
            "newspaper": name,
            "url": f"https://anno.onb.ac.at/cgi-content/anno?aid={aid}&datum={date_compact}",
            "archive": "ANNO (Österreichische Nationalbibliothek)",
            "license": "Public Domain"
        }

    # Niederländische Zeitungen (Delpher) – spezifische URLs wenn verfügbar
    _delpher_urls = delpher_urls or {}
    for name in DELPHER_NEWSPAPERS.keys():
        specific_url = _delpher_urls.get(name)
        registry[name] = {
            "newspaper": name,
            "url": specific_url if specific_url else "https://www.delpher.nl/nl/kranten",
            "archive": "Delpher (Koninklijke Bibliotheek)",
            "license": "Public Domain / KB Terms"
        }

    # Washington Post (archive.org)
    registry["Washington Post"] = {
        "newspaper": "The Washington Post",
        "url": f"https://archive.org/search?query=title%3A%22Washington+Post%22+AND+year%3A1926",
        "archive": "Internet Archive",
        "license": "Public Domain"
    }

    # North China Herald (archive.org)
    registry["North China Herald"] = {
        "newspaper": "North China Herald (Shanghai)",
        "url": f"https://archive.org/search?query=title%3A%22North+China+Herald%22+AND+year%3A1926",
        "archive": "Internet Archive",
        "license": "Public Domain"
    }

    # Pravda (archive.org)
    registry["Pravda"] = {
        "newspaper": "Правда (Pravda)",
        "url": f"https://archive.org/details/pravda-newspaper",
        "archive": "Internet Archive",
        "license": "Public Domain"
    }

    # El Sol (BNE – Biblioteca Nacional de España) – spezifische URL wenn verfügbar
    _bne_url = bne_issue_url or "https://hemerotecadigital.bne.es/hd/es/results?parent=0312db23-f8f4-4c11-98ef-fcaa99442e30"
    registry["El Sol"] = {
        "newspaper": "El Sol (Madrid)",
        "url": _bne_url,
        "archive": "Hemeroteca Digital (BNE)",
        "license": "Public Domain"
    }

    return registry

# ============ ALTO-XML PARSER ============
def extract_alto_text(xml_content):
    """Text aus ALTO-XML extrahieren (v2, v3, BnF).
    
    Verwendet TextBlock-Grenzen als Absatztrenner (\n\n).
    Zeilen innerhalb eines TextBlocks werden mit Leerzeichen verbunden.
    So entstehen sinnvolle Absätze für die spätere Keyword-Suche.
    """
    try:
        root = ET.fromstring(xml_content)
    except ET.ParseError:
        return ""
    
    for ns_uri in [
        'http://www.loc.gov/standards/alto/ns-v2#',
        'http://www.loc.gov/standards/alto/ns-v3#',
        'http://bibnum.bnf.fr/ns/alto_prod',
        'http://schema.ccs-gmbh.com/ALTO'
    ]:
        ns = {'alto': ns_uri}
        blocks = root.findall('.//alto:TextBlock', ns)
        if blocks:
            paragraphs = []
            for tb in blocks:
                block_lines = []
                for tl in tb.findall('alto:TextLine', ns):
                    words = [s.get('CONTENT', '')
                             for s in tl.findall('alto:String', ns)]
                    if words:
                        block_lines.append(' '.join(words))
                # Zeilen innerhalb eines Blocks zu einem Absatz verbinden
                if block_lines:
                    paragraphs.append(' '.join(block_lines))
            # Absätze mit \n\n trennen – passend zur Kontextextraktion
            return '\n\n'.join(paragraphs)
    
    # Fallback: ohne Namespace
    for tag_prefix in ['{http://www.loc.gov/standards/alto}', '']:
        blocks = root.findall(f'.//{tag_prefix}TextBlock')
        if blocks:
            paragraphs = []
            for tb in blocks:
                block_lines = []
                for tl in tb.findall(f'.//{tag_prefix}TextLine'):
                    words = []
                    for s in tl.findall(f'.//{tag_prefix}String'):
                        w = s.get('CONTENT', '')
                        if w:
                            words.append(w)
                    if words:
                        block_lines.append(' '.join(words))
                if block_lines:
                    paragraphs.append(' '.join(block_lines))
            return '\n\n'.join(paragraphs)
    
    return ""

# ============ STUFE 0b: TEXTAUFBEREITUNG ============

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
    count = 0
    count += text.count('ſ')
    count += text.count('⸗')
    count += text.count('ꝛ')
    text = text.replace('ſ', 's')
    text = text.replace('⸗', '-')
    text = text.replace('ꝛ', 'r')
    if count > 0:
        print(f"    Fraktur-Normalisierung: {count} Korrekturen")
    return text


def resegment_paragraphs(text, min_len=200, max_len=3000):
    """Absätze auf sinnvolle Größe normalisieren (200-3000 Zeichen).

    Problem: ALTO-XML erzeugt je nach Archiv extrem unterschiedliche
    Absatzstrukturen – von Einzel-Wort-Absätzen (Berliner Tageblatt)
    bis zu 55.000-Zeichen-Monsterabsätzen (Kölnische Zeitung).

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
        # Seitenmarker unverändert durchlassen
        if para.strip().startswith('--- Seite') and para.strip().endswith('---'):
            split_paras.append(para)
            continue

        if len(para) <= max_len:
            split_paras.append(para)
            continue

        # An Satzgrenzen aufbrechen: Punkt/!/? + Leerzeichen + Großbuchstabe
        remaining = para
        while len(remaining) > max_len:
            # Letzte Satzgrenze vor max_len finden
            best_pos = -1
            for match in re.finditer(r'[.!?]\s+(?=[A-ZÄÖÜ])', remaining[:max_len]):
                best_pos = match.end()

            if best_pos > min_len:
                split_paras.append(remaining[:best_pos].strip())
                remaining = remaining[best_pos:].strip()
                splits += 1
            else:
                # Fallback: Am letzten Leerzeichen vor max_len trennen
                space_pos = remaining[:max_len].rfind(' ')
                if space_pos > min_len:
                    split_paras.append(remaining[:space_pos].strip())
                    remaining = remaining[space_pos:].strip()
                    splits += 1
                else:
                    # Harter Schnitt
                    split_paras.append(remaining[:max_len].strip())
                    remaining = remaining[max_len:].strip()
                    splits += 1

        if remaining.strip():
            split_paras.append(remaining.strip())

    # Schritt 2: Mikro-Absätze zusammenfassen
    merged = []
    buffer = ""

    for para in split_paras:
        # Seitenmarker nicht zusammenfassen
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


def prepare_corpus_text(corpus):
    """Stufe 0b: Fraktur-Normalisierung + Absatz-Neusegmentierung auf gesamten Korpus anwenden."""
    print("\n" + "="*60)
    print("STUFE 0b: TEXTAUFBEREITUNG")
    print("="*60)

    prepared = {}
    total_fraktur = 0

    for source_name, text in corpus.items():
        print(f"\n  {source_name}:")
        original_len = len(text)

        # 1. Fraktur-Normalisierung
        text = normalize_fraktur(text)

        # 2. Absatz-Neusegmentierung
        text = resegment_paragraphs(text)

        prepared[source_name] = text
        print(f"    {original_len:,} → {len(text):,} Zeichen")

    return prepared


# ============ STUFE 0: DOWNLOAD ============

def find_newspapers(date_str):
    """Alle Zeitungsausgaben eines Tages in der DDB finden – gezielt pro Titel."""
    all_docs = []
    
    for display_name, search_term in GERMAN_PAPERS_SEARCH.items():
        # Gezielte Suche pro Zeitungstitel
        query = f'paper_title:"{search_term}" AND publication_date:[{date_str}T00:00:00Z TO {date_str}T23:59:59Z]'
        url = (
            "https://api.deutsche-digitale-bibliothek.de/search/index/"
            "newspaper-issues/select"
            f"?oauth_consumer_key={DDB_API_KEY}"
            f"&q={url_quote(query)}"
            "&rows=10"
            "&fl=id,paper_title,publication_date"
        )
        try:
            resp = requests.get(url, timeout=45)
            data = resp.json()
            docs = data.get("response", {}).get("docs", [])
            if docs:
                # Display-Namen anhängen für spätere Zuordnung
                for d in docs:
                    d["_display_name"] = display_name
                all_docs.extend(docs)
                print(f"    {display_name}: {len(docs)} Ausgabe(n) gefunden")
            else:
                print(f"    {display_name}: Nicht gefunden für {date_str}")
        except Exception as e:
            print(f"    {display_name}: Suchfehler: {e}")
        time.sleep(0.3)
    
    print(f"  DDB gezielt: {len(all_docs)} Ausgaben für {len(GERMAN_PAPERS_SEARCH)} Zeitungstitel")
    return all_docs

def select_key_papers(docs):
    """Aus den DDB-Ergebnissen je eine Ausgabe pro Zeitung auswählen.
    
    Dedupliziert nach Base-ID (32-Zeichen-Hash). Bei mehreren Ausgaben 
    (Morgen/Abend) wird die erste genommen.
    """
    selected = {}
    seen_base_ids = set()
    
    for doc in docs:
        display_name = doc.get("_display_name", doc.get("paper_title", "Unbekannt"))
        item_id = doc.get("id", "")
        base_id = item_id[:32]
        
        if base_id in seen_base_ids:
            continue
        
        if display_name not in selected:
            selected[display_name] = base_id
            seen_base_ids.add(base_id)
    
    return selected

def fetch_mets_record(item_id, paper_name):
    """METS/MODS-Record einer Zeitungsausgabe abrufen."""
    url = f"https://api.deutsche-digitale-bibliothek.de/items/{item_id}/source/record"
    try:
        resp = requests.get(url, headers=HEADERS_DDB, timeout=30)
        if resp.status_code == 200:
            return resp.content
        else:
            print(f"    {paper_name}: METS HTTP {resp.status_code}")
            return None
    except Exception as e:
        print(f"    {paper_name}: METS Fehler: {e}")
        return None

def extract_alto_urls_from_mets(mets_xml):
    """ALTO-URLs aus einem METS-Record extrahieren.
    
    DDB-METS enthalten oft zwei fileGrps:
    - 'FULLTEXT': Relative Pfade zum Provider-Server (nicht direkt nutzbar)
    - 'DDB_FULLTEXT': Absolute DDB-Binary-URLs (bevorzugt)
    
    Wir bevorzugen DDB_FULLTEXT, da diese immer absolute URLs enthalten.
    Relative Pfade ohne bekannten Base-URL werden übersprungen.
    """
    alto_urls_by_group = {}  # USE-Attribut → [urls]
    try:
        root = ET.fromstring(mets_xml)
    except ET.ParseError:
        return []
    
    ns_mets = '{http://www.loc.gov/METS/}'
    ns_xlink = '{http://www.w3.org/1999/xlink}'
    
    for file_grp in root.iter(f'{ns_mets}fileGrp'):
        use = file_grp.get('USE', '')
        if 'FULLTEXT' in use.upper() or 'ALTO' in use.upper():
            urls = []
            for file_el in file_grp.iter(f'{ns_mets}file'):
                for flocat in file_el.iter(f'{ns_mets}FLocat'):
                    href = flocat.get(f'{ns_xlink}href', '')
                    if href:
                        urls.append(href)
            if urls:
                alto_urls_by_group[use] = urls
    
    # Bevorzugt DDB_FULLTEXT (absolute URLs), dann FULLTEXT, dann Fallback
    selected_urls = []
    for preferred in ['DDB_FULLTEXT', 'FULLTEXT', 'ALTO']:
        for use, urls in alto_urls_by_group.items():
            if preferred in use.upper() and urls:
                selected_urls = urls
                break
        if selected_urls:
            break
    
    # Fallback: alle xlink:href die auf .xml enden
    if not selected_urls:
        for elem in root.iter():
            href = elem.get(f'{ns_xlink}href', '')
            if href and ('.xml' in href.lower() or 'alto' in href.lower()):
                selected_urls.append(href)
    
    # Nur absolute URLs (http/https) behalten
    absolute_urls = [u for u in selected_urls if u.startswith(('http://', 'https://'))]
    
    if len(absolute_urls) < len(selected_urls):
        skipped = len(selected_urls) - len(absolute_urls)
        # Falls DDB_FULLTEXT-Gruppe leer war, aber FULLTEXT relative hatte:
        # Schaue ob eine andere Gruppe absolute URLs hat
        if not absolute_urls:
            for use, urls in alto_urls_by_group.items():
                abs_in_group = [u for u in urls if u.startswith(('http://', 'https://'))]
                if abs_in_group:
                    absolute_urls = abs_in_group
                    break
    
    return absolute_urls

def fetch_alto_page(url, paper_name, page_num):
    """Eine einzelne ALTO-Seite herunterladen und Text extrahieren."""
    # URL-Validierung
    if not url or not url.startswith(('http://', 'https://')):
        print(f"    {paper_name} S.{page_num}: Ungültige URL übersprungen: {url[:50]}")
        return ""
    
    try:
        # Für DDB Binary-URLs Auth-Header mitschicken
        headers = {}
        if 'deutsche-digitale-bibliothek.de' in url:
            headers = HEADERS_DDB
        
        resp = requests.get(url, headers=headers, timeout=30)
        if resp.status_code == 200:
            text = extract_alto_text(resp.content)
            return text
        else:
            print(f"    {paper_name} S.{page_num}: HTTP {resp.status_code}")
            return ""
    except Exception as e:
        print(f"    {paper_name} S.{page_num}: Fehler: {e}")
        return ""

def fetch_ddb_newspaper(item_id, paper_name, max_pages=None):
    """Vollständigen Text einer DDB-Zeitung herunterladen (alle Seiten)."""
    print(f"  {paper_name} (ID: {item_id[:20]}...)")
    
    mets_xml = fetch_mets_record(item_id, paper_name)
    if not mets_xml:
        return ""
    
    alto_urls = extract_alto_urls_from_mets(mets_xml)
    if not alto_urls:
        print(f"    {paper_name}: Keine ALTO-URLs im METS gefunden")
        return ""
    
    if max_pages:
        alto_urls = alto_urls[:max_pages]
    
    print(f"    {paper_name}: {len(alto_urls)} Seiten gefunden")
    
    all_text = []
    for i, url in enumerate(alto_urls):
        text = fetch_alto_page(url, paper_name, i + 1)
        if text:
            all_text.append(f"--- Seite {i+1} ---\n\n{text}")
        time.sleep(0.5)
    
    full_text = '\n\n'.join(all_text)
    print(f"    {paper_name}: {len(full_text)} Zeichen extrahiert")
    return full_text

def _gallica_get(url, headers, max_retries=3):
    """HTTP-GET mit Retry bei 429 (Too Many Requests)."""
    for attempt in range(max_retries):
        try:
            r = requests.get(url, headers=headers, timeout=30)
            if r.status_code == 429:
                wait = 15 * (attempt + 1)
                print(f"    ⏳ Gallica 429 – warte {wait}s (Versuch {attempt + 1}/{max_retries})")
                time.sleep(wait)
                continue
            return r
        except Exception as e:
            if attempt < max_retries - 1:
                time.sleep(10)
                continue
            raise
    return r


def fetch_gallica(series_ark, name, date_str):
    """Französische Zeitung von Gallica herunterladen (alle Seiten)."""
    date_compact = date_str.replace("-", "")

    # Schritt 1: ARK-ID der Ausgabe finden
    url = f"https://gallica.bnf.fr/ark:/12148/{series_ark}/date{date_compact}.item"
    try:
        resp = _gallica_get(url, HEADERS_GALLICA)
        if resp.status_code != 200:
            print(f"  {name}: Lookup HTTP {resp.status_code}")
            return ""
        arks = re.findall(r"(bpt6k[0-9a-z]+)", resp.text)
        if not arks:
            print(f"  {name}: Keine Ausgabe für {date_str}")
            return ""
        ark = arks[0]
        print(f"  {name}: ARK {ark}")
    except Exception as e:
        print(f"  {name}: Fehler beim ARK-Lookup: {e}")
        return ""

    # Schritt 2: Alle Seiten als ALTO herunterladen
    all_text = []
    page = 1
    max_pages = 20

    while page <= max_pages:
        alto_url = f"https://gallica.bnf.fr/RequestDigitalElement?O={ark}&E=ALTO&Deb={page}"
        try:
            r = _gallica_get(alto_url, HEADERS_GALLICA)
            if r.status_code == 429:
                print(f"    {name}: Rate-Limit bei Seite {page}, breche ab")
                break
            if r.status_code == 200 and len(r.content) > 200:
                text = extract_alto_text(r.content)
                if text and len(text.strip()) > 50:
                    all_text.append(f"--- Seite {page} ---\n{text}")
                    print(f"    {name} S.{page}: {len(text)} Zeichen")
                    page += 1
                    time.sleep(5)  # 5s statt 2.5s
                else:
                    break
            else:
                break
        except Exception as e:
            print(f"    {name} S.{page}: Fehler: {e}")
            break

    full_text = '\n\n'.join(all_text)
    print(f"  {name}: Gesamt {len(full_text)} Zeichen, {len(all_text)} Seiten")
    return full_text

def fetch_loc_newspaper(lccn, name, date_str, max_pages=20):
    """US-Zeitung von der Library of Congress (Chronicling America) herunterladen.
    
    Funktioniert für alle Zeitungen im LoC-Bestand per LCCN-Code.
    Die LoC liefert den OCR-Text zeilenweise (ca. 30-40 Zeichen pro Zeile,
    entsprechend der Zeitungsspalten). Wir gruppieren aufeinanderfolgende
    Zeilen zu Absätzen, wobei kurze Zeilen (<15 Zeichen) als Überschriften
    oder Absatztrenner behandelt werden.
    """
    all_text = []
    page = 1
    
    while page <= max_pages:
        url = f"https://www.loc.gov/resource/{lccn}/{date_str}/ed-1/?sp={page}&st=text&fo=json"
        try:
            resp = requests.get(url, timeout=30)
            if resp.status_code != 200:
                break
            
            data = resp.json()
            
            # Volltext aus fulltext_service abrufen
            ft_url = data.get("fulltext_service", "")
            if not ft_url:
                break
            
            ft_resp = requests.get(ft_url, timeout=30)
            if ft_resp.status_code != 200:
                break
            
            # JSON parsen: { "pfad": { "full_text": "..." } }
            try:
                ft_data = ft_resp.json()
                text = ""
                for key, val in ft_data.items():
                    text = val.get("full_text", "")
                    break
            except (json.JSONDecodeError, AttributeError):
                text = ft_resp.text
            
            if text and len(text.strip()) > 50:
                # OCR-Zeilen zu Absätzen gruppieren
                paragraphed_text = _merge_ocr_lines_to_paragraphs(text)
                all_text.append(f"--- Seite {page} ---\n\n{paragraphed_text}")
                print(f"    {name} S.{page}: {len(text)} Zeichen")
                page += 1
                time.sleep(0.5)
            else:
                break
        except Exception as e:
            print(f"    {name} S.{page}: Fehler: {e}")
            break
    
    full_text = '\n\n'.join(all_text)
    print(f"  {name}: Gesamt {len(full_text)} Zeichen, {len(all_text)} Seiten")
    return full_text

def _merge_ocr_lines_to_paragraphs(raw_text):
    """OCR-Zeilen (ca. 30-40 Zeichen) zu sinnvollen Absätzen zusammenfassen.
    
    Heuristik: Kurze Zeilen (<15 Zeichen) gelten als Überschriften oder
    Absatztrenner und beginnen einen neuen Absatz. Aufeinanderfolgende
    längere Zeilen werden zu einem Fließtext-Absatz verbunden.
    """
    lines = raw_text.split('\n')
    paragraphs = []
    current_para = []
    
    for line in lines:
        stripped = line.strip()
        
        if not stripped:
            # Leerzeile = Absatzende
            if current_para:
                paragraphs.append(' '.join(current_para))
                current_para = []
        elif len(stripped) < 15 and stripped.isupper():
            # Kurze GROSSBUCHSTABEN-Zeile = Überschrift → neuer Absatz
            if current_para:
                paragraphs.append(' '.join(current_para))
                current_para = []
            paragraphs.append(stripped)
        else:
            current_para.append(stripped)
    
    if current_para:
        paragraphs.append(' '.join(current_para))
    
    return '\n\n'.join(paragraphs)

def fetch_nyt_articles(date_str):
    """NYT-Artikel-Metadaten für einen Tag."""
    d = datetime.strptime(date_str, "%Y-%m-%d")
    url = f"https://api.nytimes.com/svc/archive/v1/{d.year}/{d.month}.json?api-key={NYT_API_KEY}"
    try:
        resp = requests.get(url, timeout=60)
        if resp.status_code == 200:
            all_articles = resp.json().get("response", {}).get("docs", [])
            day_articles = [a for a in all_articles if a.get("pub_date", "").startswith(date_str)]
            print(f"  NYT: {len(day_articles)} Artikel für {date_str}")
            
            # Zu Text konvertieren
            lines = []
            for a in day_articles:
                headline = a.get("headline", {}).get("main", "")
                abstract = a.get("abstract", "")
                page = a.get("print_page", "?")
                keywords = ", ".join([k.get("value", "") for k in a.get("keywords", [])])
                mat_type = a.get("type_of_material", "")
                lines.append(f"[S.{page}] [{mat_type}] {headline}")
                if abstract:
                    lines.append(f"  → {abstract}")
                if keywords:
                    lines.append(f"  Schlagwörter: {keywords}")
                lines.append("")
            
            return "\n".join(lines), day_articles
        else:
            print(f"  NYT: HTTP {resp.status_code}")
            return "", []
    except Exception as e:
        print(f"  NYT: Fehler: {e}")
        return "", []

# ============ TROVE (National Library of Australia) ============

TROVE_NEWSPAPERS = {
    "The Sydney Morning Herald": 35,
    "The Argus": 13,
    "The Brisbane Courier": 68,
}


def fetch_trove_newspapers(date_str, verbose=False):
    """
    Holt australische Zeitungstexte von Trove (NLA) für ein Datum.
    Nutzt die Trove API v3 (newspaper/article endpoint).
    Rückgabe: Tuple (Dict {Zeitungsname: Text}, Dict {Zeitungsname: troveUrl})
    """
    if not TROVE_API_KEY:
        print("  ⚠ TROVE_API_KEY nicht gesetzt – Trove wird übersprungen")
        return {}, {}

    results = {}
    trove_urls = {}  # {paper_name: URL der ersten Trove-Seite}

    for paper_name, title_id in TROVE_NEWSPAPERS.items():
        try:
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
                print(f"  ⚠ Trove API-Key ungültig (HTTP 403)")
                return {}
            if r.status_code != 200:
                if verbose:
                    print(f"  {paper_name}: HTTP {r.status_code}")
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
                    print(f"  {paper_name}: Keine Artikel für {date_str}")
                continue

            # Artikeltexte zusammenbauen
            page_texts = []
            for article in articles[:20]:
                title = article.get("heading", "")
                text = article.get("articleText", "")
                if text:
                    text = re.sub(r"<[^>]+>", " ", text)
                    text = re.sub(r"\s+", " ", text).strip()
                    if len(text) > 50:
                        page_texts.append(f"[{title}]\n{text}")

            if page_texts:
                results[paper_name] = "\n\n".join(page_texts)
                # Erste Trove-URL als spezifische Quellenreferenz merken
                if articles:
                    trove_url = articles[0].get("troveUrl", "")
                    if trove_url:
                        trove_urls[paper_name] = trove_url
                if verbose:
                    print(f"  {paper_name}: {len(page_texts)} Artikel, {len(results[paper_name]):,} Zeichen")

        except Exception as e:
            print(f"  {paper_name}: Fehler – {e}")

        time.sleep(1.0)

    return results, trove_urls


# ============ PRAVDA (archive.org) ============

_RUSSIAN_MONTHS_GENITIVE = {
    1: "января", 2: "февраля", 3: "марта", 4: "апреля",
    5: "мая", 6: "июня", 7: "июля", 8: "августа",
    9: "сентября", 10: "октября", 11: "ноября", 12: "декабря",
}


def _estimate_pravda_issue_number(date_str):
    """
    Schätzt die Pravda-Ausgabenummer für ein Datum (1926).
    Kalibriert an bekannten Ankerpunkten:
      Nr. 48 = 27.02.1926, Nr. 50 = 02.03.1926, Nr. 54 = 06.03.1926
    """
    year, month, day = [int(x) for x in date_str.split("-")]
    target = _dt.date(year, month, day)
    anchor_date = _dt.date(1926, 2, 27)
    anchor_nr = 48
    delta_days = (target - anchor_date).days
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
    """
    year, month, day = [int(x) for x in date_str.split("-")]
    month_name = _RUSSIAN_MONTHS_GENITIVE.get(month, "")
    date_pattern = f"{day} {month_name} {year}"

    estimate = _estimate_pravda_issue_number(date_str)
    candidates = list(range(max(1, estimate - 3), estimate + 4))

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
                print(f"  Pravda Nr. {nr}: {identifier} – {title}")

            djvu_filename = _pravda_find_djvutxt_filename(identifier, verbose=verbose)
            if not djvu_filename:
                djvu_filename = f"{identifier}_djvu.txt"

            txt_url = f"https://archive.org/download/{identifier}/{requests.utils.quote(djvu_filename)}"
            r_txt = requests.get(txt_url, timeout=60, headers={
                "User-Agent": "Vor100Jahren-Bot/1.0 (historical research)"
            })

            if r_txt.status_code != 200:
                if verbose:
                    print(f"  Pravda Nr. {nr}: DjVuTXT nicht verfügbar ({r_txt.status_code})")
                continue

            text = r_txt.text
            if len(text) < 200:
                continue

            if date_pattern in text:
                if verbose:
                    print(f"  ✓ Pravda Nr. {nr}: Datum {date_pattern} bestätigt, {len(text):,} Zeichen")
                return text[:80000]

            if nr == estimate and not best_text:
                best_text = text[:80000]
                best_nr = nr

        except Exception as e:
            if verbose:
                print(f"  Pravda Nr. {nr}: Fehler – {e}")

        time.sleep(1.5)

    if best_text:
        if verbose:
            print(f"  ⚠ Pravda Nr. {best_nr}: Datum nicht im OCR, verwende Schätzung ({len(best_text):,} Zeichen)")
        return best_text

    return ""


# ============ BNE: EL SOL (Hemeroteca Digital) ============

# Fester Parent-UUID für "El Sol (Madrid. 1917)" in der BNE Hemeroteca Digital
BNE_EL_SOL_PARENT = "0312db23-f8f4-4c11-98ef-fcaa99442e30"
BNE_EL_SOL_START = datetime.strptime("1917-11-29", "%Y-%m-%d").date()


def _bne_find_issue_uuid(date_str, verbose=False):
    """Findet die Issue-UUID für El Sol an einem bestimmten Datum."""
    target = datetime.strptime(date_str, "%Y-%m-%d").date()
    delta_days = (target - BNE_EL_SOL_START).days
    if delta_days < 0:
        return None
    estimated_offset = max(0, int(delta_days * 0.92) - 5)

    base_url = (
        f"https://hemerotecadigital.bne.es/hd/es/results"
        f"?parent={BNE_EL_SOL_PARENT}&t=alt-asc"
    )
    target_fmt = f"{target.day}/{target.month}/{target.year}"

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

        dates_found = []
        for i in range(2, len(name_parts), 3):
            dates_found.append(name_parts[i])

        if not dates_found:
            return None

        for idx, d in enumerate(dates_found):
            if d == target_fmt and idx < len(unique_viewers):
                if verbose:
                    print(f"    El Sol: Gefunden bei Offset {estimated_offset + idx}")
                return unique_viewers[idx]

        first_date_str = dates_found[0]
        try:
            d, m, y = map(int, first_date_str.split("/"))
            first_date = datetime.strptime(f"{y}-{m:02d}-{d:02d}", "%Y-%m-%d").date()
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
                estimated_offset += 5
        except (ValueError, IndexError):
            estimated_offset += 10

        if verbose:
            print(f"    BNE: Versuch {attempt+1}, Offset → {estimated_offset} "
                  f"(erste Ausgabe: {first_date_str})")

    return None


def fetch_bne_el_sol(date_str, max_pages=10, verbose=False):
    """Holt El Sol von der BNE Hemeroteca Digital.

    Rückgabe: Tuple (text, issue_url) – issue_url ist die spezifische
    Ausgabe-URL oder None, wenn keine Ausgabe gefunden wurde.
    """
    from bs4 import BeautifulSoup

    issue_uuid = _bne_find_issue_uuid(date_str, verbose=verbose)
    if not issue_uuid:
        print(f"    El Sol: Keine Ausgabe für {date_str} gefunden")
        return "", None

    issue_url = (
        f"https://hemerotecadigital.bne.es/hd/es/results"
        f"?parent={issue_uuid}&t=alt-asc"
    )
    try:
        r = requests.get(issue_url, timeout=90)
        if r.status_code != 200:
            text_url = f"https://hemerotecadigital.bne.es/hd/es/text?id={issue_uuid}"
            r2 = requests.get(text_url, timeout=90)
            if r2.status_code == 200 and len(r2.text) > 100:
                text = BeautifulSoup(r2.text, "html.parser").get_text(separator="\n")
                return re.sub(r"\n{3,}", "\n\n", text).strip(), issue_url
            return "", issue_url
    except Exception as e:
        if verbose:
            print(f"    BNE-Seiten-Anfrage fehlgeschlagen: {e}")
        return "", None

    page_uuids = re.findall(r'/hd/es/low\?id=([a-f0-9-]+)', r.text)
    page_uuids = list(dict.fromkeys(page_uuids))

    if not page_uuids:
        text_url = f"https://hemerotecadigital.bne.es/hd/es/text?id={issue_uuid}"
        try:
            r2 = requests.get(text_url, timeout=90)
            if r2.status_code == 200 and len(r2.text) > 100:
                text = BeautifulSoup(r2.text, "html.parser").get_text(separator="\n")
                return re.sub(r"\n{3,}", "\n\n", text).strip(), issue_url
        except Exception:
            pass
        return "", issue_url

    if verbose:
        print(f"    El Sol: {len(page_uuids)} Seiten gefunden")

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

    return "\n\n".join(all_text), issue_url


# ============ WASHINGTON POST (archive.org) ============


def fetch_washington_post(date_str, verbose=False):
    """
    Holt die Washington Post für ein Datum von archive.org.
    Identifier-Muster: per_washington-post_YYYY-MM-DD_NNNNN
    """
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
                print(f"  Washington Post: Keine Ausgabe für {date_str}")
            return ""

        identifier = docs[0]["identifier"]
        if verbose:
            print(f"  Washington Post: {identifier}")

        txt_url = f"https://archive.org/download/{identifier}/{identifier}_djvu.txt"
        r_txt = requests.get(txt_url, timeout=60, headers={
            "User-Agent": "Vor100Jahren-Bot/1.0 (historical research)"
        })

        if r_txt.status_code != 200:
            if verbose:
                print(f"  Washington Post: DjVuTXT nicht verfügbar")
            return ""

        text = r_txt.text
        if len(text) > 80000:
            text = text[:80000]
        return text

    except Exception as e:
        if verbose:
            print(f"  Washington Post: Fehler – {e}")
        return ""


# ============ NORTH CHINA HERALD (archive.org) ============


def fetch_north_china_herald(date_str, verbose=False):
    """
    Holt den North China Herald von archive.org.
    Wochenzeitung (samstags) – sucht exaktes Datum und ±7 Tage.
    Identifier-Muster: north-china-herald-YYYY.MM.DD
    """
    from datetime import timedelta

    year, month, day = [int(x) for x in date_str.split("-")]
    target = datetime(year, month, day)

    candidates = [target + timedelta(days=d) for d in [0, -1, 1, -2, 2, -3, 3, -4, 4, -5, 5, -6, 6, -7, 7]]

    for candidate in candidates:
        date_dotted = candidate.strftime("%Y.%m.%d")
        identifier = f"north-china-herald-{date_dotted}"

        try:
            txt_url = f"https://archive.org/download/{identifier}/{date_dotted}_djvu.txt"
            r = requests.get(txt_url, timeout=60, headers={
                "User-Agent": "Vor100Jahren-Bot/1.0 (historical research)"
            })

            if r.status_code == 200 and len(r.text) > 500:
                text = r.text
                actual_date = candidate.strftime("%Y-%m-%d")
                if verbose:
                    print(f"  NCH: {identifier} ({len(text):,} Zeichen)")
                if actual_date != date_str:
                    print(f"  ℹ North China Herald: nächste Ausgabe {actual_date} (Wochenblatt)")

                if len(text) > 80000:
                    text = text[:80000]
                return text

        except Exception:
            pass

        time.sleep(0.5)

    if verbose:
        print(f"  North China Herald: Keine Ausgabe nahe {date_str}")
    return ""


# ============ ANNO – Österreichische Nationalbibliothek ============

ANNO_NEWSPAPERS = {
    "nfp": ("Neue Freie Presse", 20),
    "wrz": ("Wiener Zeitung", 12),
}
ANNO_BACKUP = {"wrz"}


def fetch_anno_newspaper(aid, date_str, max_pages=20, verbose=False):
    """Holt Zeitungstext von ANNO (ÖNB) für ein Datum."""
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
                print(f"  ANNO {aid} Seite {page}: {e}")
            break
        time.sleep(0.5)

    if not all_text:
        return ""

    text = "\n\n".join(all_text)
    if len(text) > 80000:
        text = text[:80000]
    return text


def fetch_all_anno(date_str, verbose=False, need_backup=False):
    """Holt ANNO-Zeitungen. Backup-Titel nur wenn need_backup=True."""
    results = {}
    for aid, (name, max_pages) in ANNO_NEWSPAPERS.items():
        if aid in ANNO_BACKUP and not need_backup:
            continue
        text = fetch_anno_newspaper(aid, date_str, max_pages, verbose)
        if text:
            results[name] = text
            print(f"  ✓ {name}: {len(text):,} Zeichen, {text.count('--- Seite')} Seiten")
        else:
            print(f"  ✗ {name}: nicht verfügbar für {date_str}")
    return results


# ============ DELPHER – Koninklijke Bibliotheek (NL) ============

DELPHER_NEWSPAPERS = {
    "Nieuwe Rotterdamsche Courant": "Nieuwe Rotterdamsche Courant",
}


def fetch_delpher_newspaper(paper_title, date_str, max_articles=30, verbose=False):
    """Holt Zeitungsartikel von Delpher (KB.nl) via SRU-API.

    Rückgabe: Tuple (text, delpher_viewer_url) – delpher_viewer_url ist
    die spezifische Viewer-URL oder None.
    """
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
            return "", None
    except Exception as e:
        if verbose:
            print(f"  Delpher SRU: {e}")
        return "", None

    urns = re.findall(r'<dc:identifier>(http://resolver\.kb\.nl/resolve\?urn=[^<]+:ocr)</dc:identifier>', r.text)

    if not urns:
        if verbose:
            print(f"  Delpher: Keine Artikel für {paper_title} am {date_str}")
        return "", None

    # Spezifische Delpher-Viewer-URL aus erster URN ableiten
    # URN-Format: http://resolver.kb.nl/resolve?urn=MMKB19:...:ocr
    # Viewer-URL: https://www.delpher.nl/nl/kranten/view?coll=ddd&identifier=MMKB19:...
    first_urn = urns[0]
    delpher_viewer_url = None
    urn_match = re.search(r'urn=([^:]+:[^:]+)', first_urn)
    if urn_match:
        urn_id = urn_match.group(1)
        delpher_viewer_url = f"https://www.delpher.nl/nl/kranten/view?coll=ddd&identifier={urn_id}"

    all_text = []
    for urn_url in urns[:max_articles]:
        try:
            r_ocr = requests.get(urn_url, timeout=30, headers={
                "User-Agent": "Vor100Jahren-Bot/1.0 (historical research)"
            })
            if r_ocr.status_code == 200 and len(r_ocr.text) > 50:
                text = r_ocr.text
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
        time.sleep(0.3)

    if not all_text:
        return "", delpher_viewer_url

    text = "\n\n".join(all_text)
    if len(text) > 80000:
        text = text[:80000]
    return text, delpher_viewer_url


def fetch_all_delpher(date_str, verbose=False):
    """Holt alle Delpher-Zeitungen für ein Datum.

    Rückgabe: Tuple (Dict {name: text}, Dict {name: viewer_url})
    """
    results = {}
    viewer_urls = {}
    for display_name, search_title in DELPHER_NEWSPAPERS.items():
        text, viewer_url = fetch_delpher_newspaper(search_title, date_str, verbose=verbose)
        if text:
            results[display_name] = text
            print(f"  ✓ {display_name}: {len(text):,} Zeichen")
        else:
            print(f"  ✗ {display_name}: nicht verfügbar für {date_str}")
        if viewer_url:
            viewer_urls[display_name] = viewer_url
    return results, viewer_urls


def run_download(date_str):
    """Stufe 0: Vollständiger Download aller Quellen.

    Prüft zunächst, ob ein Corpus aus Step 1 vorliegt (corpus_YYYY-MM-DD.json).
    Falls ja, wird dieser wiederverwendet und kein erneuter Download durchgeführt.
    """
    print("\n" + "="*60)
    print("STUFE 0: DOWNLOAD ALLER QUELLEN")
    print("="*60)

    # Prüfe ob Corpus aus Step 1 vorhanden ist
    corpus_file = os.path.join(OUTPUT_DIR, f"corpus_{date_str}.json")
    if os.path.exists(corpus_file):
        print(f"  ✓ Corpus aus Step 1 gefunden: {corpus_file}")
        with open(corpus_file, "r", encoding="utf-8") as f:
            corpus = json.load(f)
        total_chars = sum(len(t) for t in corpus.values())
        print(f"  Geladen: {len(corpus)} Quellen, {total_chars:,} Zeichen")
        print(f"  → Überspringe erneuten Download")
        stats = {"de_papers": 0, "de_pages": 0, "de_chars": 0,
                 "fr_papers": 0, "fr_pages": 0, "fr_chars": 0,
                 "us_papers": 0, "us_pages": 0, "us_chars": 0, "nyt_articles": 0}
        # Basisstatistik aus geladenem Corpus ableiten
        for name, text in corpus.items():
            chars = len(text)
            pages = text.count("--- Seite")
            if any(name.startswith(p) for p in ("Vorwärts", "Berliner", "Deutsche", "Kölnische", "Reichsanzeiger", "Badische", "Sächsische", "Harburger", "Westfälischer", "Hamburger")):
                stats["de_papers"] += 1
                stats["de_chars"] += chars
                stats["de_pages"] += pages
            elif name.startswith("Le "):
                stats["fr_papers"] += 1
                stats["fr_chars"] += chars
                stats["fr_pages"] += pages
            elif name in ("The Washington Times", "Evening Star", "Washington Post"):
                stats["us_papers"] += 1
                stats["us_chars"] += chars
                stats["us_pages"] += pages
            elif name == "NYT (Metadaten)":
                stats["nyt_articles"] = text.count("\n---\n") + 1
        # Bei gecachtem Corpus stehen keine spezifischen URLs zur Verfügung
        return corpus, stats, {}

    print(f"  Kein vorhandener Corpus – starte frischen Download ...")

    corpus = {}
    stats = {"de_papers": 0, "de_pages": 0, "de_chars": 0,
             "fr_papers": 0, "fr_pages": 0, "fr_chars": 0,
             "us_papers": 0, "us_pages": 0, "us_chars": 0, "nyt_articles": 0}

    # Spezifische URLs sammeln für Quellenapparat
    source_urls = {
        "ddb_item_ids": {},    # {display_name: item_id}
        "trove_urls": {},      # {paper_name: trove_article_url}
        "bne_issue_url": None, # spezifische BNE-Ausgabe-URL
        "delpher_urls": {},    # {paper_name: resolver_url}
    }

    # 1. Deutsche Zeitungen (DDB)
    print("\n--- Deutsche Zeitungen (DDB) ---")
    docs = find_newspapers(date_str)
    selected = select_key_papers(docs)
    source_urls["ddb_item_ids"] = dict(selected)  # Item-IDs für Quellenapparat merken
    print(f"  Ausgewählt: {len(selected)} von {len(docs)} Ausgaben")
    
    for name, item_id in selected.items():
        text = fetch_ddb_newspaper(item_id, name)
        if text:
            corpus[name] = text
            stats["de_papers"] += 1
            stats["de_chars"] += len(text)
            stats["de_pages"] += text.count("--- Seite")
    
    # 2. Französische Zeitungen (Gallica)
    print("\n--- Französische Zeitungen (Gallica) ---")
    for name, ark in GALLICA_SERIES.items():
        text = fetch_gallica(ark, name, date_str)
        if not text:
            # Retry nach Pause (Gallica Rate-Limiting)
            print(f"    ⏳ {name}: erster Versuch leer – Retry in 10s ...")
            time.sleep(10)
            text = fetch_gallica(ark, name, date_str)
        if text:
            corpus[name] = text
            stats["fr_papers"] += 1
            stats["fr_chars"] += len(text)
            stats["fr_pages"] += text.count("--- Seite")
        else:
            print(f"    ✗ {name}: nicht verfügbar")
        time.sleep(5)  # Pause zwischen Zeitungen gegen Rate-Limiting
    
    # 3. US-Zeitungen (Library of Congress – Washington Times primär)
    print("\n--- US-Zeitungen (Library of Congress: Washington Times) ---")
    for lccn, (paper_name, _display, max_pages) in LOC_NEWSPAPERS.items():
        if lccn in LOC_BACKUP:
            continue  # Evening Star nur als Backup
        text = fetch_loc_newspaper(lccn, paper_name, date_str, max_pages)
        if text:
            corpus[paper_name] = text
            stats["us_papers"] += 1
            stats["us_chars"] += len(text)
            stats["us_pages"] += text.count("--- Seite")

    # 4. NYT (Metadaten)
    print("\n--- New York Times (Archive API) ---")
    nyt_text, nyt_articles = fetch_nyt_articles(date_str)
    if nyt_text:
        corpus["NYT (Metadaten)"] = nyt_text
        stats["nyt_articles"] = len(nyt_articles)

    # 5. Washington Post (archive.org)
    print("\n--- Washington Post (archive.org) ---")
    stats["wapo_chars"] = 0
    wapo_text = fetch_washington_post(date_str)
    if wapo_text:
        corpus["Washington Post"] = wapo_text
        stats["wapo_chars"] = len(wapo_text)
        print(f"  ✓ Washington Post: {len(wapo_text):,} Zeichen")
    else:
        print(f"  ✗ Washington Post: nicht verfügbar für {date_str}")
        # Evening Star als Backup aktivieren
        if stats["us_papers"] == 0:
            print("  ↳ Aktiviere Evening Star als Backup ...")
            for lccn, (paper_name, _display, max_pages) in LOC_NEWSPAPERS.items():
                if lccn in LOC_BACKUP:
                    text = fetch_loc_newspaper(lccn, paper_name, date_str, max_pages)
                    if text:
                        corpus[paper_name] = text
                        stats["us_papers"] += 1
                        stats["us_chars"] += len(text)
                        stats["us_pages"] += text.count("--- Seite")

    # 6. Österreichische Zeitungen (ANNO)
    print("\n--- Österreichische Zeitungen (ANNO) ---")
    stats["anno_papers"] = 0
    stats["anno_chars"] = 0
    anno_results = fetch_all_anno(date_str, need_backup=False)
    if not anno_results:
        print("  ↳ NFP leer – aktiviere Wiener Zeitung als Backup ...")
        anno_results = fetch_all_anno(date_str, need_backup=True)
    for name, text in anno_results.items():
        corpus[name] = text
        stats["anno_papers"] += 1
        stats["anno_chars"] += len(text)

    # 7. Niederländische Zeitungen (Delpher)
    print("\n--- Niederländische Zeitungen (Delpher) ---")
    stats["delpher_chars"] = 0
    delpher_results, delpher_viewer_urls = fetch_all_delpher(date_str)
    source_urls["delpher_urls"] = delpher_viewer_urls
    for name, text in delpher_results.items():
        corpus[name] = text
        stats["delpher_chars"] += len(text)

    # 8. Australische Zeitungen (Trove)
    print("\n--- Australische Zeitungen (Trove) ---")
    trove_results, trove_page_urls = fetch_trove_newspapers(date_str)
    source_urls["trove_urls"] = trove_page_urls
    stats["au_papers"] = 0
    stats["au_chars"] = 0
    for name, text in trove_results.items():
        corpus[name] = text
        stats["au_papers"] += 1
        stats["au_chars"] += len(text)

    # 7. North China Herald (archive.org)
    print("\n--- North China Herald (archive.org) ---")
    stats["nch_chars"] = 0
    nch_text = fetch_north_china_herald(date_str)
    if nch_text:
        corpus["North China Herald"] = nch_text
        stats["nch_chars"] = len(nch_text)
        print(f"  ✓ North China Herald: {len(nch_text):,} Zeichen")
    else:
        print(f"  ✗ North China Herald: nicht verfügbar nahe {date_str}")

    # 10. Pravda (archive.org)
    print("\n--- Pravda (archive.org) ---")
    pravda_text = fetch_pravda(date_str)
    stats["pravda_chars"] = 0
    if pravda_text:
        corpus["Pravda"] = pravda_text
        stats["pravda_chars"] = len(pravda_text)
        print(f"  ✓ Pravda: {len(pravda_text):,} Zeichen")
    else:
        print(f"  ✗ Pravda: nicht verfügbar für {date_str}")

    # 11. El Sol (BNE – Hemeroteca Digital)
    print("\n--- El Sol (BNE – Hemeroteca Digital) ---")
    stats["bne_chars"] = 0
    bne_text, bne_issue_url = fetch_bne_el_sol(date_str)
    if bne_issue_url:
        source_urls["bne_issue_url"] = bne_issue_url
    if bne_text:
        corpus["El Sol"] = bne_text
        stats["bne_chars"] = len(bne_text)
        print(f"  ✓ El Sol: {len(bne_text):,} Zeichen")
    else:
        print(f"  ✗ El Sol: nicht verfügbar für {date_str}")

    # Corpus speichern
    corpus_file = os.path.join(OUTPUT_DIR, f"corpus_{date_str}.json")
    with open(corpus_file, 'w', encoding='utf-8') as f:
        json.dump(corpus, f, ensure_ascii=False, indent=2)

    total_chars = sum(len(t) for t in corpus.values())
    print(f"\n--- Download-Statistik ---")
    print(f"  Deutsche Zeitungen: {stats['de_papers']} Titel, {stats['de_pages']} Seiten, {stats['de_chars']:,} Zeichen")
    print(f"  Französische Zeitungen: {stats['fr_papers']} Titel, {stats['fr_pages']} Seiten, {stats['fr_chars']:,} Zeichen")
    print(f"  US-Zeitungen (LoC): {stats['us_papers']} Titel, {stats['us_pages']} Seiten, {stats['us_chars']:,} Zeichen")
    print(f"  NYT: {stats['nyt_articles']} Artikelmetadaten")
    print(f"  Washington Post: {stats['wapo_chars']:,} Zeichen")
    print(f"  ANNO (Österreich): {stats['anno_papers']} Titel, {stats['anno_chars']:,} Zeichen")
    print(f"  Delpher (NL): {stats['delpher_chars']:,} Zeichen")
    print(f"  Australische Zeitungen: {stats['au_papers']} Titel, {stats['au_chars']:,} Zeichen")
    print(f"  North China Herald: {stats['nch_chars']:,} Zeichen")
    print(f"  Pravda: {stats['pravda_chars']:,} Zeichen")
    print(f"  El Sol (BNE): {stats['bne_chars']:,} Zeichen")
    print(f"  GESAMT: {total_chars:,} Zeichen in {len(corpus)} Quellen")
    print(f"  Gespeichert: {corpus_file}")

    return corpus, stats, source_urls

# ============ STUFE 1: ARTIKELAUSWAHL ============

def extract_keywords_from_topic(topic, snippet=""):
    """Extrahiert Suchbegriffe aus Thema und Textauszug per Heuristik.

    Filtert Stoppwörter und kurze Wörter heraus, gibt relevante
    Begriffe für die Kontextsuche im Zeitungskorpus zurück.
    """
    stopwords = {
        "der", "die", "das", "den", "dem", "des", "ein", "eine", "einer", "eines",
        "und", "oder", "aber", "als", "auch", "auf", "aus", "bei", "bis", "für",
        "mit", "nach", "über", "von", "vor", "wie", "wird", "hat", "ist", "sind",
        "war", "zur", "zum", "sich", "nicht", "noch", "nur", "wird", "soll",
        "the", "and", "for", "from", "with", "that", "this", "was", "are",
        "les", "des", "une", "dans", "pour", "sur", "par", "est", "qui",
    }

    text = f"{topic} {snippet}"
    # Wörter extrahieren, Satzzeichen entfernen
    words = re.findall(r"[A-ZÀ-Ýa-zà-ý]{3,}", text)

    keywords = []
    seen = set()
    for word in words:
        lower = word.lower()
        if lower not in stopwords and lower not in seen and len(word) >= 4:
            seen.add(lower)
            keywords.append(word)

    return keywords[:20]  # Max 20 Keywords


def parse_target_words(estimated_words):
    """Parst '800-1200' zu 1000 (Mittelwert) oder direkte Zahl."""
    if isinstance(estimated_words, int):
        return estimated_words
    if isinstance(estimated_words, str):
        # Format: "800-1200" oder "800" oder "ca. 500"
        numbers = re.findall(r"\d+", estimated_words)
        if len(numbers) >= 2:
            return (int(numbers[0]) + int(numbers[1])) // 2
        elif numbers:
            return int(numbers[0])
    # Fallback je nach Artikeltyp
    return 500


def generate_keywords_via_claude(topic, snippet=""):
    """Generiert mehrsprachige Suchbegriffe per Claude API (Fallback).

    Wird nur aufgerufen, wenn search_keywords in der Step-1-JSON fehlen.
    """
    from anthropic import Anthropic

    try:
        client = Anthropic(api_key=ANTHROPIC_API_KEY)
        response = client.messages.create(
            model="claude-haiku-3-20240307",
            max_tokens=300,
            messages=[{
                "role": "user",
                "content": f"""Generiere 15-20 Suchbegriffe für eine OCR-Kontextsuche in Zeitungen von 1926.

Thema: {topic}
Kontext: {snippet}

Liefere Begriffe auf Deutsch, Französisch und Englisch.
Berücksichtige auch Personennamen, Ortsnamen und OCR-typische Varianten (z.B. ohne Umlaute).

Antworte NUR mit einem JSON-Array von Strings, z.B.:
["Begriff1", "Begriff2", "terme_français", "english_term"]"""
            }],
        )
        raw = response.content[0].text.strip()
        return json.loads(raw)
    except Exception as e:
        print(f"      Claude-Keywords-Fallback fehlgeschlagen: {e}")
        return []


def load_article_plan(json_path):
    """Lädt den Artikelplan aus der Step-1-Auswahl-JSON.

    Liest die Datei aus step1_select.py mit selection.status='completed'
    und konvertiert die Auswahl in das interne article_plan-Format.
    """
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    date_str = data.get("date_historical", TARGET_DATE)
    selection = data.get("selection", {})

    if selection.get("status") != "completed":
        print("FEHLER: Keine abgeschlossene Artikelauswahl in der JSON-Datei.")
        print("  Bitte zuerst step1_select.py ausführen.")
        sys.exit(1)

    selected = selection.get("selected_articles", [])
    if not selected:
        print("FEHLER: Keine Artikel in der Auswahl gefunden.")
        sys.exit(1)

    # Proposals für Snippet-Zugriff indexieren
    proposals_by_rank = {}
    for p in data.get("proposals", []):
        proposals_by_rank[p.get("rank")] = p

    articles = []
    for article in selected:
        rank = article.get("rank", 0)
        proposal = proposals_by_rank.get(rank, {})

        # Keywords: Primär aus Step-1-JSON (search_keywords), Fallback Heuristik
        keywords = proposal.get("search_keywords", [])
        snippet = proposal.get("snippet", "")

        if keywords:
            print(f"    Nr. {rank}: {len(keywords)} Keywords aus Step 1")
        else:
            # Fallback: Heuristik + Claude-Ergänzung
            keywords = extract_keywords_from_topic(article["topic"], snippet)
            claude_keywords = generate_keywords_via_claude(article["topic"], snippet)
            if claude_keywords:
                # Zusammenführen, Duplikate entfernen
                seen = {kw.lower() for kw in keywords}
                for kw in claude_keywords:
                    if kw.lower() not in seen:
                        seen.add(kw.lower())
                        keywords.append(kw)
            print(f"    Nr. {rank}: {len(keywords)} Keywords (Heuristik+Claude-Fallback)")

        # Wortanzahl parsen
        target_words = parse_target_words(article.get("estimated_words", "500"))

        articles.append({
            "rank": rank,
            "score": article.get("source_count", 0) * 10.0,
            "topic": article["topic"],
            "type": article.get("assigned_type", "Artikel"),
            "target_words": target_words,
            "category": article.get("category", "Vermischtes"),
            "keywords": keywords,
            "headline_suggestion": article.get("headline_suggestion", ""),
            "sources": article.get("sources", []),
        })

    print(f"  {len(articles)} Artikel aus Step-1-Auswahl geladen")
    return {"date": date_str, "articles": articles}

# ============ STUFE 2: KONTEXTEXTRAKTION ============

def extract_context(corpus, keywords, max_chars=18000):
    """Themenrelevante Absätze aus dem OCR-Korpus extrahieren.

    Zweistufige Auswahl:
    1. Pro Quelle den besten Treffer reservieren (Quellenvielfalt sichern)
    2. Restbudget mit den besten übrigen Treffern auffüllen
    """
    results = []

    for source_name, full_text in corpus.items():
        paragraphs = full_text.split('\n\n')

        for i, para in enumerate(paragraphs):
            if len(para.strip()) < 80:
                continue

            # Keyword-Score berechnen
            score = sum(1 for kw in keywords if kw.lower() in para.lower())

            if score > 0:
                # Kontextfenster: Absatz davor + Treffer + Absatz danach
                before = paragraphs[i-1] if i > 0 else ""
                after = paragraphs[i+1] if i < len(paragraphs) - 1 else ""

                results.append({
                    "text": f"{before}\n\n{para}\n\n{after}".strip(),
                    "keyword_score": score,
                    "source": source_name,
                    "paragraph_index": i
                })

    # Nach Keyword-Score sortieren
    results.sort(key=lambda x: -x["keyword_score"])

    # --- Phase 1: Quellenvielfalt sichern ---
    # Pro Quelle den besten Treffer reservieren
    best_per_source = {}
    for hit in results:
        src = hit["source"]
        if src not in best_per_source:
            best_per_source[src] = hit

    output = []
    total = 0
    seen_paragraphs = set()

    # Erst die besten Treffer jeder Quelle einfügen (nach Score sortiert)
    phase1_hits = sorted(best_per_source.values(), key=lambda x: -x["keyword_score"])
    for hit in phase1_hits:
        key = (hit["source"], hit["paragraph_index"])
        if total + len(hit["text"]) > max_chars:
            continue  # Skip statt break: evtl. passen kleinere Treffer noch
        seen_paragraphs.add(key)
        output.append(hit)
        total += len(hit["text"])

    # --- Phase 2: Restbudget mit besten übrigen Treffern auffüllen ---
    for hit in results:
        if total >= max_chars:
            break
        key = (hit["source"], hit["paragraph_index"])
        if key in seen_paragraphs:
            continue
        seen_paragraphs.add(key)

        if total + len(hit["text"]) > max_chars:
            continue
        output.append(hit)
        total += len(hit["text"])

    return output

def run_context_extraction(corpus, article_plan):
    """Stufe 2: Kontextextraktion für alle Artikel."""
    print("\n" + "="*60)
    print("STUFE 2: KONTEXTEXTRAKTION")
    print("="*60)
    
    contexts = {}
    
    for article in article_plan["articles"]:
        topic = article["topic"]
        keywords = article["keywords"]
        
        hits = extract_context(corpus, keywords)
        contexts[topic] = hits
        
        sources_found = set(h["source"] for h in hits)
        total_chars = sum(len(h["text"]) for h in hits)
        print(f"\n  [{article['type']}] {topic}")
        print(f"    Treffer: {len(hits)} Absätze aus {len(sources_found)} Quellen")
        print(f"    Quellen: {', '.join(sources_found)}")
        print(f"    Kontext: {total_chars:,} Zeichen")
    
    return contexts

# ============ STUFE 3: ARTIKELGENERIERUNG ============

ARTICLE_SYSTEM_PROMPT = """Du bist ein erfahrener Journalist der Weimarer Republik im Jahr 1926.
Du schreibst Zeitungsartikel im authentischen Stil eines deutschen Qualitätsblattes der 1920er Jahre.

Stilmerkmale:
- Gehobener, feuilletonistischer Ton der 1920er Jahre (z.B. "gleichwohl", "indes", "alsbald")
- Lange, verschachtelte Sätze mit Einschüben und Nebensätzen
- Indirekte Rede für Zitate und Berichte
- Datumszeilen wie "Berlin, 28. Februar." oder "Genf, 28. Februar. (Eigener Drahtbericht.)"
- Bezug auf andere Zeitungen als Quellen (z.B. "Wie das <em>Berliner Tageblatt</em> meldet...")
- Nüchterner, sachlicher Grundton mit gelegentlicher Wertung

WICHTIG – Anführungszeichen und Hervorhebungen (einheitliche Konvention):
Verwende Anführungszeichen und Kursivierung KONSISTENT nach folgenden Regeln:

1. DEUTSCHE ANFÜHRUNGSZEICHEN „…" — NUR für wörtliche Zitate:
   - Direkte Rede: Hörsing erklärte: „Das ganze Deutschland soll es sein."
   - Wörtlich zitierte Passagen aus Quellen: Die Prawda schrieb von einer „Vorbereitung
     des Krieges gegen die UdSSR".
   - NICHT verwenden für Zeitungsnamen, Werktitel, Hervorhebungen oder einzelne Begriffe.

2. KURSIV <em>…</em> — für Namen von Publikationen und Werktiteln:
   - Zeitungsnamen: Wie das <em>Berliner Tageblatt</em> meldet…
   - Buchtitel: In seiner Broschüre <em>Krieg und Kurie</em>…
   - Filmtitel: Der Monumentalfilm <em>Die letzten Tage von Pompeji</em>…
   - Schiffsnamen, Organisationszeitschriften etc.

3. EINFACHE ANFÜHRUNGSZEICHEN ‚…' — für distanzierende/ironische Verwendung:
   - Begriffe, von denen sich der Autor distanziert oder die in übertragener
     Bedeutung verwendet werden: die sogenannte ‚Friedenspolitik'
   - NICHT für wörtliche Zitate verwenden.

4. KEINE Anführungszeichen für bloße Hervorhebungen einzelner Wörter.
   FALSCH: Die „machtvolle" Demonstration…
   RICHTIG: Die machtvolle Demonstration…
   Oder, wenn Hervorhebung nötig: Die <em>machtvolle</em> Demonstration…

WICHTIG – Rechtschreibung und Grammatik:
- Verwende IMMER die aktuelle deutsche Rechtschreibung (Duden 2024).
- KEINE historischen Schreibweisen wie "daß", "muß", "Noth", "Theil", "eigenthümlich".
- Stattdessen: "dass", "muss", "Not", "Teil", "eigentümlich".
- Der Stil und Tonfall soll an die 1920er erinnern, die Orthografie muss aber modern sein.

WICHTIG – Satzzeichen und Gedankenstriche:
- Verwende AUSSCHLIESSLICH den Geviertstrich (Em-Dash) — (U+2014) als Gedankenstrich.
- KEIN Halbgeviertstrich (En-Dash) – (U+2013) als Gedankenstrich. Der En-Dash ist NUR für Zahlenbereiche (z.B. 1916–1922) zulässig.
- KEIN Bindestrich mit Leerzeichen ( - ) als Gedankenstrich.
- Gedankenstriche werden mit Leerzeichen umgeben: "Text — Einschub — Text".

WICHTIG – Eigennamen nicht-deutscher Herkunft:
- Verwende im Fließtext durchgängig die ZEITGENÖSSISCH-DEUTSCHE Schreibweise,
  wie sie in deutschen Zeitungen der 1920er Jahre üblich war.
  Beispiele: "Tschangtsolin" (nicht "Chang Tso-Lin"), "Fengjusian" (nicht "Feng Yu-Hsiang"),
  "Mussolini", "Briand", "Coolidge".
- Bei der ERSTEN Nennung einer Person ergänze die moderne Umschrift in Klammern,
  z.B. "Tschangtsolin (Zhang Zuolin)", "Fengjusian (Feng Yuxiang)".
- Verwende für jeden Namen EINE EINZIGE Schreibweise im gesamten Artikel.
  KEINE Mischung verschiedener Umschriftsysteme (Wade-Giles, Pinyin, deutsche Umschrift etc.).
- Für den [[Wikipedia-Marker]] verwende IMMER die MODERNE Form als Linkziel und die
  zeitgenössische als Anzeige: [[Zhang Zuolin|Tschangtsolin]], [[Feng Yuxiang|Fengjusian]].
  So wird der Link korrekt aufgelöst, während der Anzeigetext zum Zeitungsstil passt.
- Bei Ortsnamen: Verwende die zeitgenössische Form im Text und die moderne als Linkziel,
  z.B. [[Tianjin|Tientsin]], [[Beijing|Peking]]. Ausnahme: Wenn der moderne und der
  zeitgenössische Name identisch sind, genügt [[Name]].

WICHTIG – Wikipedia-Verlinkung (Qualitätsregeln):

GRUNDSATZ: Jeder Artikel MUSS mindestens 2–3 [[Wiki]]-Marker enthalten!
Wenn du am Ende deines Artikels weniger als 2 Marker gesetzt hast, prüfe erneut,
ob Personen, das zentrale Thema oder historische Konzepte verlinkbar sind.

A) ZENTRALES THEMA VERLINKEN:
- Das ZENTRALE THEMA oder Ereignis des Artikels MUSS als [[Wiki]]-Marker gesetzt
  werden, sofern ein passender Wikipedia-Artikel existiert.
  Beispiele: [[Rifkrieg (1921–1926)|Rif-Aufstand]], [[Völkerbund]],
  [[Prohibition in den Vereinigten Staaten|Prohibition]],
  [[Locarno-Verträge|Locarno-Pakt]], [[Dawes-Plan]].
- Das Thema sollte beim ERSTEN Vorkommen im Text verlinkt werden.

B) PERSONEN – VOLLSTÄNDIGE NAMEN UND PRIORITÄT:
- Bei ALLEN Personen MUSS der VOLLSTÄNDIGE NAME (Vor- und Zuname) als Linkziel
  verwendet werden. Suche den vollen Namen im Quellenmaterial.
  RICHTIG: [[Martin Schiele|Schiele]], [[Oskar Hergt|Hergt]], [[Jimmy Doolittle|Doolittle]]
  FALSCH:  [[Schiele]], [[Hergt]], [[Doolittle]] (ohne Vornamen als Linkziel)
- ALLE im Artikel namentlich genannten Personen mit enzyklopädischer Relevanz MÜSSEN
  verlinkt werden – auch wenn sie nur mit Nachnamen genannt werden.
- Personen, die in der HAUPTÜBERSCHRIFT oder UNTERZEILE genannt werden, sind die
  WICHTIGSTEN Verlinkungen. Stelle sicher, dass diese Personen IMMER verlinkt werden
  und im Artikeltext VOR weniger wichtigen Personen ihren [[Wiki]]-Marker erhalten.
- Wenn eine Person nur beiläufig ohne vollen Namen erwähnt wird oder du dir nicht
  sicher bist, welcher Wikipedia-Artikel gemeint ist, setze KEINEN Link.
- Der NACHNAME im Linkziel MUSS zum Anzeigenamen passen.
  RICHTIG:  [[Rudolf Ramek|Dr. Ramek]] (Nachname "Ramek" stimmt überein)
  FALSCH:   [[Ignaz Seipel|Dr. Ramek]] (Nachname "Seipel" ≠ "Ramek")
- Verlinke KEINE Personen, die offensichtlich keine enzyklopädische Relevanz haben
  (z.B. Verbrechensopfer, Unfallbeteiligte, lokale Privatpersonen).

C) QUELLENREGEL:
- Setze [[Wikipedia-Marker]] NUR für Personen, Orte und Begriffe, die im obigen
  Quellenmaterial NAMENTLICH GENANNT werden. Verlinke KEINE Personen oder Begriffe,
  die du aus eigenem Wissen ergänzt und die nicht in den Quellen erscheinen.

D) NICHT VERLINKEN:
- Verlinke KEINE Städtenamen oder Ortsnamen (z.B. Berlin, Paris, Wien, Genf, Tientsin,
  Washington, Moskau, London, Rom, Peking). Städte sollen als reiner Text erscheinen.
- Verlinke KEINE Ländernamen oder Staatenbezeichnungen (z.B. Deutschland, Frankreich,
  Sowjetunion, China, England, Vereinigte Staaten, Niederlande, Österreich).
- Verlinke KEINE Zeitungsnamen oder Pressebezeichnungen (z.B. Vossische Zeitung,
  Berliner Tageblatt, Le Temps, The Times, Vorwärts, Echo de Paris, Figaro).
  Zeitungsnamen werden in den Quellenangaben separat erfasst.
- Verlinke KEINE generischen Alltagsbegriffe (z.B. Truppen, Soldaten, Aufstand,
  Regierung, Parlament, Krieg). Nur SPEZIFISCHE Fachbegriffe, Ereignisse oder
  Konzepte mit eigenem Wikipedia-Artikel sind verlinkbar.

E) SCHREIBWEISE:
- Die Schreibweise des Linkziels folgt weiterhin den Eigennamen-Regeln oben
  (moderne Form als Linkziel).

Markierungen (werden später automatisch aufgelöst):
- [[Begriff]] für Wikipedia-Links (z.B. [[Völkerbund]], [[Gustav Stresemann|Stresemann]])
- [[Ziel|Anzeige]] für alternative Anzeige (z.B. [[Aristide Briand|Briand]])
- BEVORZUGE die [[Ziel|Anzeige]]-Form, IMMER mit vollständigem Namen als Linkziel!
- {{Bild: Beschreibung}} für ein passendes historisches Bild (max. 1 pro Artikel)

Antworte AUSSCHLIESSLICH mit einem validen JSON-Objekt."""

ARTICLE_USER_PROMPT = """DATUM: {date}
THEMA: {topic}
ARTIKELTYP: {article_type} ({target_words} Wörter)
KATEGORIE: {category}
HEADLINE-VORSCHLAG: {headline_suggestion}

QUELLENMATERIAL AUS ZEITUNGEN DES {date_upper}:
{context_material}

AUFGABE:
Schreibe einen {article_type} ({target_words} Wörter) zu obigem Thema.
Stütze dich AUSSCHLIESSLICH auf das Quellenmaterial und verarbeite es im Stil eines 1926er Zeitungsartikels.

STRENGE THEMENBINDUNG:
- Schreibe NUR über das angegebene Thema. Weiche NIEMALS auf andere Themen ab.
- Verwende NUR Informationen, die im obigen Quellenmaterial enthalten sind.
- Erfinde KEINE Fakten, Zitate, Ereignisse oder Details, die nicht im Quellenmaterial stehen.
- Wenn das Quellenmaterial für die Ziel-Wortanzahl nicht ausreicht, schreibe einen
  KÜRZEREN Artikel statt themenfremdes Material hinzuzufügen. Ein prägnanter, quellengestützter
  Kurzbeitrag ist besser als ein aufgeblähter Artikel mit erfundenen Passagen.
- Insbesondere: Mische KEINE Nachrichten aus anderen Themengebieten ein, nur weil sie
  zufällig im selben Quellenmaterial auftauchen.

QUELLENVIELFALT:
- Beziehe Material aus ALLEN zum Thema passenden Quellen im obigen Quellenmaterial ein.
- Verarbeite sowohl deutsche als auch ausländische Quellen (französisch, englisch, etc.).
- Typischer 1920er-Stil: "Wie der Pariser 'Temps' meldet...", "Nach Berichten der 'Times'...",
  "Die 'Neue Freie Presse' berichtet aus Wien...", "Aus New York wird gemeldet..."
- Jede Quelle, die THEMENRELEVANTES Material enthält, soll im Artikel verarbeitet werden.
- In der Quellenangabe (primary_sources) ALLE genutzten Zeitungen auflisten.

PFLICHT – [[Wikipedia]]-Marker (mindestens 2–3 pro Artikel!):
- Zentrales Thema: [[Artikelname|Anzeigename]] z.B. [[Rifkrieg (1921–1926)|Rif-Aufstand]]
- Personen: [[Vollständiger Name|Nachname]] z.B. [[Gustav Stresemann|Stresemann]]
- Historische Konzepte: [[Völkerbund]], [[Dawes-Plan]], [[Locarno-Verträge]]
- Personen aus der Überschrift haben HÖCHSTE PRIORITÄT und MÜSSEN verlinkt werden.
Füge genau einen {{{{Bild: Beschreibung}}}}-Marker ein.

PFLICHTFELD – Redaktionelle Anmerkung (editorial_note):
Jeder Artikel MUSS eine redaktionelle Anmerkung enthalten. Diese ordnet das Ereignis
aus heutiger Sicht ein: Was wurde daraus? Wie bewerten Historiker es heute? Welche
Entwicklungen folgten? Die Anmerkung darf NIEMALS leer sein. Auch bei scheinbar
selbsterklärenden Themen gibt es immer einen relevanten historischen Kontext, eine
spätere Wendung oder eine moderne Neubewertung, die für heutige Leser wertvoll ist.
Umfang: 2–4 Sätze.

Antworte als JSON:
{{
  "headline": "Schlagzeile im 1920er-Stil",
  "subheadline": "Erklärende Unterzeile mit Spiegelstrichen",
  "dateline": "Ortsname, {date_day}. {date_month}.",
  "body_raw": "Der vollständige Artikeltext mit mind. 2–3 [[Vollständiger Name|Anzeige]]-Markern UND einem {{{{Bild:}}}}-Marker",
  "editorial_note": "PFLICHT: Einordnung aus heutiger Sicht (was wurde aus dem Ereignis? 2–4 Sätze, NIEMALS leer lassen)"
}}"""


def _parse_article_json(raw_text, fallback_topic=""):
    """Claude-Antwort als JSON parsen – mit Reparatur-Strategien.

    Claude generiert manchmal ungültiges JSON, z.B. unescapte Anführungszeichen
    in deutschen Texten (typische Guillemets „…" und »…«). Diese Funktion
    versucht mehrere Reparaturen, bevor sie auf Regex-Extraktion zurückfällt.
    """
    # Versuch 1: Direkt parsen
    try:
        return json.loads(raw_text)
    except json.JSONDecodeError:
        pass

    # Versuch 2: Typografische Anführungszeichen escapen
    # „..." und »...« innerhalb von JSON-Strings stören den Parser nicht,
    # aber unescapte ASCII-" innerhalb von Werten schon.
    # Strategie: Finde "body_raw": "..." und fixe innere doppelte Anführungszeichen
    try:
        # Ersetze typische Problemstellen: „ und " die nicht escaped sind
        fixed = raw_text
        # Ersetze echte Anführungszeichen in deutschem Text durch typographische
        # Finde Muster: ,"text" innerhalb von JSON-Werten
        fixed = fixed.replace('„', '\u201E').replace('"', '\u201C')
        # Stelle JSON-Strukturzeichen wieder her
        fixed = re.sub(r'\u201E\s*(headline|subheadline|dateline|body_raw|editorial_note)\u201C', r'"\1"', fixed)
        fixed = re.sub(r':\s*\u201E', ': "', fixed)
        fixed = re.sub(r'\u201C\s*([,}])', r'"\1', fixed)
        return json.loads(fixed)
    except (json.JSONDecodeError, Exception):
        pass

    # Versuch 3: Regex-basierte Feld-Extraktion als robuster Fallback
    print(f"    JSON-Repair: Verwende Regex-Extraktion")
    result = {}
    for field in ["headline", "subheadline", "dateline", "body_raw", "editorial_note"]:
        # Suche "field": "value" – greedy bis zum nächsten Feld oder Ende
        pattern = rf'"{field}"\s*:\s*"(.*?)"(?:\s*[,}}]\s*"(?:headline|subheadline|dateline|body_raw|editorial_note)"|\s*}})'
        match = re.search(pattern, raw_text, re.DOTALL)
        if match:
            value = match.group(1)
            # JSON-Escape-Sequenzen auflösen
            value = value.replace('\\n', '\n').replace('\\"', '"').replace('\\\\', '\\')
            result[field] = value

    if result.get("body_raw"):
        return result

    # Versuch 4: Letzte Rettung – gesamten Text als body_raw verwenden
    print(f"    WARNUNG: Auch Regex-Extraktion fehlgeschlagen, verwende Rohtext")
    return {
        "headline": fallback_topic,
        "body_raw": raw_text,
    }



def format_context_for_prompt(context_hits, max_chars=15000):
    """Formatiert Kontext-Treffer als lesbares Quellenmaterial für den Prompt.

    Quellenvielfalt: Pro Quelle wird mindestens der beste Treffer einbezogen,
    bevor das Budget mit weiteren Treffern aufgefüllt wird.
    """
    if not context_hits:
        return "(Kein spezifisches Quellenmaterial verfügbar)"

    # Phase 1: Pro Quelle den ersten (= besten) Treffer reservieren
    seen_sources = set()
    phase1 = []
    phase2 = []
    for hit in context_hits:
        src = hit.get("source", "Unbekannt")
        if src not in seen_sources:
            seen_sources.add(src)
            phase1.append(hit)
        else:
            phase2.append(hit)

    sections = []
    total = 0
    for hit in phase1 + phase2:
        source = hit.get("source", "Unbekannt")
        text = hit.get("text", "").strip()
        if not text:
            continue
        entry = f"[{source}]\n{text}"
        if total + len(entry) > max_chars:
            if hit in phase1:
                continue  # Skip statt break für Phase-1-Treffer
            break
        sections.append(entry)
        total += len(entry)

    return "\n\n---\n\n".join(sections) if sections else "(Kein Quellenmaterial)"


def generate_articles(article_plan, contexts):
    """Stufe 3: Artikel per Claude API im 1920er-Stil generieren."""
    from anthropic import Anthropic

    print("\n" + "="*60)
    print("STUFE 3: ARTIKELGENERIERUNG (Claude API)")
    print("="*60)

    client = Anthropic(api_key=ANTHROPIC_API_KEY)
    date_str = article_plan["date"]
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    months_de = ["", "Januar", "Februar", "März", "April", "Mai", "Juni",
                 "Juli", "August", "September", "Oktober", "November", "Dezember"]

    articles = []

    for i, plan in enumerate(article_plan["articles"]):
        topic = plan["topic"]
        article_type = plan["type"]
        target_words = plan["target_words"]

        print(f"\n  [{i+1}/{len(article_plan['articles'])}] {article_type}: {topic}")
        print(f"    Ziel: {target_words} Wörter")

        # Kontext-Treffer für dieses Thema
        context_hits = contexts.get(topic, [])
        context_material = format_context_for_prompt(context_hits)
        print(f"    Kontext: {len(context_hits)} Treffer, {len(context_material):,} Zeichen")

        user_prompt = ARTICLE_USER_PROMPT.format(
            date=date_str,
            date_upper=date_str.upper(),
            topic=topic,
            article_type=article_type,
            target_words=target_words,
            category=plan.get("category", "Vermischtes"),
            headline_suggestion=plan.get("headline_suggestion", topic),
            context_material=context_material,
            date_day=dt.day,
            date_month=months_de[dt.month],
        )

        try:
            response = client.messages.create(
                model="claude-sonnet-4-20250514",
                max_tokens=4096,
                system=ARTICLE_SYSTEM_PROMPT,
                messages=[{"role": "user", "content": user_prompt}],
            )

            raw_text = response.content[0].text.strip()

            # JSON extrahieren (falls in Code-Block)
            json_match = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", raw_text, re.DOTALL)
            if json_match:
                raw_text = json_match.group(1)

            article_data = _parse_article_json(raw_text, topic)

            articles.append({
                "topic": topic,
                "type": article_type,
                "category": plan.get("category", "Vermischtes"),
                "target_words": target_words,
                "score": plan.get("score", 0),
                "headline": article_data.get("headline", topic),
                "subheadline": article_data.get("subheadline", ""),
                "dateline": article_data.get("dateline", ""),
                "body_raw": article_data.get("body_raw", ""),
                "editorial_note": article_data.get("editorial_note", ""),
            })

            word_count = len(article_data.get("body_raw", "").split())
            editorial = article_data.get("editorial_note", "").strip()
            print(f"    ✓ {article_data.get('headline', '?')}: {word_count} Wörter")

            # ── Nachvalidierung: Fehlende editorial_note nachgenerieren ──
            if not editorial:
                print(f"    ⚠ editorial_note leer – starte Nachgenerierung...")
                try:
                    note_prompt = (
                        f"Du bist Historiker und Redakteur des Projekts 'Vor 100 Jahren'.\n\n"
                        f"Folgender Zeitungsartikel vom {date_str} wurde generiert:\n\n"
                        f"Überschrift: {article_data.get('headline', '')}\n"
                        f"Unterzeile: {article_data.get('subheadline', '')}\n"
                        f"Artikeltext (Auszug, erste 500 Zeichen): {article_data.get('body_raw', '')[:500]}\n\n"
                        f"Schreibe eine redaktionelle Anmerkung (editorial_note) für heutige Leser:\n"
                        f"- Was wurde aus dem beschriebenen Ereignis?\n"
                        f"- Wie bewerten Historiker es heute?\n"
                        f"- Welche Entwicklungen folgten?\n"
                        f"Umfang: 2–4 Sätze. Antworte NUR mit dem Text der Anmerkung, ohne JSON oder Anführungszeichen."
                    )
                    note_response = client.messages.create(
                        model="claude-sonnet-4-20250514",
                        max_tokens=512,
                        messages=[{"role": "user", "content": note_prompt}],
                    )
                    generated_note = note_response.content[0].text.strip()
                    # Sicherheitscheck: Muss mindestens 20 Zeichen lang sein
                    if len(generated_note) >= 20:
                        articles[-1]["editorial_note"] = generated_note
                        print(f"    ✓ editorial_note nachgeneriert ({len(generated_note)} Zeichen)")
                    else:
                        print(f"    ✗ Nachgenerierung zu kurz ({len(generated_note)} Zeichen), übersprungen")
                except Exception as note_err:
                    print(f"    ✗ Nachgenerierung fehlgeschlagen: {note_err}")

        except Exception as e:
            print(f"    FEHLER bei Artikelgenerierung: {e}")
            articles.append({
                "topic": topic,
                "type": article_type,
                "category": plan.get("category", "Vermischtes"),
                "target_words": target_words,
                "score": plan.get("score", 0),
                "headline": topic,
                "subheadline": "",
                "dateline": "",
                "body_raw": f"[Artikelgenerierung fehlgeschlagen: {e}]",
                "editorial_note": "",
            })

        # Rate-Limiting zwischen API-Aufrufen
        if i < len(article_plan["articles"]) - 1:
            time.sleep(2)

    print(f"\n  {len(articles)} Artikel generiert")
    return articles

# ============ STUFE 4: ANREICHERUNG ============

# ── Wikipedia-Link-Validierung ──────────────────────────────
# Cache für Wikipedia-API-Ergebnisse (vermeidet Mehrfach-Abfragen
# desselben Lemmas innerhalb eines Pipeline-Laufs).
_wiki_cache = {}

# User-Agent gemäß Wikimedia-Policy (https://w.wiki/4wJS)
_WIKI_HEADERS = {
    "User-Agent": "Vor100Jahren-Bot/1.0 (historical newspaper project; Python/requests)"
}


def _resolve_disambiguation(title, lang="de"):
    """Versucht bei einer Begriffsklärungsseite den passendsten spezifischen Artikel zu finden.

    Nutzt die Wikipedia-Suche, um nach dem spezifischsten Treffer zu suchen.
    Gibt (resolved_title, url) zurück oder None.
    """
    api_url = f"https://{lang}.wikipedia.org/w/api.php"
    params = {
        "action": "query",
        "list": "search",
        "srsearch": title,
        "srlimit": 5,
        "format": "json",
        "formatversion": 2,
    }
    try:
        r = requests.get(api_url, params=params, headers=_WIKI_HEADERS, timeout=10)
        if r.status_code != 200:
            return None
        data = r.json()
        results = data.get("query", {}).get("search", [])
        title_lower = title.lower()
        for result in results:
            result_title = result.get("title", "")
            # Überspringe die Begriffsklärungsseite selbst
            if result_title.lower() == title_lower:
                continue
            # Der Titel muss den Suchbegriff enthalten (z.B. "Rifkrieg (1921–1926)" enthält "Rifkrieg")
            if title_lower in result_title.lower():
                # Validiere, dass es kein weiterer Disambig ist
                check_params = {
                    "action": "query",
                    "titles": result_title,
                    "prop": "pageprops",
                    "ppprop": "disambiguation",
                    "format": "json",
                    "formatversion": 2,
                }
                r2 = requests.get(api_url, params=check_params, headers=_WIKI_HEADERS, timeout=10)
                if r2.status_code == 200:
                    pages = r2.json().get("query", {}).get("pages", [])
                    if pages and not pages[0].get("missing", False):
                        if "disambiguation" not in pages[0].get("pageprops", {}):
                            resolved = pages[0]["title"]
                            url = f"https://{lang}.wikipedia.org/wiki/{resolved.replace(' ', '_')}"
                            print(f"    Wiki-Link: Begriffsklärung '{title}' → spezifischer Artikel '{resolved}'")
                            return (resolved, url)
    except Exception:
        pass
    print(f"    Wiki-Link ABGELEHNT: '{title}' – kein passender spezifischer Artikel gefunden")
    return None


def _validate_wikipedia_title(title, lang="de", resolve_disambig=False):
    """Prüft via Wikipedia-API, ob ein Artikel existiert.

    resolve_disambig: Wenn True, wird bei Begriffsklärungsseiten versucht,
    den passendsten spezifischen Artikel zu finden (nur für retroaktive
    Anreicherung sinnvoll, nicht für normale Wiki-Resolution).

    Gibt bei Erfolg (resolved_title, url) zurück.
    Die API folgt automatisch Redirects, d.h. 'Tientsin' wird
    zum kanonischen Titel 'Tianjin' aufgelöst.

    Begriffsklärungsseiten (Disambiguation) werden abgelehnt,
    da sie keine sinnvollen Zielseiten für Links darstellen.

    Bei Misserfolg wird None zurückgegeben.
    """
    cache_key = f"{lang}:{title}"
    if cache_key in _wiki_cache:
        return _wiki_cache[cache_key]

    api_url = f"https://{lang}.wikipedia.org/w/api.php"
    params = {
        "action": "query",
        "titles": title,
        "redirects": 1,
        "prop": "pageprops|categories",
        "ppprop": "disambiguation",
        "cllimit": "10",
        "format": "json",
        "formatversion": 2,
    }
    try:
        r = requests.get(api_url, params=params, headers=_WIKI_HEADERS, timeout=10)
        if r.status_code == 200:
            data = r.json()
            pages = data.get("query", {}).get("pages", [])
            if pages and not pages[0].get("missing", False):
                # Begriffsklärungsseiten erkennen und ablehnen
                pageprops = pages[0].get("pageprops", {})
                if "disambiguation" in pageprops:
                    if resolve_disambig:
                        print(f"    Wiki-Link: '{title}' ist Begriffsklärungsseite – suche spezifischen Artikel...")
                        specific = _resolve_disambiguation(title, lang)
                        if specific:
                            _wiki_cache[cache_key] = specific
                            return specific
                    print(f"    Wiki-Link ABGELEHNT: '{title}' ist Begriffsklärungsseite")
                    _wiki_cache[cache_key] = None
                    return None
                # Familienname/Vorname-Seiten erkennen und ablehnen
                # (z.B. "Lehmann" → Kategorie:Familienname, kein sinnvolles Linkziel)
                categories = pages[0].get("categories", [])
                cat_titles = {c.get("title", "") for c in categories}
                disam_categories = {
                    "Kategorie:Begriffsklärung",
                    "Kategorie:Familienname",
                    "Kategorie:Deutscher Personenname",
                    "Kategorie:Vorname",
                    "Kategorie:Männlicher Vorname",
                    "Kategorie:Weiblicher Vorname",
                    "Kategorie:Schweizer Familienname",
                    "Category:Disambiguation pages",  # en.wikipedia
                    "Category:Surnames",
                    "Category:Given names",
                }
                matched_cats = cat_titles & disam_categories
                if matched_cats:
                    cat_name = next(iter(matched_cats))
                    print(f"    Wiki-Link ABGELEHNT: '{title}' ist {cat_name}")
                    _wiki_cache[cache_key] = None
                    return None
                resolved = pages[0]["title"]
                url = f"https://{lang}.wikipedia.org/wiki/{resolved.replace(' ', '_')}"
                result = (resolved, url)
                _wiki_cache[cache_key] = result
                return result
    except Exception:
        pass

    _wiki_cache[cache_key] = None
    return None


def _names_are_consistent(display_text, resolved_title):
    """Prüft, ob Anzeigename und aufgelöstes Wikipedia-Linkziel zusammenpassen.

    Verhindert semantische Fehlzuordnungen wie:
      'Dr. Ramek' → 'Ignaz Seipel'   (falscher Nachname)
      'Frank Belt' → 'Frank-Walter Steinmeier' (falsche Person)

    Strategie:
      - Ignoriere Titel/Anreden (Dr., Mr., Prof., etc.) und Klammerzusätze
      - Extrahiere signifikante Wörter (≥4 Zeichen) aus dem Anzeigenamen
      - Das LÄNGSTE signifikante Wort muss im aufgelösten Titel vorkommen
        (verhindert Matches auf generische Vornamen wie 'Frank')
      - Zusätzlich: Unicode-Normalisierung für diakritische Zeichen
        (Benesch ↔ Beneš, Müller ↔ Mueller etc.)
      - Klammerzusätze im Anzeigenamen werden VOR dem Entfernen
        ebenfalls geprüft (für '(Zhang Zuolin)'-Muster)

    Gibt True zurück, wenn die Namen konsistent sind.
    """
    import re as _re
    import unicodedata

    def _strip_diacritics(s):
        """Entfernt diakritische Zeichen: Beneš → Benes, Masaryk → Masaryk."""
        nfkd = unicodedata.normalize('NFKD', s)
        return ''.join(c for c in nfkd if not unicodedata.combining(c))

    def _transliteration_normalize(s):
        """Normalisiert gängige Transliterationen für Vergleiche.

        Behandelt u.a. deutsche Transliterationen slawischer Namen:
          Benesch → Benes (via sch→s), Tschechisch → Tsechis (via tsch→ts)
        """
        s = _strip_diacritics(s)
        # Reihenfolge wichtig: 'tsch' vor 'sch' ersetzen
        s = s.replace('tsch', 'ts').replace('sch', 's')
        return s

    # 1. Prüfe zunächst, ob Klammerzusatz im Anzeigenamen das Linkziel enthält
    #    z.B. 'Tschangtsolin (Zhang Zuolin)' → resolved 'Zhang Zuolin'
    paren_match = _re.search(r'\(([^)]+)\)', display_text)
    if paren_match:
        paren_content = paren_match.group(1).lower().strip()
        resolved_lower_raw = resolved_title.lower().replace('_', ' ')
        if paren_content in resolved_lower_raw or resolved_lower_raw in paren_content:
            return True

    # 2. Hauptvergleich mit bereinigten Namen
    display_clean = _re.sub(r'\([^)]*\)', '', display_text).strip()
    resolved_clean = _re.sub(r'\([^)]*\)', '', resolved_title).strip()

    # Titel/Anreden und Partikel
    _titles = {'dr', 'prof', 'mr', 'mrs', 'ms', 'sir', 'lord', 'graf',
               'freiherr', 'baron', 'fürst', 'prinz', 'herzog', 'von', 'van',
               'de', 'del', 'der', 'den', 'het', 'le', 'la', 'el'}

    def _significant_words(name):
        words = _re.split(r'[\s.\-_,]+', name.lower())
        return [w for w in words if len(w) >= 3 and w not in _titles]

    display_words = _significant_words(display_clean)
    resolved_words = _significant_words(resolved_clean)

    if not display_words:
        # Nur Titel/kurze Partikel → kein sinnvoller Vergleich, durchlassen
        return True

    # Verwende das LETZTE signifikante Wort (= vermutlich Nachname)
    # statt des längsten, um generische Vornamen ('Frank') zu vermeiden
    key_display = display_words[-1]

    # Normalisierte Versionen für Transliterations-Vergleich
    resolved_lower = resolved_clean.lower().replace('_', ' ').replace('-', ' ')
    resolved_normalized = _transliteration_normalize(resolved_lower)
    key_normalized = _transliteration_normalize(key_display)

    # Prüfe ob das Schlüsselwort im Linkziel vorkommt (original oder normalisiert)
    if key_display in resolved_lower:
        return True
    if key_normalized in resolved_normalized:
        return True

    # Umgekehrt: letztes Wort des Linkziels im Anzeigenamen?
    if resolved_words:
        key_resolved = resolved_words[-1]
        display_lower = display_clean.lower().replace('_', ' ').replace('-', ' ')
        display_normalized = _transliteration_normalize(display_lower)
        key_res_normalized = _transliteration_normalize(key_resolved)

        if key_resolved in display_lower:
            return True
        if key_res_normalized in display_normalized:
            return True

    # 3. Wortstamm-Vergleich: gemeinsamer Präfix >= 3 Zeichen
    #    z.B. "Rif-Aufstandes" ↔ "Rifkrieg (1921–1926)" → gemeinsamer Stamm "rif"
    for dw in display_words:
        for rw in resolved_words:
            # Gemeinsamer Präfix
            prefix_len = 0
            dw_norm = _transliteration_normalize(dw)
            rw_norm = _transliteration_normalize(rw)
            for a, b in zip(dw_norm, rw_norm):
                if a == b:
                    prefix_len += 1
                else:
                    break
            if prefix_len >= 3:
                return True

    return False


def _is_relevant_match(search_term, candidate_title):
    """Prüft, ob ein OpenSearch-Treffer tatsächlich zum Suchbegriff passt.

    Verhindert False Positives wie 'Li Fengchow' → 'Li Feng (Cao Wei)',
    'Paul Cuomo' → 'Paul Cook' oder 'Gustav Sollmann' → 'Gustav Köllmann'.

    Strategie: Das LÄNGSTE signifikante Wort (≥4 Zeichen) des Suchbegriffs
    muss im Ergebnis-Titel vorkommen. So werden Matches basierend auf
    generischen Vornamen (Paul, Gustav) oder Partikeln (Van, von) verhindert.
    """
    # Teilstring-Match: 'Kuominchun' → 'Kuominchun-Clique' usw.
    if search_term.lower() in candidate_title.lower():
        return True
    if candidate_title.lower() in search_term.lower():
        return True

    # Wortbasierter Match: Das längste Wort des Suchbegriffs muss im Ergebnis
    # enthalten sein. Filtert kurze generische Wörter (Paul, Van, Li) aus.
    search_words = sorted(
        [w.lower() for w in search_term.split() if len(w) >= 4],
        key=len, reverse=True
    )
    if not search_words:
        # Nur kurze Wörter (z.B. "Li") – exakte Wort-Übereinstimmung nötig
        search_words_all = {w.lower() for w in search_term.split() if len(w) >= 2}
        candidate_words_all = {w.lower().strip("(),-") for w in candidate_title.split() if len(w) >= 2}
        return len(search_words_all & candidate_words_all) == len(search_words_all)

    longest = search_words[0]
    candidate_lower = candidate_title.lower()
    candidate_words = {w.lower().strip("(),-") for w in candidate_title.split()}

    # Exakter Wort-Match oder Teilstring-Match für das längste Wort
    if longest in candidate_words:
        return True
    if longest in candidate_lower:
        return True

    return False


def _search_wikipedia(search_term, lang="de"):
    """Fallback: Durchsucht Wikipedia per OpenSearch nach dem besten Treffer.

    Nützlich wenn der exakte Titel nicht passt, aber ein verwandter
    Artikel existiert (z.B. 'Nickel Plate Railroad' →
    'New York, Chicago and St. Louis Railroad').

    Verwendet eine Relevanzprüfung, um falsche Treffer zu vermeiden.
    """
    cache_key = f"search:{lang}:{search_term}"
    if cache_key in _wiki_cache:
        return _wiki_cache[cache_key]

    api_url = f"https://{lang}.wikipedia.org/w/api.php"
    params = {
        "action": "opensearch",
        "search": search_term,
        "limit": 5,
        "namespace": 0,
        "format": "json",
    }
    try:
        r = requests.get(api_url, params=params, headers=_WIKI_HEADERS, timeout=10)
        if r.status_code == 200:
            data = r.json()
            # OpenSearch gibt [term, [titles], [descriptions], [urls]] zurück
            if len(data) >= 4 and data[1]:
                # Prüfe alle Treffer auf Relevanz, nimm den ersten passenden
                for i, candidate in enumerate(data[1]):
                    if _is_relevant_match(search_term, candidate):
                        resolved = candidate
                        url = data[3][i]
                        result = (resolved, url)
                        _wiki_cache[cache_key] = result
                        return result
    except Exception:
        pass

    _wiki_cache[cache_key] = None
    return None


def validate_wikipedia_link(link_target, resolve_disambig=False):
    """Validiert einen Wikipedia-Linktitel mit Fallback-Kaskade.

    resolve_disambig: Wenn True, wird bei Begriffsklärungsseiten versucht,
    den passendsten spezifischen Artikel zu finden (nur für retroaktive
    Anreicherung verwenden).

    Kaskade:
      1. Exakter Titel auf de.wikipedia.org (inkl. Redirects)
      2. OpenSearch auf de.wikipedia.org
      3. Exakter Titel auf en.wikipedia.org (inkl. Redirects)
      4. OpenSearch auf en.wikipedia.org
      5. Kein Link (None) – Anzeigetext bleibt erhalten, aber ohne Hyperlink

    Gibt (resolved_title, url) oder None zurück.
    """
    # 1. Exakt auf de.wikipedia
    result = _validate_wikipedia_title(link_target, lang="de", resolve_disambig=resolve_disambig)
    if result:
        return result

    # 2. Suche auf de.wikipedia
    result = _search_wikipedia(link_target, lang="de")
    if result:
        return result

    # 3. Exakt auf en.wikipedia
    result = _validate_wikipedia_title(link_target, lang="en", resolve_disambig=resolve_disambig)
    if result:
        return result

    # 4. Suche auf en.wikipedia
    result = _search_wikipedia(link_target, lang="en")
    if result:
        return result

    # 5. Nichts gefunden
    return None


_EXCLUDED_LINK_TARGETS = None

def _get_excluded_link_targets():
    """Gibt das Set der ausgeschlossenen Link-Ziele zurück (Städte, Länder, Zeitungen).

    Wird beim ersten Aufruf initialisiert und danach gecacht.
    Alle Einträge werden kleingeschrieben gespeichert für case-insensitive Vergleich.
    """
    global _EXCLUDED_LINK_TARGETS
    if _EXCLUDED_LINK_TARGETS is not None:
        return _EXCLUDED_LINK_TARGETS

    cities = {
        # Hauptstädte und Großstädte
        "berlin", "paris", "wien", "genf", "london", "rom", "moskau", "washington",
        "peking", "beijing", "tientsin", "tianjin", "new york", "tokio", "tokyo",
        "madrid", "brüssel", "prag", "warschau", "budapest", "bukarest", "belgrad",
        "athen", "ankara", "kairo", "genua", "münchen", "hamburg", "köln", "dresden",
        "frankfurt", "frankfurt am main", "leipzig", "breslau", "danzig", "königsberg",
        "stettin", "straßburg", "strassburg", "marseille", "lyon", "mailand", "turin",
        "amsterdam", "den haag", "haag", "kopenhagen", "stockholm", "oslo",
        "helsinki", "riga", "kaunas", "krakau", "lemberg", "shanghai", "kanton",
        "guangzhou", "nanking", "nanjing", "locarno", "lausanne", "bern", "zürich",
        "venedig", "florenz", "neapel", "barcelona", "lissabon", "konstantinopel",
        "istanbul", "baghdad", "bagdad", "jerusalem", "damaskus", "bombay",
        "mumbai", "kalkutta", "kolkata", "delhi", "singapur", "sydney",
        "melbourne", "ottawa", "montreal", "buenos aires", "rio de janeiro",
        "mexiko-stadt", "havanna", "johannesburg", "kapstadt", "algier",
        "st. petersburg", "leningrad", "petrograd", "tiflis", "tbilissi",
        # Deutsche Städte
        "stuttgart", "potsdam", "düsseldorf", "dortmund", "essen", "magdeburg",
        "mannheim", "ludwigshafen", "ludwigshafen am rhein", "heidelberg",
        "darmstadt", "wuppertal", "elberfeld", "gelsenkirchen", "osterode am harz",
        "erfurt", "jena", "weimar", "gotha", "arnstadt", "sonneberg", "füssen",
        "innsbruck", "st. gallen", "ascona",
        # Berliner Stadtteile
        "charlottenburg", "schöneberg", "kreuzberg", "friedrichshain", "wedding",
        "zehlendorf", "treptow", "neukölln", "steglitz", "tempelhof", "spandau",
        "reinickendorf", "pankow", "lichtenberg", "moabit", "prenzlauer berg",
        # Internationale Städte
        "chicago", "cleveland", "philadelphia", "jersey city", "riverdale",
        "sofia", "rotterdam", "heerlen", "newcastle", "yokohama",
        "chalon-sur-saône", "mantua", "versailles",
        "donostia / san sebastián", "san sebastián", "timișoara",
        # Chinesische Städte und Orte
        "harbin", "mukden", "baoding", "qingdao", "shanhaiguan", "zhangjiakou",
        "dagu", "taku", "machang",
        # Geographische Orte und Landmarken
        "wall street", "nankou-pass", "pei-ho", "brennerpass", "oberetschtal",
        "nordpol", "südpol", "santorin", "pyramiden", "sakkara",
        "gelber fluss", "seine", "tiber", "new york avenue",
        "katowice", "chorzów",
    }

    countries = {
        # Staaten
        "deutschland", "frankreich", "england", "großbritannien", "vereinigtes königreich",
        "sowjetunion", "russland", "china", "japan", "vereinigte staaten", "usa",
        "italien", "spanien", "portugal", "niederlande", "belgien", "schweiz",
        "österreich", "ungarn", "tschechoslowakei", "polen", "rumänien",
        "jugoslawien", "bulgarien", "griechenland", "türkei", "ägypten",
        "indien", "kanada", "australien", "brasilien", "argentinien", "mexiko",
        "schweden", "norwegen", "dänemark", "finnland", "estland", "lettland",
        "litauen", "irland", "schottland", "wales", "preußen", "bayern",
        "sachsen", "württemberg", "baden", "persien", "iran", "siam", "thailand",
        "abessinien", "äthiopien", "südafrika", "kolumbien", "chile", "peru",
        "bolivien", "venezuela", "kuba", "deutsches reich", "weimarer republik",
        "mongolei", "ontario", "michigan", "nebraska", "texas", "utah",
        # Regionen und Provinzen
        "thüringen", "hessen", "rheinland", "rheinprovinz", "saargebiet",
        "oberschlesien", "südtirol", "tirol", "böhmen", "mähren", "slowakei",
        "siebenbürgen", "vojvodina", "mandschurei", "ostasien",
        "hebei", "henan", "shaanxi", "shandong", "jehol",
        "baden (land)",
    }

    newspapers = {
        "vossische zeitung", "berliner tageblatt", "vorwärts", "lokal-anzeiger",
        "berliner lokal-anzeiger", "deutsche allgemeine zeitung",
        "kölnische zeitung", "frankfurter zeitung", "hamburger echo",
        "le temps", "le figaro", "figaro", "le matin", "l'echo de paris",
        "echo de paris", "le journal", "l'humanité", "the times",
        "daily mail", "daily telegraph", "manchester guardian",
        "new york times", "new york herald", "washington post",
        "chicago tribune", "pravda", "izvestija", "iswestija", "prawda",
        "neue freie presse", "wiener zeitung", "neue zürcher zeitung",
        "corriere della sera", "temps", "times", "tribune",
        "badische presse", "sächsische staatszeitung", "harburger tageblatt",
        "westfälischer merkur", "reichsanzeiger", "deutscher reichsanzeiger",
        "hamburger fremdenblatt", "münchner neueste nachrichten",
        "germania", "kreuzzeitung", "tägliche rundschau",
        "rote fahne", "volksstimme",
        "popolo d'italia", "the baltimore sun", "the north china herald",
        "washington times",
    }

    _EXCLUDED_LINK_TARGETS = cities | countries | newspapers
    return _EXCLUDED_LINK_TARGETS


def _is_excluded_link(link_target, display_text):
    """Prüft, ob ein Link-Ziel eine Stadt, ein Land oder eine Zeitung ist."""
    excluded = _get_excluded_link_targets()
    target_lower = link_target.lower().replace("_", " ")
    display_lower = display_text.lower().replace("_", " ")
    return target_lower in excluded or display_lower in excluded


def resolve_wikipedia_links(text):
    """[[Begriff]] Markierungen zu Wikipedia-URLs auflösen.

    Unterstützt drei Notationen:
      [[Begriff]]          -> Link auf 'Begriff', Anzeige 'Begriff'
      [[Ziel|Anzeige]]     -> Link auf 'Ziel', Anzeige 'Anzeige'
      [[Begriff]]suffix    -> Link auf 'Begriff', Anzeige 'Begriffsuffix'

    Die dritte Form fängt Komposita ab: [[Völkerbund]]es -> 'Völkerbundes'
    Die bevorzugte Form ist [[Ziel|Anzeige]], z.B. [[Völkerbund|Völkerbundes]].
    Die Suffix-Erkennung dient als Fallback.

    Jeder Link wird über die Wikipedia-API validiert. Nicht-existierende
    Seiten werden als reiner Text (ohne Hyperlink) ausgegeben.
    Städte, Länder und Zeitungsnamen werden als reiner Text ausgegeben.
    """
    links_found = []
    validated_count = 0
    removed_count = 0
    fallback_count = 0
    excluded_count = 0

    def replace_link(match):
        nonlocal validated_count, removed_count, fallback_count, excluded_count
        full_match = match.group(1)
        suffix = match.group(2) or ""  # Buchstaben direkt nach ]]

        # [[Linkziel|Anzeigename]] oder [[Begriff]]
        if '|' in full_match:
            link_target, display_text = full_match.split('|', 1)
        else:
            link_target = full_match
            display_text = full_match

        # Suffix an Anzeigetext anhängen (für Komposita-Fallback)
        display_text = display_text + suffix

        # Ausschluss-Filter: Städte, Länder, Zeitungen → kein Link
        if _is_excluded_link(link_target, display_text):
            excluded_count += 1
            print(f"    Wiki-Link AUSGESCHLOSSEN: '{link_target}' (Stadt/Land/Zeitung)")
            return display_text

        # Wikipedia-Link validieren
        validation = validate_wikipedia_link(link_target)

        if validation:
            resolved_title, wiki_url = validation

            # Nachnamen-Konsistenzprüfung: Anzeigename ↔ aufgelöstes Linkziel
            if not _names_are_consistent(display_text, resolved_title):
                removed_count += 1
                print(f"    Wiki-Link ENTFERNT: '{display_text}' → '{resolved_title}' "
                      f"(Namens-Inkonsistenz: Anzeige passt nicht zum Linkziel)")
                return display_text

            # Prüfen ob Fallback auf en.wikipedia nötig war
            if "en.wikipedia.org" in wiki_url:
                fallback_count += 1
                print(f"    Wiki-Link: '{link_target}' → en: '{resolved_title}'")
            elif resolved_title.replace('_', ' ') != link_target:
                print(f"    Wiki-Link: '{link_target}' → de: '{resolved_title}' (Redirect)")
            validated_count += 1

            links_found.append({
                "term": display_text,
                "wikipedia_url": wiki_url,
                "link_target": resolved_title
            })

            return f'<a href="{wiki_url}">{display_text}</a>'
        else:
            # Kein Wikipedia-Artikel gefunden → reiner Text ohne Link
            removed_count += 1
            print(f"    Wiki-Link ENTFERNT: '{link_target}' (kein Artikel gefunden)")
            return display_text

    # Regex: [[...]] gefolgt von optionalen Wort-Buchstaben (Suffix)
    html_text = re.sub(r'\[\[([^\]]+)\]\](\w*)', replace_link, text)

    total = validated_count + removed_count + excluded_count
    if total > 0:
        print(f"    Wiki-Validierung: {validated_count}/{total} OK"
              f" ({fallback_count} en-Fallback, {removed_count} entfernt,"
              f" {excluded_count} ausgeschlossen [Stadt/Land/Zeitung])")

    return html_text, links_found

def _search_wikimedia_commons(query, max_results=5):
    """[DEPRECATED – nicht mehr aktiv genutzt, ersetzt durch _fetch_wikipedia_pageimage]
    Sucht auf Wikimedia Commons nach Bildern und gibt validierte Ergebnisse zurück.

    Verwendet die MediaWiki-API (generator=search im File-Namespace),
    prüft die Lizenz und validiert die Thumbnail-URL per HEAD-Request.
    """
    params = {
        "action": "query",
        "format": "json",
        "generator": "search",
        "gsrnamespace": "6",          # File-Namespace
        "gsrsearch": query,
        "gsrlimit": str(max_results),
        "prop": "imageinfo",
        "iiprop": "url|extmetadata|size|mime",
        "iiurlwidth": "800",
    }
    try:
        r = requests.get(
            "https://commons.wikimedia.org/w/api.php",
            params=params,
            headers=_WIKI_HEADERS,
            timeout=15,
        )
        if r.status_code != 200:
            print(f"    ⚠ Wikimedia-API HTTP {r.status_code}")
            return []
        data = r.json()
    except Exception as e:
        print(f"    ⚠ Wikimedia-API-Fehler: {e}")
        return []

    results = []
    pages = data.get("query", {}).get("pages", {})
    for _pid, page in sorted(pages.items(), key=lambda x: x[1].get("index", 999)):
        ii = (page.get("imageinfo") or [{}])[0]
        mime = ii.get("mime", "")
        if not mime.startswith("image/"):
            continue
        # Nur Bilder > 200px Breite akzeptieren
        if ii.get("width", 0) < 200:
            continue

        ext = ii.get("extmetadata", {})
        license_short = ext.get("LicenseShortName", {}).get("value", "")
        # Nur freie Lizenzen akzeptieren
        free_licenses = ("public domain", "pd", "cc0", "cc by", "cc-by",
                         "cc by-sa", "cc-by-sa", "gfdl", "fal")
        if not any(fl in license_short.lower() for fl in free_licenses):
            continue

        thumb_url = ii.get("thumburl", "")
        desc_url = ii.get("descriptionurl", "")
        artist = ext.get("Artist", {}).get("value", "")
        # HTML-Tags aus Artist entfernen
        artist = re.sub(r"<[^>]+>", "", artist).strip()
        description = ext.get("ImageDescription", {}).get("value", "")
        description = re.sub(r"<[^>]+>", "", description).strip()

        results.append({
            "thumb_url": thumb_url,
            "source_url": desc_url,
            "credit": f"{artist} / {license_short}" if artist else license_short,
            "alt_text": description[:200] if description else page.get("title", ""),
        })

    return results


def _validate_image_url(url):
    """[DEPRECATED – nicht mehr aktiv genutzt] Prüft per HEAD-Request, ob eine Bild-URL erreichbar ist (HTTP 200)."""
    try:
        r = requests.head(url, headers=_WIKI_HEADERS, timeout=10, allow_redirects=True)
        return r.status_code == 200
    except Exception:
        return False


def _looks_like_person_name(text):
    """Heuristik: Ist der Text vermutlich ein Personenname?

    Erkennt Muster wie 'Vorname Nachname', 'Émile Vandervelde',
    'Gustav Stresemann'. Mindestens zwei Wörter, jedes kapitalisiert.
    Filtert typische Nicht-Personen-Muster (Zeitungen, Institutionen).
    """
    parts = text.strip().split()
    if len(parts) < 2 or len(parts) > 4:
        return False
    if not all(p[0].isupper() for p in parts if len(p) > 1):
        return False
    # Typische Nicht-Personen-Wörter ausschließen
    non_person_words = {
        "zeitung", "tageblatt", "presse", "anzeiger", "echo", "merkur",
        "courier", "courant", "gazette", "herald", "times", "post",
        "partei", "verein", "bund", "liga", "rat", "gericht", "amt",
        "republik", "reich", "kongress", "konferenz", "vertrag",
        "palast", "theater", "oper", "universität", "akademie",
    }
    for part in parts:
        if part.lower() in non_person_words:
            return False
    return True


# Dateinamen-Wörter, die auf unpassende Bilder hindeuten
_NSFW_FILENAME_WORDS = {
    "vagina", "penis", "vulva", "genitalia", "genital", "anus", "anal",
    "sex", "sexual", "erotic", "erotica", "nude", "nudity", "naked",
    "pornograph", "coitus", "intercourse", "orgasm", "fetish",
    "breast", "nipple", "buttock", "scrotum", "testicle", "phallus",
}


def _is_nsfw_image(candidate):
    """Prüft ob ein Wikimedia-Suchergebnis ein unpassendes Bild ist."""
    check_fields = (
        candidate.get("alt_text", "").lower(),
        candidate.get("thumb_url", "").lower(),
    )
    for text in check_fields:
        for word in _NSFW_FILENAME_WORDS:
            if word in text:
                return True
    return False


def _extract_image_keywords(article):
    """Extrahiert geeignete Bildsuche-Keywords aus dem Artikel.

    Nutzt die bereits aufgelösten Wikipedia-Links und filtert:
    - Städte, Länder, Zeitungen → ausgeschlossen (zu generisch)
    - Personen und distinkte Begriffe → bevorzugt
    - Maximal 3 Keywords, um die Suche fokussiert zu halten

    Gibt eine priorisierte Liste von Suchbegriffen zurück:
    1. Personennamen (aus Wikipedia-Links)
    2. Distinkte Begriffe (Organisationen, Ereignisse, etc.)
    """
    excluded = _get_excluded_link_targets()
    person_keywords = []
    other_keywords = []

    wiki_links = article.get("wikipedia_links", [])
    for link in wiki_links:
        term = link.get("term", "").strip()
        target = link.get("link_target", "").strip()
        # Ausschluss: Städte, Länder, Zeitungen
        if term.lower() in excluded or target.lower() in excluded:
            continue
        keyword = target if target else term
        # Personen priorisieren (haben typischerweise bessere Porträtbilder)
        if _looks_like_person_name(keyword):
            person_keywords.append(keyword)
        else:
            other_keywords.append(keyword)

    # Personen zuerst, dann andere – maximal 3 insgesamt
    return (person_keywords + other_keywords)[:3]


def _download_and_localize_image(wikimedia_url, alt_text=""):
    """Lädt ein Wikimedia-Bild herunter und speichert es lokal unter images/.

    Deduplizierung: Wenn das Bild (identische URL) bereits lokal existiert,
    wird der vorhandene Pfad zurückgegeben. So können verschiedene Ausgaben
    dasselbe Bild referenzieren, ohne es mehrfach zu speichern.

    Returns:
        str: Lokaler Pfad relativ zum Repo-Root (z.B. "images/Friedrich_Ebert.jpg"),
             oder die Original-URL bei Fehler.
    """
    import urllib.parse as _up
    import hashlib as _hl

    os.makedirs(IMAGES_DIR, exist_ok=True)

    # Mapping-Datei für URL→lokaler-Dateiname (persistente Deduplizierung)
    mapping_path = os.path.join(os.path.dirname(IMAGES_DIR), "image_mapping.json")
    if os.path.exists(mapping_path):
        with open(mapping_path, "r", encoding="utf-8") as f:
            mapping = json.load(f)
    else:
        mapping = {}

    # Bereits bekannt?
    if wikimedia_url in mapping:
        local_path = mapping[wikimedia_url]["local_path"]
        full_path = os.path.join(os.path.dirname(IMAGES_DIR), local_path)
        if os.path.exists(full_path) and os.path.getsize(full_path) > 100:
            return local_path
        # Datei fehlt trotz Mapping → neu herunterladen

    # Dateinamen aus alt_text ableiten
    name = alt_text.strip() if alt_text else ""
    if not name:
        # Fallback: aus URL extrahieren
        path = _up.unquote(_up.urlparse(wikimedia_url).path)
        name = path.split("/")[-1]

    # Extension aus URL ermitteln
    url_path = _up.unquote(_up.urlparse(wikimedia_url).path)
    ext_match = re.search(r"\.(jpg|jpeg|png|svg|gif)(?:\.|$)", url_path, re.IGNORECASE)
    ext = f".{ext_match.group(1).lower()}" if ext_match else ".jpg"
    if ext == ".svg" and url_path.endswith(".png"):
        ext = ".png"

    # Dateinamen sanitisieren
    clean_name = re.sub(r"[^\w\-. ()]", "", name)
    clean_name = clean_name.replace(" ", "_")
    clean_name = re.sub(r"_+", "_", clean_name)[:60].strip("_")
    if not clean_name:
        clean_name = _hl.md5(wikimedia_url.encode()).hexdigest()[:12]

    filename = f"{clean_name}{ext}"

    # Kollisionsvermeidung
    existing_files = {f.lower() for f in os.listdir(IMAGES_DIR)} if os.path.isdir(IMAGES_DIR) else set()
    if filename.lower() in existing_files:
        # Prüfen ob es dasselbe Bild ist (gleicher Dateiname → wahrscheinlich ja)
        candidate_path = os.path.join(IMAGES_DIR, filename)
        if os.path.exists(candidate_path) and os.path.getsize(candidate_path) > 100:
            # Existiert bereits mit diesem Namen → verwenden
            local_rel = f"images/{filename}"
            mapping[wikimedia_url] = {
                "local_file": filename,
                "local_path": local_rel,
                "alt": alt_text,
            }
            with open(mapping_path, "w", encoding="utf-8") as f:
                json.dump(mapping, f, ensure_ascii=False, indent=2)
            return local_rel
        # Anderer Inhalt → neuen Namen generieren
        base = clean_name
        c = 2
        while f"{base}_{c}{ext}".lower() in existing_files:
            c += 1
        filename = f"{base}_{c}{ext}"

    local_full = os.path.join(IMAGES_DIR, filename)
    local_rel = f"images/{filename}"

    # Download mit Bot-User-Agent (Wikimedia blockiert generische Browser-UAs per 429)
    bot_ua = "Vor100JahrenBot/1.0 (https://vor100jahren.de; sgresse@web.de) python-requests"
    for attempt in range(3):
        try:
            resp = requests.get(wikimedia_url, headers={"User-Agent": bot_ua}, timeout=30)
            resp.raise_for_status()
            with open(local_full, "wb") as f:
                f.write(resp.content)
            print(f"    📥 Bild lokal gespeichert: {filename} ({len(resp.content)/1024:.0f} KB)")
            break
        except requests.exceptions.HTTPError as e:
            if e.response and e.response.status_code == 429:
                wait = (attempt + 1) * 3
                print(f"    ⏳ Wikimedia 429, warte {wait}s...")
                time.sleep(wait)
            else:
                print(f"    ⚠ Bild-Download fehlgeschlagen ({e}), nutze Original-URL")
                return wikimedia_url
        except Exception as e:
            print(f"    ⚠ Bild-Download fehlgeschlagen ({e}), nutze Original-URL")
            return wikimedia_url
    else:
        print(f"    ⚠ Bild-Download nach 3 Versuchen fehlgeschlagen, nutze Original-URL")
        return wikimedia_url

    # Mapping aktualisieren
    mapping[wikimedia_url] = {
        "local_file": filename,
        "local_path": local_rel,
        "alt": alt_text,
    }
    with open(mapping_path, "w", encoding="utf-8") as f:
        json.dump(mapping, f, ensure_ascii=False, indent=2)

    return local_rel


def _ensure_wikimedia_thumbnail(url, width=800):
    """Konvertiert eine volle Wikimedia-URL in eine Thumbnail-URL.

    Wenn die URL bereits ein Thumbnail ist, wird sie unverändert zurückgegeben.
    Unterstützt sowohl /wikipedia/commons/ als auch /wikipedia/de/ URLs.

    Beispiel:
        .../wikipedia/commons/a/ab/Bild.jpg
        → .../wikipedia/commons/thumb/a/ab/Bild.jpg/800px-Bild.jpg
    """
    if not url or "upload.wikimedia.org" not in url:
        return url
    if "/thumb/" in url:
        return url  # Bereits ein Thumbnail
    # Pattern: /wikipedia/{repo}/{hash1}/{hash2}/Filename.ext
    import re
    m = re.match(
        r"(https://upload\.wikimedia\.org/wikipedia/(?:commons|de|en))/([0-9a-f]/[0-9a-f]{2})/(.+)",
        url,
    )
    if not m:
        return url  # Unbekanntes Format, nicht konvertieren
    base, hash_path, filename = m.group(1), m.group(2), m.group(3)
    return f"{base}/thumb/{hash_path}/{filename}/{width}px-{filename}"


def _fetch_wikipedia_pageimage(title, lang="de"):
    """Holt das Hauptbild und die Kurzbeschreibung eines Wikipedia-Artikels.

    Nutzt die pageimages- und pageterms-API. Die Wikidata-Kurzbeschreibung
    (z.B. "deutscher Politiker (1878–1929)") wird als Bildunterschrift verwendet.

    Gibt bei Erfolg ein Dict zurück:
        {
            "thumb_url": "https://...",
            "source_url": "https://commons.wikimedia.org/wiki/File:...",
            "credit": "Lizenz",
            "alt_text": "Bildbeschreibung",
            "page_title": "Aufgelöster Titel",
            "description": "Kurzbeschreibung aus Wikipedia/Wikidata"
        }
    Bei Misserfolg: None
    """
    api_url = f"https://{lang}.wikipedia.org/w/api.php"
    params = {
        "action": "query",
        "titles": title,
        "redirects": 1,
        "prop": "pageimages|pageprops|pageterms",
        "piprop": "thumbnail|name",
        "pithumbsize": 800,
        "wbptterms": "description",
        "format": "json",
        "formatversion": 2,
    }
    try:
        r = requests.get(api_url, params=params, headers=_WIKI_HEADERS, timeout=10)
        if r.status_code != 200:
            return None
        data = r.json()
        pages = data.get("query", {}).get("pages", [])
        if not pages or pages[0].get("missing", False):
            return None
        page = pages[0]
        thumb = page.get("thumbnail", {})
        if not thumb.get("source"):
            return None
        # Dateiname für Commons-Link
        page_image = page.get("pageimage", "")
        commons_url = f"https://commons.wikimedia.org/wiki/File:{page_image}" if page_image else ""
        # Kurzbeschreibung aus Wikidata (pageterms)
        terms = page.get("terms", {})
        descriptions = terms.get("description", [])
        description = descriptions[0] if descriptions else ""
        return {
            "thumb_url": _ensure_wikimedia_thumbnail(thumb["source"]),
            "source_url": commons_url,
            "credit": "Wikimedia Commons",
            "alt_text": page.get("title", title),
            "page_title": page.get("title", title),
            "description": description,
        }
    except Exception as e:
        print(f"    ⚠ Wikipedia-Bild-API-Fehler ({lang}): {e}")
        return None


def _fetch_image_for_link(link_target, display_term):
    """Versucht das Wikipedia-Hauptbild für einen Link-Target zu holen.

    Kaskade: de.wikipedia → en.wikipedia
    Gibt (image_dict, caption) oder (None, None) zurück.
    """
    # Erst de.wikipedia
    result = _fetch_wikipedia_pageimage(link_target, lang="de")
    if result:
        return result, display_term

    # Dann en.wikipedia
    result = _fetch_wikipedia_pageimage(link_target, lang="en")
    if result:
        return result, display_term

    return None, None


def resolve_image_markers(text, topic, article=None, used_urls=None,
                          headline_targets=None):
    """{{Bild: Beschreibung}} Markierungen auflösen.

    Strategie: Wikipedia-Hauptbilder über die pageimages-API.

    Priorisierung der Bildsuche-Kandidaten:
    1. Personen aus Body-Wikipedia-Links (stabile historische Porträts)
    2. HEADLINE/SUBHEADLINE-Begriffe (thematisch relevante Eigennamen)
    3. Themenbegriff (topic) als direkter Wikipedia-Suchbegriff
    4. Andere Body-Wikipedia-Links (Organisationen, Ereignisse, etc.)

    Personen-Links liefern die zuverlässigsten historischen Bilder und
    werden daher bevorzugt. Headline-Begriffe ergänzen die Suche um
    themenspezifische Begriffe, die im Body nicht verlinkt wurden.

    Prinzipien:
    - Erstes verfügbares, nicht-dupliziertes Bild verwenden.
    - Kein Bild erzwingen – lieber kein Bild als ein unpassendes.
    - Kein Fallback auf Wikimedia Commons Freitextsuche.

    Die Bildunterschrift (caption) wird aus dem Wikipedia-Kontext abgeleitet.
    """
    images = []
    if used_urls is None:
        used_urls = set()
    if headline_targets is None:
        headline_targets = []

    # {{Bild: ...}} Marker extrahieren und entfernen
    clean_text = re.sub(r'\{\{Bild:[^}]+\}\}', '', text).strip()

    if not article:
        return clean_text, images

    # --- Kandidaten-Listen aufbauen (nach Priorität) ---
    excluded = _get_excluded_link_targets()

    # Priorität 1: Headline/Subheadline-Begriffe (direkte Wiki-Marker)
    headline_candidates = []
    seen_targets = set()
    for target, term in headline_targets:
        if target.lower() in excluded or term.lower() in excluded:
            continue
        target_key = target.lower().replace("_", " ")
        if target_key not in seen_targets:
            headline_candidates.append((target, term))
            seen_targets.add(target_key)

    # Priorität 2: Themenbegriff (topic) als Wikipedia-Suchkandidat
    #   Der Topic-String enthält oft den zentralen Begriff des Artikels.
    #   Auch ohne expliziten Wiki-Marker kann er zu einem passenden
    #   Wikipedia-Artikel mit historischem Bild führen.
    topic_candidates = []
    if topic:
        topic_clean = topic.strip()
        topic_key = topic_clean.lower().replace("_", " ")
        if topic_key not in seen_targets and topic_key not in excluded:
            topic_candidates.append((topic_clean, topic_clean))
            seen_targets.add(topic_key)

    # Priorität 3+4+5+6: Body-Wikipedia-Links
    #   Personen werden in drei Gruppen aufgeteilt:
    #   - Hauptüberschrift-Personen: Name kommt in headline vor → höchste Priorität
    #   - Subheadline-Personen: Name nur in subheadline → mittlere Priorität
    #   - Body-Personen: nur im Body erwähnt → niedrigste Priorität (Nebenfiguren)
    #   Dies verhindert, dass Nebenfiguren (z.B. Dawes in einem Prohibition-Artikel)
    #   das Bild dominieren, wenn sie nur am Rande erwähnt werden.
    main_headline = article.get("headline", "").lower()
    sub_headline = article.get("subheadline", "").lower()
    full_headline_text = (main_headline + " " + sub_headline)
    wiki_links = article.get("wikipedia_links", [])
    main_headline_persons = []    # Höchste Prio: In der Hauptüberschrift
    sub_headline_persons = []     # Mittlere Prio: Nur in der Subheadline
    body_person_links = []        # Niedrige Prio: Nur im Body
    other_links = []
    for link in wiki_links:
        term = link.get("term", "").strip()
        target = link.get("link_target", "").strip()
        if not target:
            continue
        target_key = target.lower().replace("_", " ")
        if target_key in seen_targets:
            continue  # Bereits als Headline/Topic-Kandidat
        if target.lower() in excluded or term.lower() in excluded:
            continue
        seen_targets.add(target_key)
        if _looks_like_person_name(target):
            last_name = target.split()[-1].lower() if target.split() else ""
            display_lower = term.lower()
            # Prüfe ZUERST die Hauptüberschrift, DANN die Subheadline
            if (last_name and last_name in main_headline) or display_lower in main_headline:
                main_headline_persons.append((target, term))
            elif (last_name and last_name in sub_headline) or display_lower in sub_headline:
                sub_headline_persons.append((target, term))
            else:
                body_person_links.append((target, term))
        else:
            other_links.append((target, term))

    # Zusammenführen (max. 7 API-Aufrufe):
    #   1. Hauptüberschrift-Personen (zentrale Figur des Artikels)
    #   2. Subheadline-Personen (sekundäre Figuren)
    #   3. Headline-Begriffe (thematische Wiki-Marker aus Headline/Subheadline)
    #   4. Topic-Begriff (Themenbegriff des Artikels)
    #   5. Body-Personen (nur im Fließtext erwähnt → Nebenfiguren)
    #   6. Andere Body-Links (Organisationen, Ereignisse, etc.)
    candidates = (main_headline_persons + sub_headline_persons
                  + headline_candidates + topic_candidates
                  + body_person_links + other_links)[:7]

    if not candidates:
        return clean_text, images

    n_mh = len(main_headline_persons)
    n_sh = len(sub_headline_persons)
    n_hl = len(headline_candidates)
    n_tp = len(topic_candidates)
    n_bp = len(body_person_links)
    n_ot = len(other_links)
    print(f"    🔍 Bildsuche (Wikipedia pageimages): {len(candidates)} Kandidaten"
          f" ({n_mh} HL-Pers., {n_sh} SubHL-Pers., {n_hl} HL-Begr., {n_tp} Topic,"
          f" {n_bp} Body-Pers., {n_ot} andere)")

    for target, term in candidates:
        result, caption = _fetch_image_for_link(target, term)
        if result:
            thumb = result["thumb_url"]
            # Duplikat-Erkennung
            if thumb in used_urls:
                print(f"    – Duplikat übersprungen: {target}")
                continue
            # NSFW-Filter
            if _is_nsfw_image(result):
                print(f"    ✗ NSFW-Bild übersprungen: {target}")
                continue
            # Caption: Wikipedia-Titel + Kurzbeschreibung (z.B. "Gustav Stresemann, deutscher Politiker (1878–1929)")
            wiki_desc = result.get("description", "")
            if wiki_desc:
                full_caption = f"{result['page_title']}, {wiki_desc}"
            else:
                full_caption = result["page_title"]
            print(f"    ✓ Bild via Wikipedia: '{target}' → {thumb[:70]}…")
            print(f"      Caption: {full_caption[:80]}")
            # Bild lokal speichern (vermeidet Wikimedia-Hotlinking / 429-Fehler)
            local_url = _download_and_localize_image(thumb, alt_text=result["alt_text"])
            images.append({
                "url": local_url,
                "credit": result["credit"],
                "source_url": result["source_url"],
                "alt_text": result["alt_text"],
                "caption": full_caption,
            })
            used_urls.add(thumb)
            return clean_text, images
        else:
            print(f"    – Kein Bild für '{target}'")

    print(f"    – Kein Wikipedia-Bild verfügbar")
    return clean_text, images

def build_source_apparatus(context_hits, source_registry):
    """Quellenapparat dynamisch aus Kontext-Treffern erzeugen.
    
    Extrahiert die eindeutigen Quellennamen aus den tatsächlichen
    Keyword-Treffern der Kontextextraktion und schlägt deren
    Metadaten in der Quellen-Registry nach.
    
    So werden nur Quellen zitiert, die wirklich zum Artikel
    beigetragen haben – unabhängig von statischen Listen.
    """
    primary_sources = []
    seen = set()
    
    for hit in context_hits:
        source_name = hit["source"]
        if source_name in seen:
            continue
        seen.add(source_name)
        
        if source_name in source_registry:
            meta = source_registry[source_name]
            primary_sources.append({
                "newspaper": meta["newspaper"],
                "date": TARGET_DATE,
                "url": meta["url"],
                "archive": meta["archive"],
                "pages_cited": "diverse",
                "license": meta["license"]
            })
        else:
            # Unbekannte Quelle – Fallback mit Basisinformationen
            primary_sources.append({
                "newspaper": source_name,
                "date": TARGET_DATE,
                "url": "",
                "archive": "Unbekannt",
                "pages_cited": "diverse",
                "license": "Unbekannt"
            })
    
    return primary_sources

def strip_wiki_markers(text):
    """[[Wiki]]-Marker zu reinem Text auflösen (ohne HTML-Links).

    Für Felder wie headline/subheadline/dateline, wo keine HTML-Links
    erwünscht sind, aber Claude trotzdem Marker einfügt.
    """
    if not text:
        return text
    # [[Ziel|Anzeige]] → Anzeige
    text = re.sub(r'\[\[([^\]|]+)\|([^\]]+)\]\]', r'\2', text)
    # [[Begriff]]suffix → Begriffsuffix
    text = re.sub(r'\[\[([^\]]+)\]\](\w*)', lambda m: m.group(1) + m.group(2), text)
    return text


def _extract_wiki_marker_targets(text):
    """Extrahiert Link-Targets aus [[Wiki]]-Markern, ohne den Text zu verändern.

    Gibt eine Liste von (link_target, display_text)-Tupeln zurück.
    Wird verwendet, um aus Headline/Subheadline Bildsuche-Kandidaten
    zu gewinnen, BEVOR die Marker entfernt werden.
    """
    if not text:
        return []
    targets = []
    for match in re.finditer(r'\[\[([^\]]+)\]\]', text):
        full = match.group(1)
        if '|' in full:
            link_target, display_text = full.split('|', 1)
        else:
            link_target = full
            display_text = full
        targets.append((link_target.strip(), display_text.strip()))
    return targets


def _retroactive_wiki_enrichment(body_text, topic, headline, subheadline):
    """Nachträgliche Wikipedia-Verlinkung für Texte ohne [[Wiki]]-Marker.

    Wird aufgerufen, wenn resolve_wikipedia_links() null Links gefunden hat.
    Sucht eigenständig nach verlinkbaren Begriffen basierend auf:
    1. Topic-String (zentrales Thema des Artikels)
    2. Headline/Subheadline (wichtige Eigennamen und Begriffe)
    3. Markante Mehrwort-Begriffe im Body (Personen, Organisationen)

    Injiziert [[Wiki]]-Marker in den Text und gibt den angereicherten Text zurück.
    """
    if not body_text or not body_text.strip():
        return body_text

    excluded = _get_excluded_link_targets()
    # Generische Wörter, die zwar existierende Wikipedia-Artikel haben,
    # aber als Link-Ziele in historischen Zeitungsartikeln ungeeignet sind
    generic_words = {
        "truppen", "aufstand", "stämme", "soldaten", "armee", "krieg",
        "regierung", "partei", "verhandlungen", "abkommen", "vertrag",
        "general", "minister", "präsident", "behörden", "kommission",
        "operationen", "angriff", "kampf", "offensive", "militär",
        "kolonie", "gebiet", "stadt", "land", "staat", "reich",
        "feindliche", "französische", "deutsche", "britische", "englische",
        "verstärkungen", "schwierigkeiten", "entlegenen", "gebieten",
        "anhaltende", "feindliche", "marokkanischen",
    }
    candidates = []
    seen = set()

    def _add_candidate(search_term, display_term=None):
        """Kandidat hinzufügen, wenn er noch nicht gesehen wurde und nicht ausgeschlossen ist."""
        if not search_term or not search_term.strip():
            return
        key = search_term.lower().strip()
        if key in seen or key in excluded:
            return
        # Einwort-Begriffe auf generische Wörter prüfen
        if " " not in key and key in generic_words:
            return
        seen.add(key)
        candidates.append((search_term.strip(), display_term or search_term.strip()))

    # 1. Topic-String als Kandidat (häufig der wichtigste Begriff)
    if topic:
        _add_candidate(topic)

    # 2. Signifikante Begriffe aus Headline/Subheadline extrahieren
    #    Wir suchen nach Wörtern mit Großbuchstaben (Eigennamen) und
    #    Mehrwort-Phrasen, die Wikipedia-Artikel haben könnten
    for text in (headline, subheadline):
        if not text:
            continue
        # Bereits gestrippte Texte (keine [[]] mehr) – suche Eigennamen
        words = text.split()
        i = 0
        while i < len(words):
            # Mehrwort-Eigennamen erkennen (z.B. "Abd el-Krim", "Rif-Aufstand")
            if words[i][0:1].isupper() and words[i].lower() not in excluded:
                # Versuche 3-Wort, 2-Wort, dann 1-Wort Phrasen
                for length in (3, 2, 1):
                    if i + length <= len(words):
                        phrase = " ".join(words[i:i+length])
                        # Nur Phrasen mit mindestens 4 Zeichen und nicht rein generisch
                        clean = re.sub(r'[.,;:!?\-\(\)"\u201E\u201C\u00BB\u00AB\u2014\u2013\']+', '', phrase).strip()
                        if len(clean) >= 4 and clean[0:1].isupper():
                            _add_candidate(clean)
                i += 1
            else:
                i += 1

    if not candidates:
        return body_text

    print(f"    🔄 Nachanreicherung: {len(candidates)} Kandidaten aus Topic/Headline")

    # Für jeden Kandidaten prüfen: existiert ein Wikipedia-Artikel?
    # Wenn ja UND der Term kommt im Body vor → [[Wiki]]-Marker injizieren
    enriched = body_text
    links_added = 0
    max_links = 5  # Maximal 5 nachträgliche Links

    for search_term, display_term in candidates:
        if links_added >= max_links:
            break

        # Prüfe ob der Suchbegriff (oder ein Teil davon) im Body vorkommt
        body_match_term = None
        if search_term in enriched:
            body_match_term = search_term
        else:
            # Suche nach signifikanten Wörtern des Suchbegriffs im Body
            for word in search_term.split():
                clean_word = word.strip(".,;:—–-()")
                if len(clean_word) >= 4 and clean_word in enriched:
                    body_match_term = clean_word
                    break
            # Spezialfall NUR für Topic-Kandidaten: Zusammengesetzte Wörter
            # mit Bindestrich im Body finden.
            # z.B. Topic "Rifkrieg" → findet "Rif-Aufstandes" (Präfix-Match)
            # Versuche progressiv kürzere Präfixe (min. 3 Zeichen)
            if not body_match_term and " " not in search_term and search_term == topic:
                for prefix_len in range(len(search_term), 2, -1):
                    prefix = search_term[:prefix_len]
                    prefix_match = re.search(
                        r'\b(' + re.escape(prefix) + r'[\w]*(?:-[\w]+)*)',
                        enriched
                    )
                    if prefix_match and prefix_match.group(1) != prefix:
                        # Treffer muss länger als der Präfix sein (nicht nur das Präfix selbst)
                        body_match_term = prefix_match.group(1)
                        break

        if not body_match_term:
            continue

        # Generische Einwort-Begriffe als body_match_term ablehnen
        if body_match_term.lower() in generic_words:
            continue

        # Wikipedia-Artikel validieren (mit Disambiguierungs-Auflösung)
        validation = validate_wikipedia_link(search_term, resolve_disambig=True)
        if not validation:
            continue

        resolved_title, wiki_url = validation

        # Relevanz-Check: Der aufgelöste Titel sollte den Suchbegriff
        # (oder einen wesentlichen Teil) enthalten. Verhindert Fehlzuordnungen
        # wie "Kolonialverwaltung" → "Kolonialbehörden des Deutschen Reiches"
        search_lower = search_term.lower()
        resolved_lower = resolved_title.lower()
        if search_lower not in resolved_lower:
            # Prüfe ob zumindest ein signifikantes Wort (>=5 Zeichen) übereinstimmt
            search_words = {w for w in search_lower.split() if len(w) >= 5}
            resolved_words = set(resolved_lower.replace("(", "").replace(")", "").split())
            if not search_words & resolved_words:
                # Kein wesentliches Wort stimmt überein – wahrscheinlich Fehlzuordnung
                continue

        # Marker injizieren: Ersetze die ERSTE Vorkommen im Text
        # Nur wenn der Begriff noch nicht in einem [[]] oder <a>-Tag steht
        if f"[[{body_match_term}" in enriched or f">{body_match_term}</a>" in enriched:
            continue

        # Escape für Regex – nur außerhalb bestehender [[...]] Marker ersetzen
        escaped = re.escape(body_match_term)
        # Negative Lookahead/Lookbehind: nicht innerhalb von [[ ... ]] ersetzen
        # Einfacher Ansatz: prüfe ob die Fundstelle innerhalb eines Markers liegt
        match = re.search(escaped, enriched)
        if match:
            start_pos = match.start()
            # Prüfe ob wir innerhalb eines [[...]]-Markers sind
            before = enriched[:start_pos]
            open_brackets = before.count("[[") - before.count("]]")
            if open_brackets > 0:
                # Innerhalb eines Markers – überspringe
                continue
        new_text = re.sub(
            escaped,
            f"[[{resolved_title}|{body_match_term}]]",
            enriched,
            count=1
        )
        if new_text != enriched:
            enriched = new_text
            links_added += 1
            print(f"    ✓ Nachanreicherung: '{body_match_term}' → [[{resolved_title}]]")

    if links_added > 0:
        print(f"    🔄 Nachanreicherung abgeschlossen: {links_added} Links hinzugefügt")
    else:
        print(f"    – Nachanreicherung: keine passenden Wikipedia-Artikel gefunden")

    return enriched


def _normalize_article_text(article):
    """Post-Processing: Normalisiert Encoding, Gedankenstriche und Sonderzeichen.

    Wird nach der Artikelgenerierung aufgerufen, BEVOR die Anreicherung stattfindet.
    Korrigiert systematische Probleme, die das LLM trotz Prompt-Anweisungen produziert.
    """
    text_fields = ["headline", "subheadline", "dateline", "body_raw", "editorial_note"]

    for field in text_fields:
        text = article.get(field, "")
        if not text or not isinstance(text, str):
            continue

        # 1. UTF-8 Double-Encoding reparieren
        encoding_fixes = [
            ('\u00e2\u20ac\u201c', '\u2014'),   # â€" → —
            ('\u00e2\u20ac\u0153', '\u201e'),    # â€œ → „
            ('\u00e2\u20ac\u009d', '\u201c'),    # â€ → "
            ('\u00e2\u20ac\u2122', '\u2019'),    # â€™ → '
        ]
        for old, new in encoding_fixes:
            text = text.replace(old, new)

        # 2. En-Dashes (–) als Gedankenstriche → Em-Dashes (—)
        #    NUR wenn NICHT zwischen Ziffern (Zahlenbereiche wie 1916–1922 behalten)
        text = re.sub(r'(?<!\d)\u2013(?!\d)', '\u2014', text)

        # 3. Spaced Hyphens ( - ) als Gedankenstriche → Em-Dashes
        text = text.replace(' - ', ' \u2014 ')

        article[field] = text

    return article


def enrich_article(article, context_hits, source_registry, used_image_urls=None):
    """Einen Artikel mit Links, Bildern und Quellenapparat anreichern."""
    if used_image_urls is None:
        used_image_urls = set()

    # 0a. Wiki-Marker aus Headline/Subheadline extrahieren (für priorisierte Bildsuche)
    headline_targets = []
    for field in ("headline", "subheadline"):
        if field in article and article[field]:
            headline_targets.extend(_extract_wiki_marker_targets(article[field]))

    # 0b. Wiki-Marker in Nicht-Body-Feldern zu reinem Text auflösen
    for field in ("headline", "subheadline", "dateline", "editorial_note"):
        if field in article and article[field]:
            article[field] = strip_wiki_markers(article[field])

    # 1. Wikipedia-Links auflösen (body: als HTML-Links)
    body_raw = article.get("body_raw", "")
    body_html, wiki_links = resolve_wikipedia_links(body_raw)

    # 1b. Fallback: Wenn keine Wiki-Links gefunden wurden, nachträglich anreichern
    if not wiki_links and body_raw.strip():
        print(f"    ⚠ Keine [[Wiki]]-Marker im body_raw – starte Nachanreicherung...")
        enriched_raw = _retroactive_wiki_enrichment(
            body_raw,
            article.get("topic", ""),
            article.get("headline", ""),
            article.get("subheadline", "")
        )
        # Nochmal auflösen mit den neu injizierten Markern
        body_html, wiki_links = resolve_wikipedia_links(enriched_raw)

    # Zwischenspeichern: wiki_links für Bildsuche verfügbar machen
    article["wikipedia_links"] = wiki_links

    # 2. Bild-Marker auflösen (mit Keywords aus Wiki-Links, Duplikat-Check)
    #    headline_targets werden als priorisierte Kandidaten übergeben
    body_html, images = resolve_image_markers(
        body_html, article["topic"], article=article, used_urls=used_image_urls,
        headline_targets=headline_targets
    )
    
    # 3. HTML-Absätze formatieren
    paragraphs = [p.strip() for p in body_html.split('\n\n') if p.strip()]
    body_html = '\n'.join(f'<p>{p}</p>' for p in paragraphs)

    # 3b. Validierung: JSON-Leaks im body_html erkennen und bereinigen
    #     Wenn die Claude-API das JSON-Format nicht sauber einhält, können
    #     Felder wie "primary_sources" oder "editorial_note" als Klartext
    #     im body_html landen. Solche Fragmente werden hier abgeschnitten.
    json_leak_markers = ['"primary_sources"', '"editorial_note"', '"wikipedia_links"', '"score"', '"images"']
    for marker in json_leak_markers:
        if marker in body_html:
            leak_pos = body_html.find(marker)
            # Letzes valides </p> vor dem Leak finden
            last_valid_p = body_html.rfind('</p>', 0, leak_pos)
            if last_valid_p > 0:
                body_html = body_html[:last_valid_p + 4]
                print(f"    ⚠ JSON-Leak in body_html erkannt (Marker: {marker}). "
                      f"Text nach letztem </p> abgeschnitten.")
            break

    # 3c. Validierung: JSON-Leaks in editorial_note erkennen und bereinigen
    #     Gleiches Problem wie bei body_html: Das Modell generiert manchmal
    #     escaped JSON-Strukturen innerhalb des editorial_note-Strings.
    editorial_note = article.get("editorial_note", "")
    if editorial_note:
        json_leak_markers_note = [
            '"primary_sources"', '\\"primary_sources\\"',
            '"wikipedia_links"', '\\"wikipedia_links\\"',
            '"images"', '\\"images\\"',
            '"score"', '\\"score\\"',
            '"headline"', '\\"headline\\"',
            '"body_raw"', '\\"body_raw\\"',
        ]
        for marker in json_leak_markers_note:
            if marker in editorial_note:
                leak_pos = editorial_note.find(marker)
                # Text vor dem Leak behalten, trailing Whitespace/Interpunktion bereinigen
                cleaned = editorial_note[:leak_pos].rstrip(' ,\n\r\t\\\"')
                # Satzende sicherstellen
                if cleaned and cleaned[-1] not in '.!?"':
                    cleaned += '.'
                article["editorial_note"] = cleaned
                print(f"    ⚠ JSON-Leak in editorial_note erkannt (Marker: {marker}). "
                      f"Text abgeschnitten ({len(editorial_note)} → {len(cleaned)} Zeichen).")
                break

    # 4. Quellenapparat – dynamisch aus Kontext-Treffern
    primary_sources = build_source_apparatus(context_hits, source_registry)

    article["body_html"] = body_html
    article["wikipedia_links"] = wiki_links
    article["images"] = images
    article["primary_sources"] = primary_sources

    return article

def run_enrichment(articles, article_plan, contexts, source_registry):
    """Stufe 4: Alle Artikel anreichern.

    Verwendet die tatsächlichen Kontext-Treffer aus Stufe 2, um den
    Quellenapparat dynamisch aufzubauen. Nur Quellen mit echten
    Keyword-Treffern werden zitiert.

    Duplikat-Erkennung: Ein gemeinsames Set von bereits verwendeten
    Bild-URLs verhindert, dass dasselbe Bild in mehreren Artikeln
    einer Tagesausgabe erscheint.
    """
    print("\n" + "="*60)
    print("STUFE 4: ANREICHERUNG")
    print("="*60)

    used_image_urls = set()  # Duplikat-Tracking über alle Artikel
    enriched = []
    for i, article in enumerate(articles):
        topic = article["topic"]
        context_hits = contexts.get(topic, [])

        enriched_article = enrich_article(
            article, context_hits, source_registry,
            used_image_urls=used_image_urls
        )
        
        n_links = len(enriched_article.get("wikipedia_links", []))
        n_images = len(enriched_article.get("images", []))
        n_sources = len(enriched_article.get("primary_sources", []))
        source_names = [s["newspaper"] for s in enriched_article.get("primary_sources", [])]
        print(f"  [{article['type']}] {article['headline']}")
        print(f"    Wikipedia-Links: {n_links}, Bilder: {n_images}, Quellen: {n_sources}")
        print(f"    Quellen: {', '.join(source_names)}")
        
        enriched.append(enriched_article)
    
    return enriched

# ============ STUFE 5: EXPORT ============

def export_article_json(article, index):
    """Einen Artikel als JSON-Datei exportieren."""
    date_compact = TARGET_DATE.replace("-", "")
    filename = f"artikel_{date_compact}_{index+1:02d}.json"
    filepath = os.path.join(OUTPUT_DIR, filename)
    
    # Wörter zählen (aus body_raw, ohne HTML-Tags)
    clean_text = re.sub(r'<[^>]+>', '', article.get("body_html", article.get("body_raw", "")))
    word_count = len(clean_text.split())
    
    # Quellen zählen
    domestic_sources = [s for s in article.get("primary_sources", []) 
                       if s.get("archive") == "Deutsche Digitale Bibliothek"]
    international_sources = [s for s in article.get("primary_sources", []) 
                            if s.get("archive") != "Deutsche Digitale Bibliothek"]
    
    export = {
        "schema_version": "1.0",
        "project": "Vor 100 Jahren",
        "publication_date_historical": TARGET_DATE,
        "publication_date_modern": datetime.now().strftime("%Y-%m-%d"),
        "article_index": index + 1,
        "headline": article["headline"],
        "subheadline": article["subheadline"],
        "dateline": article["dateline"],
        "type": article["type"],
        "category": article["category"],
        "target_words": article["target_words"],
        "actual_words": word_count,
        "body_html": article["body_html"],
        "images": article.get("images", []),
        "wikipedia_links": article.get("wikipedia_links", []),
        "primary_sources": article.get("primary_sources", []),
        "editorial_note": article.get("editorial_note", ""),
        "score": article.get("score", 0),
        "domestic_source_count": len(domestic_sources),
        "international_source_count": len(international_sources),
        "wikipedia_match": len(article.get("wikipedia_links", [])) > 0
    }
    
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(export, f, ensure_ascii=False, indent=2)
    
    return filepath

def export_tagesausgabe_json(articles):
    """Alle Artikel als Tagesausgabe-JSON exportieren."""
    date_compact = TARGET_DATE.replace("-", "")
    filepath = os.path.join(OUTPUT_DIR, f"tagesausgabe_{date_compact}.json")
    
    tagesausgabe = {
        "schema_version": "1.0",
        "project": "Vor 100 Jahren",
        "date_historical": TARGET_DATE,
        "date_modern": datetime.now().strftime("%Y-%m-%d"),
        "edition_title": build_edition_title(TARGET_DATE),
        "article_count": len(articles),
        "articles": []
    }
    
    for i, article in enumerate(articles):
        clean_text = re.sub(r'<[^>]+>', '', article.get("body_html", ""))
        word_count = len(clean_text.split())
        
        tagesausgabe["articles"].append({
            "index": i + 1,
            "headline": article["headline"],
            "subheadline": article["subheadline"],
            "dateline": article["dateline"],
            "type": article["type"],
            "category": article["category"],
            "actual_words": word_count,
            "body_html": article["body_html"],
            "images": article.get("images", []),
            "wikipedia_links": article.get("wikipedia_links", []),
            "primary_sources": article.get("primary_sources", []),
            "editorial_note": article.get("editorial_note", ""),
            "score": article.get("score", 0)
        })
    
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(tagesausgabe, f, ensure_ascii=False, indent=2)
    
    return filepath

def run_export(articles, corpus=None):
    """Stufe 5: JSON-Export aller Artikel."""
    print("\n" + "="*60)
    print("STUFE 5: EXPORT")
    print("="*60)
    
    # Einzelartikel-Export deaktiviert (Daten sind vollständig in tagesausgabe enthalten)
    # for i, article in enumerate(articles):
    #     path = export_article_json(article, i)
    #     print(f"  Artikel {i+1}: {os.path.basename(path)}")

    # Tagesausgabe
    tagesausgabe_path = export_tagesausgabe_json(articles)
    print(f"  Tagesausgabe: {os.path.basename(tagesausgabe_path)}")
    
    # Kontextdaten für DOCX-Generierung speichern
    docx_data_path = os.path.join(OUTPUT_DIR, "docx_data.json")
    
    # Quellenstatistik berechnen
    all_sources = set()
    for article in articles:
        for src in article.get("primary_sources", []):
            all_sources.add(src["newspaper"])
    
    corpus_chars = sum(len(t) for t in corpus.values()) if corpus else 0
    corpus_pages = sum(t.count("--- Seite") for t in corpus.values()) if corpus else 0
    corpus_source_count = len(corpus) if corpus else len(all_sources)
    
    docx_data = {
        "date": TARGET_DATE,
        "date_display": _build_date_display(TARGET_DATE),
        "edition_title": build_edition_title(TARGET_DATE),
        "article_count": len(articles),
        "source_count": corpus_source_count,
        "corpus_chars": corpus_chars,
        "corpus_pages": corpus_pages,
        "articles": []
    }
    for article in articles:
        docx_data["articles"].append({
            "headline": article["headline"],
            "subheadline": article["subheadline"],
            "dateline": article["dateline"],
            "type": article["type"],
            "category": article["category"],
            "body_html": article["body_html"],
            "images": article.get("images", []),
            "primary_sources": article.get("primary_sources", []),
            "editorial_note": article.get("editorial_note", ""),
            "wikipedia_links": article.get("wikipedia_links", [])
        })
    
    with open(docx_data_path, 'w', encoding='utf-8') as f:
        json.dump(docx_data, f, ensure_ascii=False, indent=2)
    
    print(f"  DOCX-Daten: {os.path.basename(docx_data_path)}")
    
    return tagesausgabe_path

# ============ HILFSFUNKTIONEN ============

def _build_date_display(date_str):
    """Erzeugt die Datumsanzeige für den DOCX-Header/Titel, z.B. 'Donnerstag, den 4. März 1926'."""
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    weekdays = ["Montag", "Dienstag", "Mittwoch", "Donnerstag", "Freitag", "Samstag", "Sonntag"]
    months = ["", "Januar", "Februar", "März", "April", "Mai", "Juni",
              "Juli", "August", "September", "Oktober", "November", "Dezember"]
    return f"{weekdays[dt.weekday()]}, den {dt.day}. {months[dt.month]} {dt.year}"


def build_edition_title(date_str):
    """Erzeugt den Ausgabentitel dynamisch aus dem Datum."""
    return f"VOR 100 JAHREN – {_build_date_display(date_str)}"


# ============ HAUPTPROGRAMM ============

def main():
    global TARGET_DATE

    parser = argparse.ArgumentParser(
        description="Vor 100 Jahren – Step 2: Artikelproduktion",
        epilog="Beispiel: python step2_pipeline.py output/themenvorschlag_19260228.json",
    )
    parser.add_argument(
        "json_file",
        help="Themenvorschlag-JSON aus Step 1 (mit abgeschlossener Auswahl)",
    )
    parser.add_argument(
        "--output",
        default=OUTPUT_DIR,
        help=f"Ausgabeverzeichnis (Standard: {OUTPUT_DIR})",
    )

    args = parser.parse_args()

    # JSON laden und Datum extrahieren
    if not os.path.exists(args.json_file):
        print(f"FEHLER: Datei nicht gefunden: {args.json_file}", file=sys.stderr)
        sys.exit(1)

    TARGET_DATE_FROM_JSON = None
    with open(args.json_file, "r", encoding="utf-8") as f:
        step1_data = json.load(f)
    TARGET_DATE = step1_data.get("date_historical", TARGET_DATE)

    # API-Key prüfen
    if not ANTHROPIC_API_KEY:
        print("FEHLER: ANTHROPIC_API_KEY nicht gesetzt!", file=sys.stderr)
        sys.exit(1)

    print("="*60)
    print("VOR 100 JAHREN – Step-2-Pipeline")
    print(f"Datum: {TARGET_DATE}")
    print(f"Eingabe: {args.json_file}")
    print(f"Ausgabe: {args.output}")
    print("="*60)

    os.makedirs(args.output, exist_ok=True)

    # Stufe 0: Download
    corpus, stats, source_urls = run_download(TARGET_DATE)

    if not corpus:
        print("\nFEHLER: Kein Korpus heruntergeladen. Abbruch.")
        return

    # Stufe 0b: Textaufbereitung (Fraktur-Normalisierung + Absatz-Neusegmentierung)
    corpus = prepare_corpus_text(corpus)

    # Quellen-Registry aufbauen (dynamisch aus Konfiguration + Datum + spezifische URLs)
    source_registry = build_source_registry(
        TARGET_DATE,
        ddb_item_ids=source_urls.get("ddb_item_ids"),
        trove_urls=source_urls.get("trove_urls"),
        bne_issue_url=source_urls.get("bne_issue_url"),
        delpher_urls=source_urls.get("delpher_urls"),
    )

    # Stufe 1: Artikelauswahl aus Step-1-JSON
    print("\n" + "="*60)
    print("STUFE 1: ARTIKELAUSWAHL (aus Step-1-JSON)")
    print("="*60)
    article_plan = load_article_plan(args.json_file)
    for a in article_plan["articles"]:
        print(f"  [{a['type']}] {a['topic']}")
        print(f"    Kategorie: {a['category']}, Ziel: {a['target_words']} Wörter")

    # Stufe 2: Kontextextraktion
    contexts = run_context_extraction(corpus, article_plan)

    # Stufe 3: Artikelgenerierung per Claude API
    articles = generate_articles(article_plan, contexts)

    # Stufe 3b: Text-Normalisierung (Encoding, Gedankenstriche)
    for article in articles:
        _normalize_article_text(article)

    # Stufe 4: Anreicherung (Quellen dynamisch aus Kontext-Treffern)
    enriched_articles = run_enrichment(articles, article_plan, contexts, source_registry)

    # Stufe 4b: Finale Text-Normalisierung (body_html nach Anreicherung)
    for article in enriched_articles:
        for field in ("body_html", "editorial_note"):
            text = article.get(field, "")
            if text and isinstance(text, str):
                text = re.sub(r'(?<!\d)\u2013(?!\d)', '\u2014', text)
                text = text.replace(' - ', ' \u2014 ')
                article[field] = text

    # Stufe 5: Export (JSON)
    tagesausgabe_path = run_export(enriched_articles, corpus)

    # Stufe 5b: DOCX-Generierung
    print("\n" + "="*60)
    print("STUFE 5b: DOCX-GENERIERUNG")
    print("="*60)
    import subprocess
    docx_script = os.path.join(os.path.dirname(__file__), "generate_docx.js")
    if os.path.exists(docx_script):
        result = subprocess.run(["node", docx_script], capture_output=True, text=True, timeout=60)
        if result.returncode == 0:
            print(f"  {result.stdout.strip()}")
        else:
            print(f"  DOCX-Fehler: {result.stderr}")
    else:
        print(f"  WARNUNG: {docx_script} nicht gefunden, DOCX-Generierung übersprungen")

    # Zusammenfassung
    print("\n" + "="*60)
    print("ZUSAMMENFASSUNG")
    print("="*60)
    print(f"  Datum: {TARGET_DATE}")
    print(f"  Quellen heruntergeladen: {len(corpus)}")
    print(f"  Gesamtkorpus: {sum(len(t) for t in corpus.values()):,} Zeichen")
    print(f"  Artikel generiert: {len(enriched_articles)}")
    total_words = sum(len(re.sub(r'<[^>]+>', '', a.get('body_html', '')).split()) for a in enriched_articles)
    print(f"  Gesamtwortzahl: {total_words}")
    print(f"  JSON-Dateien: {len(enriched_articles) + 1}")
    date_compact = TARGET_DATE.replace("-", "")
    print(f"  Ausgaben: {args.output}/tagesausgabe_{date_compact}.json")
    print(f"\n  Pipeline abgeschlossen.")

if __name__ == "__main__":
    main()
