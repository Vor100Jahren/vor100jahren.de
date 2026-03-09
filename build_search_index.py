#!/usr/bin/env python3
"""
build_search_index.py — Baut den Suchindex für vor100jahren.de

Erzeugt zwei Dateien im docs/data/ Verzeichnis:
  1. search_index.json     — Volltextindex für Lunr.js (alle durchsuchbaren Felder)
  2. search_suggest.json   — Kompakter Vorschlagsindex für Autocomplete
                              (Headlines, Entitäten, Kategorien)

Aufruf:
    python3 build_search_index.py

Wird automatisch nach dem Export in step2_pipeline.py aufgerufen.
"""

import json
import glob
import os
import re
from collections import Counter

DATA_DIR = os.path.join(os.path.dirname(__file__), "docs", "data")


def strip_html(html):
    """HTML-Tags entfernen und Entities dekodieren."""
    text = re.sub(r'<[^>]+>', '', html)
    text = text.replace('&amp;', '&').replace('&lt;', '<').replace('&gt;', '>')
    text = text.replace('&quot;', '"').replace('&#39;', "'").replace('&nbsp;', ' ')
    # Mehrfache Leerzeichen normalisieren
    text = re.sub(r'\s+', ' ', text).strip()
    return text


def extract_articles():
    """Alle Artikel aus allen edition_*.json laden."""
    articles = []
    pattern = os.path.join(DATA_DIR, "edition_*.json")
    files = sorted(glob.glob(pattern))

    for filepath in files:
        filename = os.path.basename(filepath)
        # Datum aus Dateiname extrahieren: edition_1926-03-08.json → 1926-03-08
        match = re.search(r'edition_(\d{4}-\d{2}-\d{2})\.json', filename)
        if not match:
            continue
        date_str = match.group(1)

        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)

        # Format: entweder Liste von Artikeln oder Dict mit "articles" Key
        if isinstance(data, list):
            edition_articles = data
        else:
            edition_articles = data.get("articles", [])

        for i, a in enumerate(edition_articles):
            article_id = f"{date_str}_{i}"

            # Body als Klartext (inkl. editorial_note)
            body_text = strip_html(a.get("body_html", ""))
            editorial_note = a.get("editorial_note", "")
            if editorial_note:
                body_text += " " + strip_html(editorial_note)

            # Entitäten aus Wikipedia-Links
            entities = []
            for wl in a.get("wikipedia_links", []):
                term = wl.get("term", "") or wl.get("link_target", "")
                if term and term not in entities:
                    entities.append(term)

            # Bildtexte
            captions = []
            for img in a.get("images", []):
                alt = img.get("alt_text", "")
                cap = img.get("caption", "")
                if alt:
                    captions.append(alt)
                if cap:
                    captions.append(cap)

            articles.append({
                "id": article_id,
                "date": date_str,
                "index": i,
                "headline": a.get("headline", ""),
                "subheadline": a.get("subheadline", ""),
                "type": a.get("type", ""),
                "category": a.get("category", ""),
                "body": body_text,
                "entities": " ".join(entities),
                "captions": " ".join(captions),
            })

    return articles


def build_suggest_index(articles):
    """Kompakten Vorschlagsindex für Autocomplete erstellen.

    Enthält:
    - Alle Headlines (mit Datum + Artikel-ID für Navigation)
    - Alle Entitätsnamen (mit Häufigkeit)
    - Alle Kategorien
    """
    suggestions = {
        "headlines": [],
        "entities": [],
        "categories": []
    }

    entity_counter = Counter()
    category_set = set()

    for a in articles:
        # Headlines als Vorschläge
        suggestions["headlines"].append({
            "text": a["headline"],
            "sub": a["subheadline"],
            "date": a["date"],
            "id": a["id"],
        })

        # Entitäten zählen
        for entity in a["entities"].split():
            # Zusammengesetzte Namen (mit Leerzeichen) korrekt behandeln
            pass  # Wir verwenden die wiki_links direkt

        category_set.add(a["category"])

    # Entitäten direkt aus den Artikeln nochmal sauber extrahieren
    pattern = os.path.join(DATA_DIR, "edition_*.json")
    for filepath in sorted(glob.glob(pattern)):
        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)
        edition_articles = data if isinstance(data, list) else data.get("articles", [])
        for a in edition_articles:
            for wl in a.get("wikipedia_links", []):
                term = wl.get("term", "") or wl.get("link_target", "")
                if term:
                    entity_counter[term] += 1

    # Entitäten nach Häufigkeit sortiert
    suggestions["entities"] = [
        {"text": term, "count": count}
        for term, count in entity_counter.most_common()
    ]

    # Kategorien alphabetisch
    suggestions["categories"] = sorted(category_set)

    return suggestions


def main():
    print("Suchindex wird erstellt...")
    articles = extract_articles()
    print(f"  {len(articles)} Artikel aus {len(set(a['date'] for a in articles))} Editionen geladen")

    # 1. Volltextindex für Lunr.js
    index_path = os.path.join(DATA_DIR, "search_index.json")
    with open(index_path, 'w', encoding='utf-8') as f:
        json.dump(articles, f, ensure_ascii=False)
    size_kb = os.path.getsize(index_path) / 1024
    print(f"  search_index.json: {size_kb:.0f} KB ({len(articles)} Dokumente)")

    # 2. Vorschlagsindex für Autocomplete
    suggestions = build_suggest_index(articles)
    suggest_path = os.path.join(DATA_DIR, "search_suggest.json")
    with open(suggest_path, 'w', encoding='utf-8') as f:
        json.dump(suggestions, f, ensure_ascii=False)
    size_kb = os.path.getsize(suggest_path) / 1024
    n_entities = len(suggestions["entities"])
    n_headlines = len(suggestions["headlines"])
    print(f"  search_suggest.json: {size_kb:.0f} KB ({n_headlines} Headlines, {n_entities} Entitäten)")

    print("Fertig.")


if __name__ == "__main__":
    main()
