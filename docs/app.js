
    // ═══════════════════════════════════════════════════════
    // DATA (loaded from external JSON files)
    // ═══════════════════════════════════════════════════════
    let EDITIONS = {};
    let CHRONIK = [];
    let SPECIALS_INDEX = [];
    let SPECIALS_CACHE = {};
    let editionDates = [];
    let currentEditionIndex = 0;

    async function loadEditionsIndex() {
        const resp = await fetch('data/editions_index.json');
        const index = await resp.json();
        // Nur Ausgaben anzeigen, deren Tag+Monat <= heute (Jahr ignorieren)
        const now = new Date();
        const todayMonth = now.getMonth() + 1;
        const todayDay = now.getDate();
        const todayMinutes = now.getHours() * 60 + now.getMinutes();
        editionDates = index.dates.sort().filter(d => {
            const parts = d.split('-');
            const m = parseInt(parts[1]);
            const day = parseInt(parts[2]);
            // Ausgabe sichtbar ab 00:01 am jeweiligen Tag+Monat
            if (m < todayMonth) return true;
            if (m === todayMonth && day < todayDay) return true;
            if (m === todayMonth && day === todayDay && todayMinutes >= 1) return true;
            return false;
        });
        currentEditionIndex = editionDates.length - 1;
    }

    async function loadEdition(dateStr) {
        if (EDITIONS[dateStr]) return EDITIONS[dateStr];
        const resp = await fetch(`data/edition_${dateStr}.json`);
        const articles = await resp.json();
        EDITIONS[dateStr] = articles;
        return articles;
    }

    async function loadChronik() {
        const resp = await fetch('data/chronik.json');
        CHRONIK = await resp.json();
    }

    async function loadSpecialsIndex() {
        try {
            const resp = await fetch('data/specials_index.json');
            const index = await resp.json();
            SPECIALS_INDEX = index.specials || [];
        } catch (e) {
            SPECIALS_INDEX = [];
        }
    }

    async function loadSpecial(id) {
        if (SPECIALS_CACHE[id]) return SPECIALS_CACHE[id];
        const entry = SPECIALS_INDEX.find(s => s.id === id);
        if (!entry) return null;
        const resp = await fetch(`data/${entry.file}`);
        const data = await resp.json();
        SPECIALS_CACHE[id] = data;
        return data;
    }

// ═══════════════════════════════════════════════════════
    // STATE
    // ═══════════════════════════════════════════════════════
    
    // ═══════════════════════════════════════════════════════
    // HELPERS
    // ═══════════════════════════════════════════════════════
    const WOCHENTAGE = ['Sonntag','Montag','Dienstag','Mittwoch','Donnerstag','Freitag','Samstag'];
    const MONATE = ['Januar','Februar','März','April','Mai','Juni','Juli','August','September','Oktober','November','Dezember'];
    const MONATE_KURZ = ['Jan','Feb','Mär','Apr','Mai','Jun','Jul','Aug','Sep','Okt','Nov','Dez'];

    function formatDateLong(dateStr) {
        const d = new Date(dateStr + 'T12:00:00');
        return `${WOCHENTAGE[d.getDay()]}, den ${d.getDate()}. ${MONATE[d.getMonth()]} ${d.getFullYear()}`;
    }

    function formatDateShort(dateStr) {
        const parts = dateStr.split('-');
        return `${parseInt(parts[2])}. ${MONATE[parseInt(parts[1])-1]} ${parts[0]}`;
    }

    // Map article type to CSS class
    function articleClass(type) {
        const t = type.toLowerCase();
        if (t.includes('haupt')) return 'lead';
        if (t.includes('kurz')) return 'brief';
        if (t.includes('feuilleton')) return 'feuilleton';
        return 'standard';
    }

    // Source registry: map newspaper names to countries/archives
    const SOURCE_COUNTRIES = {
        'Vorwärts': 'Deutschland', 'Berliner Tageblatt': 'Deutschland',
        'Kölnische Zeitung': 'Deutschland', 'Deutscher Reichsanzeiger': 'Deutschland',
        'Badische Presse': 'Deutschland', 'Sächsische Staatszeitung': 'Deutschland',
        'Harburger Tageblatt': 'Deutschland', 'Westfälischer Merkur': 'Deutschland',
        'Hamburger Echo': 'Deutschland',
        'Le Figaro': 'Frankreich', 'Le Temps': 'Frankreich',
        'The Sun': 'USA', 'The Sun (New York)': 'USA',
        'New York Times': 'USA', 'The New York Times': 'USA',
        'The Washington Post': 'USA', 'Washington Post': 'USA',
        'Evening Star': 'USA', 'Evening Star (Washington, D.C.)': 'USA',
        'Neue Freie Presse': 'Österreich', 'Wiener Zeitung': 'Österreich',
        'Nieuwe Rotterdamsche Courant': 'Niederlande', 'NRC': 'Niederlande',
        'North China Herald': 'China',
        'Prawda': 'Russland', 'ПРАВДА': 'Russland', 'Pravda': 'Russland',
        'El Sol': 'Spanien',
        'Deutsche Allgemeine Zeitung': 'Deutschland',
        'Hufvudstadsbladet': 'Finnland',
        'Helsingin Sanomat': 'Finnland',
        'The Washington Times': 'USA', 'Washington Times': 'USA',
        'Hong Kong Telegraph': 'China',
        'Frankfurter Zeitung': 'Deutschland',
        'Vossische Zeitung': 'Deutschland',
        'Münchner Neueste Nachrichten': 'Deutschland',
        'Germania': 'Deutschland',
        'Kreuz-Zeitung': 'Deutschland',
        'Neue Zürcher Zeitung': 'Schweiz', 'NZZ': 'Schweiz',
        'The Times': 'Großbritannien',
        'Daily Telegraph': 'Großbritannien',
        'Daily Mail': 'Großbritannien',
        'Le Matin': 'Frankreich',
        'Corriere della Sera': 'Italien'
    };

    // ═══════════════════════════════════════════════════════
    // RENDER FUNCTIONS
    // ═══════════════════════════════════════════════════════

    async function renderEdition(dateStr) {
        await loadEdition(dateStr);
        const articles = EDITIONS[dateStr];
        if (!articles || articles.length === 0) return;

        // Update masthead
        document.getElementById('masthead-date').textContent = formatDateLong(dateStr);

        // Compute stats
        const allSources = new Set();
        const allCountries = new Set();
        const allLanguages = new Set();
        articles.forEach(a => {
            a.primary_sources.forEach(s => {
                allSources.add(s.newspaper);
                const country = SOURCE_COUNTRIES[s.newspaper] || 'Unbekannt';
                allCountries.add(country);
                if (['Deutschland', 'Österreich', 'Schweiz'].includes(country)) allLanguages.add('Deutsch');
                if (['Frankreich'].includes(country)) allLanguages.add('Französisch');
                if (['USA', 'Großbritannien', 'China'].includes(country)) allLanguages.add('Englisch');
                if (['Niederlande'].includes(country)) allLanguages.add('Niederländisch');
                if (['Russland'].includes(country)) allLanguages.add('Russisch');
                if (['Spanien'].includes(country)) allLanguages.add('Spanisch');
                if (['Finnland'].includes(country)) allLanguages.add('Finnisch');
                if (['Italien'].includes(country)) allLanguages.add('Italienisch');
            });
        });
        document.getElementById('masthead-stats').textContent =
            `${articles.length} Artikel aus ${allSources.size} Originalquellen · ${allCountries.size} Länder · ${allLanguages.size} Sprachen`;

        // Update title
        document.title = `VOR 100 JAHREN — ${formatDateLong(dateStr)}`;

        // Update edition navigation
        document.getElementById('btn-prev').disabled = (currentEditionIndex <= 0);
        document.getElementById('btn-next').disabled = (currentEditionIndex >= editionDates.length - 1);
        document.getElementById('edition-indicator').textContent =
            `Ausgabe ${currentEditionIndex + 1} von ${editionDates.length}`;

        // Build main content
        const main = document.getElementById('main-content');
        let html = '';

        // Artikel nach Typ sortieren: Hauptartikel → Artikel → Kurzbeitrag
        const typePriority = (type) => {
            const t = type.toLowerCase();
            if (t.includes('haupt')) return 0;
            if (t.includes('kurz')) return 2;
            return 1; // Artikel, Feuilleton etc.
        };
        const sorted = [...articles].sort((a, b) => typePriority(a.type) - typePriority(b.type));

        // Table of Contents (sortierte Reihenfolge)
        html += '<div class="toc deco-corner"><div class="toc-header">Inhalt dieser Ausgabe</div><ol class="toc-list">';
        sorted.forEach((a, i) => {
            html += `<li><a href="#artikel-${i+1}"><span class="toc-type">${a.type}</span><span class="toc-title">${escHtml(a.headline)}</span></a></li>`;
        });
        html += '</ol></div>';

        // Artikel rendern: Hauptartikel + Artikel volle Breite, Kurzbeiträge im Grid
        const fullWidth = sorted.filter(a => !a.type.toLowerCase().includes('kurz'));
        const briefs = sorted.filter(a => a.type.toLowerCase().includes('kurz'));
        let idx = 0;

        // Hauptartikel + Artikel: jeweils volle Breite
        fullWidth.forEach((a) => {
            const isLead = (idx === 0);
            html += renderArticle(a, idx, isLead);
            idx++;
        });

        // Kurzbeiträge: im 2-Spalten-Grid
        if (briefs.length > 0) {
            html += '<div class="articles-grid briefs-grid">';
            briefs.forEach((a) => {
                html += renderArticle(a, idx, false);
                idx++;
            });
            html += '</div>';
        }

        main.innerHTML = html;

        // Update sidebar
        renderSourceList(articles);
        renderChronik(dateStr);
        renderArchiveCalendar(dateStr);
        renderFooter(dateStr, allSources);
    }

    function renderArticle(a, index, isLead) {
        const cls = articleClass(a.type);
        let html = `<article class="article article--${cls}" id="artikel-${index+1}">`;

        // Meta
        html += `<div class="article-meta">
            <span class="article-type">${escHtml(a.type)}</span>
            <span class="article-category">${escHtml(a.category)}</span>
        </div>`;

        // Headline
        html += `<h2 class="article-headline">${escHtml(a.headline)}</h2>`;
        if (a.subheadline) {
            html += `<p class="article-subheadline">${escHtml(a.subheadline)}</p>`;
        }
        html += '<div class="article-separator"></div>';

        // Dateline
        if (a.dateline) {
            html += `<p class="article-dateline">${escHtml(a.dateline)}</p>`;
        }

        // Image + Body
        // Lead article: large hero image above text (Aufmacher)
        // Other articles: small floated image beside text
        if (a.images && a.images.length > 0) {
            const img = a.images[0];
            html += `<figure class="article-image">
                <img src="${escHtml(img.url)}" alt="${escHtml(img.alt_text || '')}" loading="lazy">
                <figcaption>${escHtml(img.caption || '')}
                    <span class="image-credit">${escHtml(img.credit || '')}</span>
                </figcaption>
            </figure>`;
        }

        // Body (body_html already contains <p>, <a>, <em> tags)
        // Externe Links (Wikipedia etc.) in neuem Tab oeffnen
        const bodyHtml = a.body_html.replace(/<a\s+href="(https?:\/\/[^"]*)"(?![^>]*target=)/g,
            '<a href="$1" target="_blank" rel="noopener"');
        html += `<div class="article-body">${bodyHtml}</div>`;

        // Editorial note
        if (a.editorial_note && a.editorial_note.trim()) {
            html += `<details class="editorial-note" open>
                <summary>Redaktionelle Anmerkung</summary>
                <p>${a.editorial_note}</p>
            </details>`;
        }

        // Footnotes (inline source references)
        html += renderFootnotes(a.footnotes);

        html += '</article>';
        return html;
    }

    function renderFootnotes(footnotes) {
        if (!footnotes || footnotes.length === 0) return '';
        let html = '<div class="article-footnotes"><span class="footnotes-label">Quellenverweise:</span><ol class="footnote-list">';
        footnotes.forEach(fn => {
            html += `<li id="fn-${fn.id}" class="footnote-item">`;
            html += `<a href="#fnref-${fn.id}" class="footnote-backref" title="Zurück zum Text"></a>`;
            const fnName = fn.display_name || fn.newspaper;
            if (fn.url && fn.url !== '#' && fn.url !== '') {
                html += `<a href="${escHtml(fn.url)}" target="_blank" rel="noopener">${escHtml(fnName)}</a>`;
            } else {
                html += escHtml(fnName);
            }
            if (fn.archive && fn.archive.toLowerCase() !== 'unbekannt') {
                html += ` <span class="footnote-archive">(${escHtml(fn.archive)})</span>`;
            }
            if (fn.date) {
                html += ` <span class="footnote-date">${escHtml(fn.date)}</span>`;
            }
            html += '</li>';
        });
        html += '</ol></div>';
        return html;
    }

    function renderSources(sources) {
        if (!sources || sources.length === 0) return '';
        let html = '<div class="article-sources"><span class="sources-label">Quellen:</span> ';
        const parts = sources.map(s => {
            let part = '';
            const srcName = s.display_name || s.newspaper;
            if (s.url && s.url !== '#') {
                part += `<a href="${escHtml(s.url)}" target="_blank" rel="noopener">${escHtml(srcName)}</a>`;
            } else {
                part += escHtml(srcName);
            }
            if (s.archive && s.archive.toLowerCase() !== 'unbekannt') {
                part += ` <span class="source-archive">(${escHtml(s.archive)})</span>`;
            }
            if (s.pages_cited && s.pages_cited !== 'diverse') {
                part += ` <span class="source-pages">S. ${escHtml(s.pages_cited)}</span>`;
            }
            return part;
        });
        html += parts.join(' · ');
        html += '</div>';
        return html;
    }

    function renderSourceList(articles) {
        // Collect all unique sources grouped by country
        const byCountry = {};
        articles.forEach(a => {
            a.primary_sources.forEach(s => {
                const country = SOURCE_COUNTRIES[s.newspaper] || 'Weitere';
                if (!byCountry[country]) byCountry[country] = new Set();
                byCountry[country].add(s.newspaper);
            });
        });

        // Order: Deutschland first, then alphabetical
        const countryOrder = ['Deutschland', 'Frankreich', 'Österreich', 'Schweiz', 'Niederlande', 'Großbritannien', 'Italien', 'Finnland', 'Russland', 'Spanien', 'USA', 'China', 'Weitere'];
        const sortedCountries = Object.keys(byCountry).sort((a, b) => {
            const ia = countryOrder.indexOf(a);
            const ib = countryOrder.indexOf(b);
            return (ia === -1 ? 99 : ia) - (ib === -1 ? 99 : ib);
        });

        let html = '';
        sortedCountries.forEach(country => {
            html += `<span class="source-country">${escHtml(country)}</span>`;
            [...byCountry[country]].sort().forEach(name => {
                html += `<li>${escHtml(name)}</li>`;
            });
        });

        document.getElementById('source-list').innerHTML = html;
    }

    function renderChronik(dateStr) {
        // Find Chronik events near this date (within ±7 days of Berichterstattungsdatum)
        // Also show upcoming notable events
        const current = new Date(dateStr + 'T12:00:00');
        const nearEvents = [];
        const upcomingEvents = [];

        CHRONIK.forEach(ev => {
            const reportDate = ev.berichterstattungsdatum;
            if (reportDate) {
                const rd = new Date(reportDate + 'T12:00:00');
                // Shift to the same century-offset
                const rdShifted = new Date(rd);
                const daysDiff = Math.abs((current - rdShifted) / (1000 * 60 * 60 * 24));
                if (daysDiff <= 7) {
                    nearEvents.push({ ...ev, daysDiff });
                }
            }
            // Also show future events within 6 months for the "upcoming" section
            const evDate = new Date(ev.datum + 'T12:00:00');
            const daysAhead = (evDate - current) / (1000 * 60 * 60 * 24);
            if (daysAhead > 0 && daysAhead <= 180 && (ev.relevanz === 'Zentral' || ev.relevanz === 'Hoch')) {
                upcomingEvents.push({ ...ev, daysAhead });
            }
        });

        nearEvents.sort((a, b) => a.daysDiff - b.daysDiff);
        upcomingEvents.sort((a, b) => a.daysAhead - b.daysAhead);

        let html = '';

        if (nearEvents.length > 0) {
            html += '<li style="font-family:var(--font-ui); font-size:0.7rem; letter-spacing:0.1em; text-transform:uppercase; color:var(--accent); padding:0.3rem 0; border-bottom:1px solid var(--deco-gold);">Aktuell berichtet</li>';
            nearEvents.slice(0, 3).forEach(ev => {
                html += `<li>
                    <span class="key-date-date">${formatDateShort(ev.datum)}</span>
                    <span class="key-date-title">${escHtml(ev.ereignis)}</span>
                    <span class="key-date-category">${escHtml(ev.kategorie)}</span>
                </li>`;
            });
        }

        if (upcomingEvents.length > 0) {
            html += '<li style="font-family:var(--font-ui); font-size:0.7rem; letter-spacing:0.1em; text-transform:uppercase; color:var(--accent); padding:0.3rem 0; margin-top:0.5rem; border-bottom:1px solid var(--deco-gold);">Kommende Schlüsseldaten</li>';
            upcomingEvents.slice(0, 5).forEach(ev => {
                html += `<li>
                    <span class="key-date-date">${formatDateShort(ev.datum)}</span>
                    <span class="key-date-title">${escHtml(ev.ereignis)}</span>
                    <span class="key-date-epoche">${escHtml(ev.epoche)}</span>
                </li>`;
            });
        }

        if (html === '') {
            html = '<li style="color:var(--ink-muted); font-style:italic; font-size:0.95rem;">Keine Schlüsseldaten in der Nähe dieses Datums.</li>';
        }

        document.getElementById('key-dates-list').innerHTML = html;
    }

    function renderArchiveCalendar(dateStr) {
        const year = parseInt(dateStr.split('-')[0]);
        const month = parseInt(dateStr.split('-')[1]);

        // Collect available years from edition dates
        const availableYears = [...new Set(editionDates.map(d => parseInt(d.split('-')[0])))].sort();

        // Render year selector with buttons
        let yearHtml = '';
        if (availableYears.length > 1) {
            availableYears.forEach(y => {
                const isActive = (y === year);
                yearHtml += `<span class="archive-year-btn ${isActive ? 'active' : ''}" role="button" tabindex="0" onclick="selectArchiveYear(${y})">${y}</span>`;
            });
        } else {
            yearHtml = `<span class="archive-year-single">${year}</span>`;
        }
        document.getElementById('archive-year').innerHTML = yearHtml;

        // Determine which months have editions FOR THIS YEAR
        const monthsWithEditions = new Set();
        editionDates.forEach(d => {
            const parts = d.split('-');
            if (parseInt(parts[0]) === year) {
                monthsWithEditions.add(parseInt(parts[1]));
            }
        });

        let monthsHtml = '';
        MONATE_KURZ.forEach((name, i) => {
            const m = i + 1;
            const hasEditions = monthsWithEditions.has(m);
            const isActive = (m === month);
            let cls = '';
            if (isActive) cls = 'active';
            else if (hasEditions) cls = 'has-editions';
            else cls = 'disabled';
            monthsHtml += `<span class="archive-month ${cls}" role="button" tabindex="0" onclick="selectArchiveMonth(${m})">${name}</span>`;
        });
        document.getElementById('archive-months').innerHTML = monthsHtml;

        // Show days for selected month
        const daysInMonth = new Date(year, month, 0).getDate();
        const editionsInMonth = editionDates.filter(d => {
            const parts = d.split('-');
            return parseInt(parts[0]) === year && parseInt(parts[1]) === month;
        });
        const editionDays = new Set(editionsInMonth.map(d => parseInt(d.split('-')[2])));
        const currentDay = parseInt(dateStr.split('-')[2]);

        let daysHtml = '';
        for (let day = 1; day <= daysInMonth; day++) {
            const dateKey = `${year}-${String(month).padStart(2,'0')}-${String(day).padStart(2,'0')}`;
            const hasEdition = editionDays.has(day);
            const isCurrent = (day === currentDay && parseInt(dateStr.split('-')[1]) === month);
            let cls = '';
            if (isCurrent) cls = 'current';
            else if (hasEdition) cls = 'active';
            if (hasEdition || isCurrent) {
                daysHtml += `<span class="archive-day ${cls}" role="button" tabindex="0" onclick="loadEditionByDate('${dateKey}')">${day}</span>`;
            } else {
                daysHtml += `<span class="archive-day" style="opacity:0.3; cursor:default;">${day}</span>`;
            }
        }
        document.getElementById('archive-days').innerHTML = daysHtml;
    }

    function selectArchiveYear(year) {
        // Find first edition in that year and navigate to it
        const editionsInYear = editionDates.filter(d => d.startsWith(`${year}-`));
        if (editionsInYear.length > 0) {
            loadEditionByDate(editionsInYear[0]);
        } else {
            // Just update calendar display
            renderArchiveCalendar(`${year}-01-01`);
        }
    }

    function renderFooter(dateStr, allSources) {
        // Collect unique archives
        const archives = new Set();
        const articles = EDITIONS[dateStr];
        articles.forEach(a => {
            a.primary_sources.forEach(s => {
                if (s.archive) archives.add(s.archive);
            });
        });

        const archiveLinks = {
            'Deutsche Digitale Bibliothek': 'https://www.deutsche-digitale-bibliothek.de',
            'Gallica (BnF)': 'https://gallica.bnf.fr',
            'Library of Congress': 'https://chroniclingamerica.loc.gov',
            'Library of Congress – Chronicling America': 'https://chroniclingamerica.loc.gov',
            'Internet Archive': 'https://archive.org',
            'ANNO (Österreichische Nationalbibliothek)': 'https://anno.onb.ac.at',
            'Delpher (Koninklijke Bibliotheek)': 'https://www.delpher.nl',
            'Hemeroteca Digital (BNE)': 'https://hemerotecadigital.bne.es',
        };

        let linksHtml = '';
        archives.forEach(archive => {
            const url = archiveLinks[archive];
            if (url) {
                linksHtml += `<a href="${url}" target="_blank" rel="noopener">${archive}</a> · `;
            } else {
                linksHtml += `${archive} · `;
            }
        });
        linksHtml = linksHtml.replace(/ · $/, '');
        document.getElementById('footer-archives').innerHTML = linksHtml;

        document.getElementById('colophon-bottom').textContent =
            `Projekt "Vor 100 Jahren" · Ausgabe vom ${formatDateShort(dateStr)} · ${articles.length} Artikel aus ${allSources.size} historischen Originalquellen`;
    }

    // ═══════════════════════════════════════════════════════
    // SONDERAUSGABEN (Special Editions)
    // ═══════════════════════════════════════════════════════

    function renderSpecialsSidebar() {
        const section = document.getElementById('specials-section');
        const list = document.getElementById('specials-list');
        if (!section || !list || SPECIALS_INDEX.length === 0) return;

        section.style.display = '';
        let html = '';
        SPECIALS_INDEX.forEach(s => {
            html += `<li class="special-item" role="button" tabindex="0" onclick="showSpecialEdition('${escHtml(s.id)}')">
                <span class="special-date">${formatDateShort(s.date_historical)}</span>
                <span class="special-title">${escHtml(s.event)}</span>
                <span class="special-meta">${s.article_count} Artikel · ${escHtml(s.kategorie)}</span>
            </li>`;
        });
        list.innerHTML = html;
    }

    async function showSpecialEdition(id) {
        const data = await loadSpecial(id);
        if (!data) return;

        const entry = SPECIALS_INDEX.find(s => s.id === id);

        // Update masthead
        document.getElementById('masthead-date').textContent =
            `Sonderausgabe: ${formatDateShort(data.date_historical)}`;
        document.getElementById('masthead-stats').textContent =
            `${data.articles.length} Artikel zum Ereignis: ${data.event}`;
        document.title = `VOR 100 JAHREN — Sonderausgabe: ${data.event}`;

        // Update nav
        document.getElementById('edition-indicator').textContent = 'Sonderausgabe';

        // Render content
        const main = document.getElementById('main-content');
        let html = '';

        // Editorial note banner
        if (data.editorial_note) {
            html += `<div class="special-editorial-note deco-corner">
                <div class="special-editorial-label">Redaktionelle Einordnung</div>
                <p>${escHtml(data.editorial_note)}</p>
            </div>`;
        }

        // TOC
        html += '<div class="toc deco-corner"><div class="toc-header">Inhalt dieser Sonderausgabe</div><ol class="toc-list">';
        data.articles.forEach((a, i) => {
            html += `<li><a href="#artikel-${i+1}"><span class="toc-type">${escHtml(a.dateline)}</span><span class="toc-title">${escHtml(a.headline)}</span></a></li>`;
        });
        html += '</ol></div>';

        // Articles
        data.articles.forEach((a, i) => {
            html += renderArticle(a, i, i === 0);
        });

        main.innerHTML = html;

        // Update sidebar source list
        renderSourceList(data.articles);

        // Back-to-edition button in Chronik sidebar
        const chronikSection = document.getElementById('key-dates-list');
        if (chronikSection) {
            chronikSection.innerHTML = `<li><a href="#" onclick="loadEditionByDate(editionDates[currentEditionIndex]); return false;" style="color:var(--accent); font-size:0.9rem;">&#9664; Zurück zur Tagesausgabe</a></li>`;
        }

        window.scrollTo({ top: 0, behavior: 'smooth' });
    }

    // ═══════════════════════════════════════════════════════
    // NAVIGATION
    // ═══════════════════════════════════════════════════════

    async function navigateEdition(delta) {
        const newIndex = currentEditionIndex + delta;
        if (newIndex >= 0 && newIndex < editionDates.length) {
            currentEditionIndex = newIndex;
            await renderEdition(editionDates[currentEditionIndex]);
            window.scrollTo({ top: 0, behavior: 'smooth' });
        }
    }

    async function loadEditionByDate(dateStr) {
        const idx = editionDates.indexOf(dateStr);
        if (idx !== -1) {
            currentEditionIndex = idx;
            await renderEdition(dateStr);
            window.scrollTo({ top: 0, behavior: 'smooth' });
        }
    }

    async function loadLatestEdition() {
        currentEditionIndex = editionDates.length - 1;
        await renderEdition(editionDates[currentEditionIndex]);
        window.scrollTo({ top: 0, behavior: 'smooth' });
    }

    function selectArchiveMonth(month) {
        // Find first edition in that month
        const year = editionDates[currentEditionIndex].split('-')[0];
        const monthStr = String(month).padStart(2, '0');
        const editionsInMonth = editionDates.filter(d => d.startsWith(`${year}-${monthStr}`));
        if (editionsInMonth.length > 0) {
            loadEditionByDate(editionsInMonth[0]);
        } else {
            // Just update the calendar display for that month
            const dateStr = editionDates[currentEditionIndex];
            const fakeDate = `${year}-${monthStr}-01`;
            renderArchiveCalendar(fakeDate);
        }
    }

    // ═══════════════════════════════════════════════════════
    // UTILITY
    // ═══════════════════════════════════════════════════════

    function escHtml(str) {
        if (!str) return '';
        const div = document.createElement('div');
        div.textContent = str;
        return div.innerHTML;
    }

    // ═══════════════════════════════════════════════════════
    // SEARCH
    // ═══════════════════════════════════════════════════════

    let searchIndex = null;      // Lunr.js Index
    let searchDocs = null;       // Rohdaten (Array of {id, headline, ...})
    let searchSuggestions = null; // Vorschlagsdaten
    let searchLoading = false;
    let lastSearchQuery = '';    // Letzte Suchanfrage (für "Zurück zu Ergebnissen")

    async function loadSearchData() {
        if (searchDocs) return; // Bereits geladen
        if (searchLoading) return;
        searchLoading = true;
        try {
            const [indexResp, suggestResp] = await Promise.all([
                fetch('data/search_index.json'),
                fetch('data/search_suggest.json')
            ]);
            searchDocs = await indexResp.json();
            searchSuggestions = await suggestResp.json();

            // Lunr.js Index aufbauen
            searchIndex = lunr(function () {
                this.use(lunr.de);
                this.ref('id');
                this.field('headline', { boost: 10 });
                this.field('subheadline', { boost: 5 });
                this.field('entities', { boost: 8 });
                this.field('captions', { boost: 2 });
                this.field('category', { boost: 3 });
                this.field('body');

                searchDocs.forEach(doc => { this.add(doc); });
            });
        } catch (e) {
            console.error('Suchindex konnte nicht geladen werden:', e);
        }
        searchLoading = false;
    }

    function showSuggestions(query) {
        const container = document.getElementById('search-suggestions');
        if (!query || query.length < 2 || !searchSuggestions) {
            container.style.display = 'none';
            return;
        }
        const q = query.toLowerCase();
        let items = [];

        // Headlines durchsuchen
        searchSuggestions.headlines.forEach(h => {
            if (h.text.toLowerCase().includes(q) || (h.sub && h.sub.toLowerCase().includes(q))) {
                items.push({ type: 'article', text: h.text, sub: h.date, id: h.id });
            }
        });

        // Entitäten durchsuchen
        searchSuggestions.entities.forEach(e => {
            if (e.text.toLowerCase().includes(q)) {
                items.push({ type: 'entity', text: e.text, sub: `${e.count} Artikel` });
            }
        });

        // Kategorien durchsuchen
        searchSuggestions.categories.forEach(c => {
            if (c.toLowerCase().includes(q)) {
                items.push({ type: 'category', text: c, sub: 'Kategorie' });
            }
        });

        // Begrenzen und nach Typ sortieren (Artikel zuerst, dann Entitäten, dann Kategorien)
        const typeOrder = { article: 0, entity: 1, category: 2 };
        items.sort((a, b) => typeOrder[a.type] - typeOrder[b.type]);
        items = items.slice(0, 8);

        if (items.length === 0) {
            container.style.display = 'none';
            return;
        }

        let html = '';
        items.forEach((item, i) => {
            const icon = item.type === 'article' ? '📰' : item.type === 'entity' ? '🔗' : '📂';
            html += `<div class="suggestion-item" data-index="${i}" role="button" tabindex="0" onclick="selectSuggestion('${escHtml(item.text)}', '${item.type}', '${item.id || ''}')">
                <span class="suggestion-icon">${icon}</span>
                <span class="suggestion-text">${highlightMatch(escHtml(item.text), q)}</span>
                <span class="suggestion-sub">${escHtml(item.sub)}</span>
            </div>`;
        });
        container.innerHTML = html;
        // Position: unterhalb der Topbar, vom Suchfeld bis zum Registrieren-Button
        const input = document.getElementById('search-input');
        const regBtn = document.querySelector('.btn-register');
        const topbar = document.querySelector('.topbar');
        if (input && regBtn && topbar) {
            const inputLeft = input.getBoundingClientRect().left;
            const regRight = regBtn.getBoundingClientRect().right;
            const topbarBottom = topbar.getBoundingClientRect().bottom;
            container.style.left = inputLeft + 'px';
            container.style.top = topbarBottom + 'px';
            container.style.width = (regRight - inputLeft) + 'px';
        }
        container.style.display = 'block';
    }

    function highlightMatch(text, query) {
        const idx = text.toLowerCase().indexOf(query.toLowerCase());
        if (idx === -1) return text;
        return text.slice(0, idx) + '<mark>' + text.slice(idx, idx + query.length) + '</mark>' + text.slice(idx + query.length);
    }

    function selectSuggestion(text, type, articleId) {
        const input = document.getElementById('search-input');
        if (type === 'article' && articleId) {
            // Direkt zum Artikel navigieren
            const parts = articleId.split('_');
            const dateStr = parts[0];
            const articleIndex = parseInt(parts[1]);
            input.value = text;
            document.getElementById('search-suggestions').style.display = 'none';
            navigateToArticle(dateStr, articleIndex);
        } else {
            // Suchbegriff übernehmen und Volltextsuche starten
            input.value = text;
            document.getElementById('search-suggestions').style.display = 'none';
            executeSearch(text);
        }
    }

    async function executeSearch(query) {
        if (!query || query.length < 2) return;
        await loadSearchData();
        if (!searchIndex) return;
        lastSearchQuery = query;

        // Lunr.js Suche — Wildcard für Teilwortsuche
        let results;
        try {
            results = searchIndex.search(query + '~1'); // Fuzzy search
        } catch (e) {
            // Fallback bei ungültigen Suchbegriffen
            try {
                results = searchIndex.search(query);
            } catch (e2) {
                results = [];
            }
        }

        // Ergebnisse mit Dokumentdaten anreichern
        const docsById = {};
        searchDocs.forEach(d => { docsById[d.id] = d; });

        const enriched = results.map(r => {
            const doc = docsById[r.ref];
            if (!doc) return null;
            return { ...doc, score: r.score };
        }).filter(Boolean);

        renderSearchResults(query, enriched);
    }

    function renderSearchResults(query, results) {
        const container = document.getElementById('search-results-container');
        const resultsDiv = document.getElementById('search-results');
        const infoSpan = document.getElementById('search-results-info');

        if (results.length === 0) {
            infoSpan.textContent = `Keine Ergebnisse für „${query}"`;
            resultsDiv.innerHTML = '<p class="search-no-results">Keine Artikel gefunden. Versuchen Sie einen anderen Suchbegriff.</p>';
        } else {
            infoSpan.textContent = `${results.length} Ergebnis${results.length !== 1 ? 'se' : ''} für „${query}"`;
            let html = '';
            results.forEach(r => {
                const snippet = makeSnippet(r.body, query, 150);
                html += `<div class="search-result-item" role="button" tabindex="0" onclick="navigateToArticle('${r.date}', ${r.index})">
                    <div class="search-result-meta">
                        <span class="search-result-date">${formatDateShort(r.date)}</span>
                        <span class="search-result-category">${escHtml(r.category)}</span>
                        <span class="search-result-type">${escHtml(r.type)}</span>
                    </div>
                    <div class="search-result-headline">${highlightMatch(escHtml(r.headline), query)}</div>
                    ${r.subheadline ? `<div class="search-result-subheadline">${highlightMatch(escHtml(r.subheadline), query)}</div>` : ''}
                    <div class="search-result-snippet">${highlightMatch(escHtml(snippet), query)}</div>
                </div>`;
            });
            resultsDiv.innerHTML = html;
        }

        container.style.display = 'block';
        document.getElementById('page-wrapper').style.display = 'none';
    }

    function makeSnippet(text, query, maxLen) {
        if (!text) return '';
        const q = query.toLowerCase();
        const idx = text.toLowerCase().indexOf(q);
        if (idx === -1) return text.slice(0, maxLen) + (text.length > maxLen ? '…' : '');
        const start = Math.max(0, idx - 60);
        const end = Math.min(text.length, idx + query.length + 90);
        let snippet = '';
        if (start > 0) snippet += '…';
        snippet += text.slice(start, end);
        if (end < text.length) snippet += '…';
        return snippet;
    }

    async function navigateToArticle(dateStr, articleIndex) {
        // Suchergebnisse ausblenden, aber Query merken
        document.getElementById('search-results-container').style.display = 'none';
        document.getElementById('page-wrapper').style.display = '';

        // "Zurück zu Ergebnissen"-Banner anzeigen
        showSearchBackBanner();

        await loadEditionByDate(dateStr);
        // Zum Artikel scrollen
        setTimeout(() => {
            const articles = document.querySelectorAll('#main-content article');
            if (articles[articleIndex]) {
                articles[articleIndex].scrollIntoView({ behavior: 'smooth', block: 'start' });
                articles[articleIndex].classList.add('search-highlight');
                setTimeout(() => articles[articleIndex].classList.remove('search-highlight'), 3000);
            }
        }, 300);
    }

    function showSearchBackBanner() {
        // Bestehenden Banner entfernen falls vorhanden
        const existing = document.getElementById('search-back-banner');
        if (existing) existing.remove();

        if (!lastSearchQuery) return;

        const banner = document.createElement('div');
        banner.id = 'search-back-banner';
        banner.className = 'search-back-banner';
        banner.innerHTML = `<button onclick="returnToSearchResults()">&#9664; Zurück zu Suchergebnissen für „${escHtml(lastSearchQuery)}"</button>`;
        // Banner VOR edition-layout einfügen (nicht innerhalb des Grid!)
        const editionLayout = document.querySelector('.edition-layout');
        if (editionLayout) {
            editionLayout.parentElement.insertBefore(banner, editionLayout);
        } else {
            const main = document.getElementById('main-content');
            main.parentElement.insertBefore(banner, main);
        }
    }

    function returnToSearchResults() {
        // Banner entfernen
        const banner = document.getElementById('search-back-banner');
        if (banner) banner.remove();

        // Suchergebnisse wieder anzeigen
        document.getElementById('search-results-container').style.display = 'block';
        document.getElementById('page-wrapper').style.display = 'none';
    }

    function closeSearch() {
        document.getElementById('search-results-container').style.display = 'none';
        document.getElementById('page-wrapper').style.display = '';
        document.getElementById('search-input').value = '';
        document.getElementById('search-suggestions').style.display = 'none';
        lastSearchQuery = '';
        // Banner entfernen falls vorhanden
        const banner = document.getElementById('search-back-banner');
        if (banner) banner.remove();
    }

    // Event-Listener für Suchfeld
    function initSearch() {
        const input = document.getElementById('search-input');
        if (!input) return;

        let debounceTimer;

        // Autocomplete bei Eingabe
        input.addEventListener('input', () => {
            clearTimeout(debounceTimer);
            const q = input.value.trim();
            debounceTimer = setTimeout(async () => {
                if (q.length >= 2) {
                    await loadSearchData();
                    showSuggestions(q);
                } else {
                    document.getElementById('search-suggestions').style.display = 'none';
                }
            }, 200);
        });

        // Enter-Taste: Volltextsuche
        input.addEventListener('keydown', (e) => {
            if (e.key === 'Enter') {
                e.preventDefault();
                document.getElementById('search-suggestions').style.display = 'none';
                executeSearch(input.value.trim());
            }
            if (e.key === 'Escape') {
                document.getElementById('search-suggestions').style.display = 'none';
                input.blur();
            }
        });

        // Vorschläge schließen bei Klick außerhalb
        document.addEventListener('click', (e) => {
            if (!e.target.closest('.search-wrapper')) {
                document.getElementById('search-suggestions').style.display = 'none';
            }
        });
    }

    // ═══════════════════════════════════════════════════════
    // INIT
    // ═══════════════════════════════════════════════════════

    document.addEventListener('DOMContentLoaded', async () => {
        // Show loading state
        document.getElementById('main-content').innerHTML =
            '<p style="text-align:center;padding:3rem;color:var(--ink-muted);">Ausgabe wird geladen…</p>';

        // Load index, chronik and specials in parallel
        await Promise.all([loadEditionsIndex(), loadChronik(), loadSpecialsIndex()]);

        // Suchfunktion initialisieren
        initSearch();

        // Sonderausgaben in Sidebar rendern
        renderSpecialsSidebar();

        // Check for ?date= URL parameter (e.g. from chronik.html links)
        const urlParams = new URLSearchParams(window.location.search);
        const requestedDate = urlParams.get('date');
        if (requestedDate && editionDates.includes(requestedDate)) {
            currentEditionIndex = editionDates.indexOf(requestedDate);
        }

        // Render the edition
        await renderEdition(editionDates[currentEditionIndex]);

        // Scroll-Buttons: Up = Artikelanfang / ganz oben, Down = nächster Artikel / ganz unten
        const scrollUpBtn = document.getElementById('scroll-top-btn');
        const scrollDownBtn = document.getElementById('scroll-down-btn');

        if (scrollUpBtn) {
            let upTimer = null;
            scrollUpBtn.addEventListener('click', () => {
                if (upTimer) return;
                upTimer = setTimeout(() => {
                    upTimer = null;
                    const articles = document.querySelectorAll('#main-content article');
                    let target = null;
                    for (let i = articles.length - 1; i >= 0; i--) {
                        const rect = articles[i].getBoundingClientRect();
                        if (rect.top < -10) {
                            target = articles[i];
                            break;
                        }
                    }
                    if (target) {
                        target.scrollIntoView({ behavior: 'smooth', block: 'start' });
                    } else {
                        window.scrollTo({ top: 0, behavior: 'smooth' });
                    }
                }, 250);
            });
            scrollUpBtn.addEventListener('dblclick', (e) => {
                e.preventDefault();
                if (upTimer) { clearTimeout(upTimer); upTimer = null; }
                window.scrollTo({ top: 0, behavior: 'smooth' });
            });
        }

        if (scrollDownBtn) {
            let downTimer = null;
            scrollDownBtn.addEventListener('click', () => {
                if (downTimer) return;
                downTimer = setTimeout(() => {
                    downTimer = null;
                    const articles = document.querySelectorAll('#main-content article');
                    let target = null;
                    for (let i = 0; i < articles.length; i++) {
                        const rect = articles[i].getBoundingClientRect();
                        if (rect.top > 10) {
                            target = articles[i];
                            break;
                        }
                    }
                    if (target) {
                        target.scrollIntoView({ behavior: 'smooth', block: 'start' });
                    } else {
                        window.scrollTo({ top: document.body.scrollHeight, behavior: 'smooth' });
                    }
                }, 250);
            });
            scrollDownBtn.addEventListener('dblclick', (e) => {
                e.preventDefault();
                if (downTimer) { clearTimeout(downTimer); downTimer = null; }
                window.scrollTo({ top: document.body.scrollHeight, behavior: 'smooth' });
            });
        }

        // Sichtbarkeit der Scroll-Buttons
        window.addEventListener('scroll', () => {
            const show = window.scrollY > 400;
            if (scrollUpBtn) scrollUpBtn.classList.toggle('visible', show);
            if (scrollDownBtn) scrollDownBtn.classList.toggle('visible', show);
        });
    });