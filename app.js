
    // ═══════════════════════════════════════════════════════
    // DATA (loaded from external JSON files)
    // ═══════════════════════════════════════════════════════
    let EDITIONS = {};
    let CHRONIK = [];
    let editionDates = [];
    let currentEditionIndex = 0;

    async function loadEditionsIndex() {
        const resp = await fetch('data/editions_index.json');
        const index = await resp.json();
        editionDates = index.dates.sort();
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
        'El Sol': 'Spanien'
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
                if (['Deutschland'].includes(country)) allLanguages.add('Deutsch');
                if (['Frankreich'].includes(country)) allLanguages.add('Französisch');
                if (['USA'].includes(country)) allLanguages.add('Englisch');
                if (['Österreich'].includes(country)) allLanguages.add('Deutsch');
                if (['Niederlande'].includes(country)) allLanguages.add('Niederländisch');
                if (['China'].includes(country)) allLanguages.add('Englisch');
                if (['Russland'].includes(country)) allLanguages.add('Russisch');
                if (['Spanien'].includes(country)) allLanguages.add('Spanisch');
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
        html += `<div class="article-body">${a.body_html}</div>`;

        // Editorial note
        if (a.editorial_note && a.editorial_note.trim()) {
            html += `<details class="editorial-note" open>
                <summary>Redaktionelle Anmerkung</summary>
                <p>${a.editorial_note}</p>
            </details>`;
        }

        // Sources with enhanced format
        html += renderSources(a.primary_sources);

        html += '</article>';
        return html;
    }

    function renderSources(sources) {
        if (!sources || sources.length === 0) return '';
        let html = '<div class="article-sources"><span class="sources-label">Quellen:</span> ';
        const parts = sources.map(s => {
            let part = '';
            if (s.url && s.url !== '#') {
                part += `<a href="${escHtml(s.url)}" target="_blank" rel="noopener">${escHtml(s.newspaper)}</a>`;
            } else {
                part += escHtml(s.newspaper);
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
        const countryOrder = ['Deutschland', 'Frankreich', 'Österreich', 'Niederlande', 'Russland', 'Spanien', 'USA', 'China', 'Weitere'];
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

        document.getElementById('archive-year').textContent = year;

        // Determine which months have editions
        const monthsWithEditions = new Set();
        editionDates.forEach(d => {
            const m = parseInt(d.split('-')[1]);
            monthsWithEditions.add(m);
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
            monthsHtml += `<span class="archive-month ${cls}" onclick="selectArchiveMonth(${m})">${name}</span>`;
        });
        document.getElementById('archive-months').innerHTML = monthsHtml;

        // Show days for selected month
        const daysInMonth = new Date(year, month, 0).getDate();
        const editionsInMonth = editionDates.filter(d => {
            const parts = d.split('-');
            return parseInt(parts[1]) === month;
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
                daysHtml += `<span class="archive-day ${cls}" onclick="loadEditionByDate('${dateKey}')">${day}</span>`;
            } else {
                daysHtml += `<span class="archive-day" style="opacity:0.3; cursor:default;">${day}</span>`;
            }
        }
        document.getElementById('archive-days').innerHTML = daysHtml;
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
    // INIT
    // ═══════════════════════════════════════════════════════

    document.addEventListener('DOMContentLoaded', async () => {
        // Show loading state
        document.getElementById('main-content').innerHTML =
            '<p style="text-align:center;padding:3rem;color:var(--ink-muted);">Ausgabe wird geladen…</p>';

        // Load index and chronik in parallel
        await Promise.all([loadEditionsIndex(), loadChronik()]);

        // Render the latest edition
        await renderEdition(editionDates[currentEditionIndex]);
    });