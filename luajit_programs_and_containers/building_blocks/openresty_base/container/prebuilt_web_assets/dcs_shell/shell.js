/* dcs_shell -- drawer nav + context-aware header + status popup.
 *
 * Contract with the server:
 *   Every /fragment/* response sends an HX-Trigger-After-Settle header:
 *     {"shell:context": {
 *        "title": "...",                 // top-bar title (required)
 *        "status_url": "/fragment/...",  // one-shot GET for status (optional)
 *        "status_stream_url": "/sse/..." // SSE stream for status (optional)
 *        "badge": "3"                    // optional; hides when missing
 *     }}
 *   Shell JS listens for `shell:context` and updates the chrome.
 *
 * URL persistence:
 *   Current view lives in location.hash as #view=<fragment path minus leading slash>.
 *   On load, if hash is present, that fragment is fetched; otherwise the
 *   content area stays on its initial server-rendered placeholder.
 */
(function () {
    'use strict';

    // ---- helpers ------------------------------------------------------
    const $ = (sel) => document.querySelector(sel);

    const drawer    = $('#shell-drawer');
    const scrim     = $('#shell-scrim');
    const title     = $('#shell-title');
    const badge     = $('#status-badge');
    const menuBtn   = $('#menu-btn');
    const statusBtn = $('#status-btn');
    const popup     = $('#status-popup');
    const statusBody = $('#status-body');
    const popupClose = $('#status-popup-close');
    const operatorEl = $('#operator-name');
    const alarmBtn   = $('#alarm-btn');
    const refreshBtn = $('#refresh-btn');

    // Current view's status endpoints -- updated each time shell:context fires.
    let currentStatusUrl = null;
    let currentStreamUrl = null;
    // Last known badge count; used to detect "new alarm" for the audio beep.
    let lastBadgeCount = 0;

    // ---- operator identity --------------------------------------------
    // Prompt on first visit, persist in localStorage. Rendered in the
    // header slot and also injected on every htmx request as
    // X-Operator for future audit-logging of mutations.

    function getOperator() {
        try {
            let op = localStorage.getItem('dcs.operator');
            if (op === null) {
                op = prompt('Operator name (for audit log):', '') || 'unknown';
                op = op.trim() || 'unknown';
                localStorage.setItem('dcs.operator', op);
            }
            return op;
        } catch (e) {
            return 'unknown';
        }
    }
    const operator = getOperator();
    if (operatorEl) operatorEl.textContent = operator;
    // Inject X-Operator on every htmx request via the htmx:configRequest
    // event (more robust than a body attribute when hx-boost etc. are in play).
    document.body.addEventListener('htmx:configRequest', (e) => {
        e.detail.headers['X-Operator'] = operator;
    });

    // ---- alarm toggle (audio beep on new-badge) -----------------------

    function getAlarmEnabled() {
        try { return localStorage.getItem('dcs.alarm_enabled') === '1'; }
        catch (e) { return false; }
    }
    function setAlarmEnabled(on) {
        try { localStorage.setItem('dcs.alarm_enabled', on ? '1' : '0'); }
        catch (e) {}
        renderAlarmBtn();
    }
    function renderAlarmBtn() {
        if (!alarmBtn) return;
        const on = getAlarmEnabled();
        alarmBtn.textContent = on ? '\uD83D\uDD14' : '\uD83D\uDD15';
        alarmBtn.title = on ? 'Audio alarm on (click to mute)'
                            : 'Audio alarm off (click to enable)';
    }
    // Short data-URI beep (440Hz sine, ~120ms, quiet) so no external asset
    // is needed. Lazy-create the Audio element on first play.
    let beepAudio = null;
    function playBeep() {
        if (!getAlarmEnabled()) return;
        try {
            if (!beepAudio) {
                // Minimal WAV: 8000Hz mono, 1000 samples of 440Hz tone.
                beepAudio = new Audio('data:audio/wav;base64,UklGRnoFAABXQVZFZm10IBAAAAABAAEAQB8AAEAfAAABAAgAZGF0YVYFAACAiY6SlZqen6SmqquusbO0tba3uLm6uru6urq5uLe2tLKxr62rqaalo6Gfnpybm5qZmJaXl5eWlZWUlZSTlJSUk5OTk5SUlJSVlZaXmJiZmpucnZ2eoKGio6Slpqeoqqusra6wsbO0tba4ubq7vL2+v8DBwsPEw8PDw8PDw8PDw8PCwsLCwsLCwsLCwsLCwsLCwsLCwsPDw8PDw8TExMTExMTExMTExMTExMXFxcXFxcXFxcTExMTExMPDw8PDwsLCwsLCwsHBwcHBwcHAwMDAwMDAwL+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/');
            }
            beepAudio.currentTime = 0;
            const p = beepAudio.play();
            if (p && p.catch) p.catch(() => { /* autoplay blocked; ignore */ });
        } catch (e) { /* ignore */ }
    }
    if (alarmBtn) {
        alarmBtn.addEventListener('click', () => setAlarmEnabled(!getAlarmEnabled()));
        renderAlarmBtn();
    }

    // ---- refresh button ------------------------------------------------
    // Manual re-fetch of the currently-loaded view. Uses the same
    // resolution as boot: hash -> default-view meta -> nothing.
    // Useful for polling state that changes slowly (maintenance lease
    // expiry, pg snapshot after an external mutation, etc.) without
    // forcing a full browser reload.

    function refreshCurrentView() {
        const m = location.hash.match(/^#view=(.+)$/);
        let fragment = m && m[1];
        if (!fragment) {
            const meta = document.querySelector('meta[name="dcs-default-view"]');
            if (meta && meta.content) fragment = meta.content;
        }
        if (fragment) {
            htmx.ajax('GET', fragment, '#shell-content');
        }
    }
    if (refreshBtn) {
        refreshBtn.addEventListener('click', refreshCurrentView);
    }

    // ---- drawer --------------------------------------------------------

    function openDrawer()  { drawer.classList.add('open');  scrim.classList.add('open'); }
    function closeDrawer() { drawer.classList.remove('open'); scrim.classList.remove('open'); }

    menuBtn.addEventListener('click', openDrawer);
    scrim.addEventListener('click', closeDrawer);
    document.addEventListener('keydown', (e) => {
        if (e.key === 'Escape') {
            if (popup.open)   closeStatus();
            else if (drawer.classList.contains('open')) closeDrawer();
        }
    });

    // ---- navigation ----------------------------------------------------
    // Tree leaves have data-fragment="/fragment/..."; intercept clicks,
    // fetch via htmx.ajax so HX-Trigger-After-Settle runs, close drawer,
    // update hash.

    drawer.addEventListener('click', (e) => {
        const a = e.target.closest('a[data-fragment]');
        if (!a) return;
        e.preventDefault();
        navigate(a.dataset.fragment, a);
    });

    function navigate(fragment, anchor) {
        // Highlight the newly selected leaf (clear old, set new).
        drawer.querySelectorAll('a.active').forEach(el => el.classList.remove('active'));
        if (anchor) anchor.classList.add('active');
        closeDrawer();
        // Close any open status popup; its context is about to change.
        if (popup.open) closeStatus();
        // `fragment` is always a relative URL (no leading /) -- apps
        // must keep all internal hrefs relative so they resolve
        // correctly both direct and behind the gateway's proxy prefix.
        if (location.hash !== '#view=' + fragment) {
            history.replaceState(null, '', '#view=' + fragment);
        }
        htmx.ajax('GET', fragment, '#shell-content');
    }

    window.addEventListener('hashchange', () => {
        const m = location.hash.match(/^#view=(.+)$/);
        if (m) {
            const fragment = m[1];
            const anchor = drawer.querySelector('a[data-fragment="' + fragment + '"]');
            navigate(fragment, anchor);
        }
    });

    // Boot: if the URL carries #view=..., load it. Otherwise, auto-load
    // the default view declared by <meta name="dcs-default-view" ...>
    // in the page head -- falls back to the placeholder baked into the
    // HTML if neither is present.
    window.addEventListener('DOMContentLoaded', () => {
        tickStaleElements();
        const m = location.hash.match(/^#view=(.+)$/);
        let fragment = m && m[1];
        if (!fragment) {
            const meta = document.querySelector('meta[name="dcs-default-view"]');
            if (meta && meta.content) fragment = meta.content;
        }
        if (fragment) {
            const anchor = drawer.querySelector('a[data-fragment="' + fragment + '"]');
            if (anchor) anchor.classList.add('active');
            htmx.ajax('GET', fragment, '#shell-content');
        }
    });

    // ---- shell:context event ------------------------------------------
    // Fired by htmx when the server sets
    //   HX-Trigger-After-Settle: {"shell:context": {...}}
    // on a /fragment/* response.

    document.body.addEventListener('shell:context', (e) => {
        const ctx = e.detail || {};
        if (ctx.title)  title.textContent = ctx.title;
        currentStatusUrl  = ctx.status_url        || null;
        currentStreamUrl  = ctx.status_stream_url || null;

        // [ⓘ] button gets dimmed if the view exposes no status at all.
        const hasStatus = !!(currentStatusUrl || currentStreamUrl);
        statusBtn.disabled = !hasStatus;
        statusBtn.style.opacity = hasStatus ? '1' : '0.4';

        // Badge + beep on increase. Server omits the field when count==0.
        const newCount = parseInt(ctx.badge || '0', 10) || 0;
        if (newCount > 0) {
            badge.textContent = String(newCount);
            badge.hidden = false;
        } else {
            badge.hidden = true;
        }
        if (newCount > lastBadgeCount) playBeep();
        lastBadgeCount = newCount;

        // Decorate any <time> elements that just arrived in the content.
        tickStaleElements();
    });

    // ---- stale-time ticker --------------------------------------------
    // Every second, rewrite the text of each <time datetime="..."> to
    // "Ns ago" (or similar) and apply .stale when older than the
    // data-stale-after threshold (seconds; default 30).

    function formatAgo(seconds) {
        if (seconds < 0)        return 'just now';
        if (seconds < 5)        return 'just now';
        if (seconds < 60)       return seconds + 's ago';
        if (seconds < 3600)     return Math.floor(seconds / 60) + 'm ago';
        if (seconds < 86400)    return Math.floor(seconds / 3600) + 'h ago';
        return Math.floor(seconds / 86400) + 'd ago';
    }
    function tickStaleElements() {
        const now = Date.now();
        document.querySelectorAll('time[datetime]').forEach((el) => {
            const ts = Date.parse(el.getAttribute('datetime'));
            if (isNaN(ts)) return;
            const age = Math.max(0, Math.floor((now - ts) / 1000));
            el.textContent = formatAgo(age);
            const threshold = parseInt(el.dataset.staleAfter || '30', 10);
            el.classList.toggle('stale', age > threshold);
        });
    }
    setInterval(tickStaleElements, 1000);

    // ---- status popup --------------------------------------------------

    function openStatus() {
        // Rebuild body from current endpoint each open. SSE connections
        // (if any) are created by htmx-ext-sse when we insert the sse
        // div; torn down when we clear the innerHTML on close.
        statusBody.innerHTML = '<div class="empty">Loading&hellip;</div>';
        if (currentStreamUrl) {
            // Insert an htmx-ext-sse bound div; htmx will auto-init it
            // because htmx.process() is called by the htmx.ajax we did
            // on the outer swap. But for a fresh insertion we call it
            // explicitly.
            statusBody.innerHTML =
                '<div hx-ext="sse" sse-connect="' + escapeAttr(currentStreamUrl) +
                '" sse-swap="update"><div class="empty">Waiting for first event&hellip;</div></div>';
            htmx.process(statusBody);
        } else if (currentStatusUrl) {
            htmx.ajax('GET', currentStatusUrl, '#status-body');
        } else {
            statusBody.innerHTML = '<div class="empty">No status for this view.</div>';
        }
        if (popup.showModal) popup.showModal();
        else popup.setAttribute('open', '');
    }

    function closeStatus() {
        // Clearing innerHTML removes any SSE-bound div, which causes
        // htmx-ext-sse to tear down the EventSource.
        statusBody.innerHTML = '';
        if (popup.close) popup.close();
        else popup.removeAttribute('open');
    }

    statusBtn.addEventListener('click', () => {
        if (popup.open) closeStatus(); else openStatus();
    });
    popupClose.addEventListener('click', closeStatus);
    // Desktop: clicking the backdrop (outside the dropdown) closes.
    popup.addEventListener('click', (e) => {
        if (e.target === popup) closeStatus();
    });

    // ---- util ----------------------------------------------------------

    function escapeAttr(s) {
        return String(s)
            .replace(/&/g, '&amp;')
            .replace(/"/g, '&quot;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;');
    }
})();
