/* ── Sticky offsets + topic quick search ── */
function syncStickyNavHeight() {
    var nav = document.querySelector('.nav');
    if (!nav) return;
    document.documentElement.style.setProperty('--nav-sticky-height', nav.offsetHeight + 'px');
}

window.addEventListener('resize', function() {
    clearTimeout(window.__xfiStickyResizeTimer);
    window.__xfiStickyResizeTimer = setTimeout(syncStickyNavHeight, 80);
    clearTimeout(window.__xfiSidebarResizeTimer);
    window.__xfiSidebarResizeTimer = setTimeout(initWeeklySidebarCollapse, 80);
    clearTimeout(window.__xfiSwipeVoteResizeTimer);
    window.__xfiSwipeVoteResizeTimer = setTimeout(function() {
        if (typeof refreshMobileSwipeVoting === 'function') refreshMobileSwipeVoting(document);
    }, 110);
});

function _topicSearchEsc(text) {
    var div = document.createElement('div');
    div.appendChild(document.createTextNode(text || ''));
    return div.innerHTML;
}

var TOPIC_SEARCH_STATE = { timer: null, seq: 0, items: [], activeIndex: -1, controller: null };
var TOPIC_SEARCH_CACHE = {};
var TOPIC_SEARCH_CACHE_TTL_MS = 15000;
var WEEKLY_SIDEBAR_PREF_KEY = 'xfi_weekly_sidebar_open';
var UI_SCALE_PREF_KEY = 'xfi_ui_scale';
var UI_SCALE_DEFAULT = 1.06;
var UI_SCALE_MIN = 0.90;
var UI_SCALE_MAX = 1.30;
var UI_SCALE_STEP = 0.06;
var THEME_PREF_KEY = 'xfi_theme';
var SWIPE_VOTE_HINT_KEY = 'xfi_swipe_vote_hint_seen_v2';
var SWIPE_VOTE_MOBILE_MAX_WIDTH = 768;
var SWIPE_VOTE_SIDE_COMMIT_PX = 72;
var SWIPE_VOTE_SIDE_PREVIEW_PX = 14;
var SWIPE_VOTE_DRAG_START_PX = 7;
var SWIPE_VOTE_CANCEL_TAP_PX = 7;
var SWIPE_VOTE_LONG_PRESS_MS = 420;
var SWIPE_VOTE_LONG_PRESS_SLOP_PX = 10;
var _SWIPE_VOTE_HINT_SHOWN_IN_PAGE = false;
var _SWIPE_VOTE_HINT_EL = null;

/* ── Theme toggle ── */
function toggleTheme() {
    var html = document.documentElement;
    var current = html.getAttribute('data-theme') || 'dark';
    var next = current === 'dark' ? 'light' : 'dark';
    html.setAttribute('data-theme', next);
    localStorage.setItem(THEME_PREF_KEY, next);
    updateThemeToggleIcon();
}
function initTheme() {
    var saved = localStorage.getItem(THEME_PREF_KEY);
    if (saved) document.documentElement.setAttribute('data-theme', saved);
    updateThemeToggleIcon();
}
function updateThemeToggleIcon() {
    var btn = document.getElementById('themeToggleBtn');
    if (!btn) return;
    var isDark = (document.documentElement.getAttribute('data-theme') || 'dark') !== 'light';
    btn.innerHTML = isDark ? '&#9789;' : '&#9788;';
    btn.title = isDark ? 'Switch to light mode' : 'Switch to dark mode';
}
function getEmbedTheme() {
    return (document.documentElement.getAttribute('data-theme') || 'dark') === 'light' ? 'light' : 'dark';
}

/* ── User menu panel positioning (fixed) ── */
function positionUserMenuPanel() {
    var details = document.querySelector('.user-menu');
    if (!details) return;
    var summary = details.querySelector('summary');
    var panel = details.querySelector('.user-menu-panel');
    if (!summary || !panel) return;
    var rect = summary.getBoundingClientRect();
    panel.style.top = (rect.bottom + 6) + 'px';
    panel.style.right = (window.innerWidth - rect.right) + 'px';
}
(function() {
    var details = document.querySelector('.user-menu');
    if (details) {
        details.addEventListener('toggle', positionUserMenuPanel);
    }
})();

function _abortTopicSearchController(state) {
    if (!state || !state.controller) return;
    try { state.controller.abort(); } catch (e) {}
    state.controller = null;
}

function _topicSearchCacheKey(query, limit) {
    return String(limit || 10) + '|' + String(query || '').trim().toLowerCase();
}

function _fetchTopicSearchJson(query, limit, state) {
    var q = String(query || '').trim();
    var n = Math.max(1, parseInt(limit, 10) || 10);
    var cacheKey = _topicSearchCacheKey(q, n);
    var now = Date.now();
    var cached = TOPIC_SEARCH_CACHE[cacheKey];
    if (cached && (now - cached.ts) < TOPIC_SEARCH_CACHE_TTL_MS) {
        return Promise.resolve(cached.data);
    }

    _abortTopicSearchController(state);

    var opts = {};
    if (typeof AbortController !== 'undefined') {
        state.controller = new AbortController();
        opts.signal = state.controller.signal;
    }

    function cleanupController() {
        if (state && state.controller && opts.signal && state.controller.signal === opts.signal) {
            state.controller = null;
        }
    }

    return fetch('/api/topics/search?q=' + encodeURIComponent(q) + '&limit=' + encodeURIComponent(String(n)), opts)
        .then(function(r) { return r.json(); })
        .then(function(data) {
            TOPIC_SEARCH_CACHE[cacheKey] = { ts: Date.now(), data: data };
            cleanupController();
            return data;
        })
        .catch(function(err) {
            cleanupController();
            throw err;
        });
}

function _clampUiScale(n) {
    var x = Number(n);
    if (!isFinite(x)) x = UI_SCALE_DEFAULT;
    if (x < UI_SCALE_MIN) x = UI_SCALE_MIN;
    if (x > UI_SCALE_MAX) x = UI_SCALE_MAX;
    return Math.round(x * 100) / 100;
}

function _updateUiScaleReadout(scale) {
    var el = document.getElementById('fontScaleReadout');
    if (!el) return;
    el.textContent = Math.round(scale * 100) + '%';
}

function applyUiScale(scale, persist) {
    var s = _clampUiScale(scale);
    document.documentElement.style.setProperty('--ui-scale', String(s));
    _updateUiScaleReadout(s);
    if (persist !== false) {
        try { localStorage.setItem(UI_SCALE_PREF_KEY, String(s)); } catch (e) {}
    }
    // Re-sync layout-sensitive UI after scaling (sticky bars, grid fade rows, sidebar widths).
    if (typeof syncStickyNavHeight === 'function') syncStickyNavHeight();
    if (typeof initWeeklySidebarCollapse === 'function') setTimeout(initWeeklySidebarCollapse, 40);
    if (typeof reapplyFading === 'function') {
        requestAnimationFrame(function() { reapplyFading(); });
        setTimeout(reapplyFading, 90);
    }
}

function stepUiScale(deltaSteps) {
    var cur = parseFloat(getComputedStyle(document.documentElement).getPropertyValue('--ui-scale')) || UI_SCALE_DEFAULT;
    applyUiScale(cur + (UI_SCALE_STEP * deltaSteps));
}

function resetUiScale() {
    applyUiScale(UI_SCALE_DEFAULT);
}

function initUiScaleControls() {
    var saved = null;
    try { saved = localStorage.getItem(UI_SCALE_PREF_KEY); } catch (e) {}
    applyUiScale(saved != null ? parseFloat(saved) : UI_SCALE_DEFAULT, false);
}

function _setWeeklySidebarToggleUi(isOpen) {
    var btn = document.getElementById('weeklySidebarToggle');
    if (!btn) return;
    btn.classList.toggle('is-open', !!isOpen);
    btn.setAttribute('aria-expanded', isOpen ? 'true' : 'false');
    btn.textContent = isOpen ? 'Hide' : 'Trending';
}

function initWeeklySidebarCollapse() {
    var layout = document.getElementById('weeklyLayout');
    var sidebar = document.getElementById('weeklySidebarCol');
    var btn = document.getElementById('weeklySidebarToggle');
    if (!layout || !sidebar || !btn) return;

    if (window.innerWidth <= 1100) {
        layout.classList.remove('sidebar-collapsed');
        _setWeeklySidebarToggleUi(true);
        if (typeof reapplyFading === 'function') setTimeout(reapplyFading, 60);
        return;
    }

    var pref = null;
    try { pref = localStorage.getItem(WEEKLY_SIDEBAR_PREF_KEY); } catch (e) {}
    var isOpen = pref === 'open' ? true : false; // default collapsed
    layout.classList.toggle('sidebar-collapsed', !isOpen);
    _setWeeklySidebarToggleUi(isOpen);
    if (typeof reapplyFading === 'function') setTimeout(reapplyFading, 80);
}

function toggleWeeklySidebar() {
    var layout = document.getElementById('weeklyLayout');
    if (!layout) return;
    var isCollapsed = layout.classList.toggle('sidebar-collapsed');
    var isOpen = !isCollapsed;
    try { localStorage.setItem(WEEKLY_SIDEBAR_PREF_KEY, isOpen ? 'open' : 'closed'); } catch (e) {}
    _setWeeklySidebarToggleUi(isOpen);
    if (typeof reapplyFading === 'function') {
        requestAnimationFrame(function() { reapplyFading(); });
        setTimeout(reapplyFading, 100);
    }
}

function _topicSearchQueryValue() {
    var input = document.getElementById('topicQuickSearch');
    return input ? String(input.value || '').trim() : '';
}

function goToTopicSearchResults(query) {
    var q = String(query != null ? query : _topicSearchQueryValue()).trim();
    if (q.length < 2) return;
    window.location.href = '/topics?q=' + encodeURIComponent(q);
}

function goToQuickCreateTopic() {
    var q = _topicSearchQueryValue();
    if (q.length < 2) return;
    window.location.href = '/topics/new?name=' + encodeURIComponent(q);
}

function _topicSearchTokens(q) {
    return String(q || '')
        .toLowerCase()
        .trim()
        .split(/\s+/)
        .filter(function(tok) { return tok.length >= 2; });
}

function applyLocalTopicCardFilter(query) {
    var tokens = _topicSearchTokens(query);
    var cards = document.querySelectorAll('.topic-card-sm, .topic-card');
    if (!cards.length) return;

    cards.forEach(function(card) {
        if (card.closest('#reportedGrid')) return;
        if (!tokens.length) {
            card.classList.remove('nav-search-local-hidden');
            return;
        }

        var hay = card.getAttribute('data-nav-search-text');
        if (!hay) {
            var bits = [];
            var h = card.querySelector('h3');
            var d = card.querySelector('.topic-desc');
            if (h) bits.push(h.textContent || '');
            if (d) bits.push(d.textContent || '');
            card.querySelectorAll('.topic-meta, .badge, .badge-sub, .badge-candidate').forEach(function(node) {
                bits.push(node.textContent || '');
            });
            hay = bits.join(' ').toLowerCase();
            card.setAttribute('data-nav-search-text', hay);
        }

        var matched = tokens.every(function(tok) { return hay.indexOf(tok) !== -1; });
        card.classList.toggle('nav-search-local-hidden', !matched);
    });

    if (document.getElementById('weeklyMain')) {
        reapplyFading();
        if (typeof TRIAGE_ACTIVE !== 'undefined' && TRIAGE_ACTIVE) {
            applyTriageMode();
        }
    }
}

function initTopicQuickSearch() {
    syncStickyNavHeight();

    var input = document.getElementById('topicQuickSearch');
    var wrap = document.getElementById('topicQuickSearchWrap');
    var panel = document.getElementById('topicQuickSearchResults');
    var createBtn = document.getElementById('topicQuickCreateBtn');
    if (!input || !wrap || !panel) return;

    function syncCreateButton() {
        if (!createBtn) return;
        createBtn.classList.toggle('active', input.value.trim().length >= 2);
    }

    function closePanel() {
        clearTimeout(TOPIC_SEARCH_STATE.timer);
        _abortTopicSearchController(TOPIC_SEARCH_STATE);
        panel.classList.remove('active');
        panel.innerHTML = '';
        TOPIC_SEARCH_STATE.items = [];
        TOPIC_SEARCH_STATE.activeIndex = -1;
    }

    function renderResults(items, query) {
        TOPIC_SEARCH_STATE.items = items || [];
        TOPIC_SEARCH_STATE.activeIndex = -1;

        var searchAction = (
            '<a href="/topics?q=' + encodeURIComponent(query || '') + '" class="nav-search-item" data-idx="-1">' +
                '<div class="nav-search-row">' +
                    '<span class="nav-search-name">Search all topics for &quot;' + _topicSearchEsc(query || '') + '&quot;</span>' +
                    '<span class="nav-search-meta">Results</span>' +
                '</div>' +
            '</a>'
        );

        if (!items || !items.length) {
            panel.innerHTML = searchAction + '<div class="nav-search-empty">No matching active topics. You can create a new one if needed.</div>';
            panel.classList.add('active');
            return;
        }

        panel.innerHTML = searchAction + items.map(function(t, idx) {
            var stateCls = t.topic_state === 'candidate' ? 'candidate' : 'active';
            var stateLabel = t.topic_state === 'candidate' ? 'Candidate' : 'Active';
            var meta = (t.post_count || 0) + ' posts';
            if (t.last_seen_at) meta += ' · ' + String(t.last_seen_at).slice(0, 10);
            return (
                '<a href="' + _topicSearchEsc(t.url) + '" class="nav-search-item" data-idx="' + idx + '">' +
                    '<div class="nav-search-row">' +
                        '<span class="topic-state-pill ' + stateCls + '">' + stateLabel + '</span>' +
                        '<span class="nav-search-name">' + _topicSearchEsc(t.name) + '</span>' +
                        '<span class="nav-search-meta">' + _topicSearchEsc(meta) + '</span>' +
                    '</div>' +
                    (t.description ? '<div class="nav-search-desc">' + _topicSearchEsc(t.description) + '</div>' : '') +
                '</a>'
            );
        }).join('');
        panel.classList.add('active');
    }

    function setActiveIndex(nextIdx) {
        var nodes = panel.querySelectorAll('.nav-search-item');
        if (!nodes.length) {
            TOPIC_SEARCH_STATE.activeIndex = -1;
            return;
        }
        if (nextIdx < 0) nextIdx = nodes.length - 1;
        if (nextIdx >= nodes.length) nextIdx = 0;
        TOPIC_SEARCH_STATE.activeIndex = nextIdx;
        nodes.forEach(function(n) { n.classList.remove('active'); });
        nodes[nextIdx].classList.add('active');
        nodes[nextIdx].scrollIntoView({ block: 'nearest' });
    }

    function runRemoteSearch() {
        var q = input.value.trim();
        if (q.length < 2) {
            closePanel();
            return;
        }
        var seq = ++TOPIC_SEARCH_STATE.seq;
        _fetchTopicSearchJson(q, 12, TOPIC_SEARCH_STATE)
            .then(function(data) {
                if (seq !== TOPIC_SEARCH_STATE.seq) return;
                renderResults((data && data.topics) || [], q);
            })
            .catch(function(err) {
                if (seq !== TOPIC_SEARCH_STATE.seq) return;
                if (err && err.name === 'AbortError') return;
                panel.innerHTML = '<div class="nav-search-empty">Search unavailable right now.</div>';
                panel.classList.add('active');
            });
    }

    function scheduleRemoteSearch(delayMs) {
        clearTimeout(TOPIC_SEARCH_STATE.timer);
        var q = input.value.trim();
        if (q.length < 2) {
            _abortTopicSearchController(TOPIC_SEARCH_STATE);
            closePanel();
            return;
        }
        TOPIC_SEARCH_STATE.timer = setTimeout(function() {
            runRemoteSearch();
        }, Math.max(0, delayMs || 0));
    }

    input.addEventListener('input', function() {
        clearTimeout(TOPIC_SEARCH_STATE.timer);
        applyLocalTopicCardFilter(input.value);
        syncCreateButton();
        closePanel();
        if (input.value.trim().length >= 2) {
            scheduleRemoteSearch(160);
        }
    });

    input.addEventListener('focus', function() {
        syncCreateButton();
        if (input.value.trim().length >= 2) {
            applyLocalTopicCardFilter(input.value);
            scheduleRemoteSearch(0);
        }
    });

    input.addEventListener('keydown', function(e) {
        var open = panel.classList.contains('active');
        if (e.key === 'ArrowDown' && open) {
            e.preventDefault();
            setActiveIndex(TOPIC_SEARCH_STATE.activeIndex + 1);
            return;
        }
        if (e.key === 'ArrowDown' && !open && input.value.trim().length >= 2) {
            e.preventDefault();
            clearTimeout(TOPIC_SEARCH_STATE.timer);
            runRemoteSearch();
            return;
        }
        if (e.key === 'ArrowUp' && open) {
            e.preventDefault();
            setActiveIndex(TOPIC_SEARCH_STATE.activeIndex - 1);
            return;
        }
        if (e.key === 'Escape') {
            closePanel();
            input.blur();
            return;
        }
        if (e.key === 'Enter') {
            var items = panel.querySelectorAll('.nav-search-item');
            if (items.length && TOPIC_SEARCH_STATE.activeIndex >= 0) {
                e.preventDefault();
                var idx = TOPIC_SEARCH_STATE.activeIndex;
                window.location.href = items[idx].getAttribute('href');
                return;
            }
            if (input.value.trim().length >= 2) {
                e.preventDefault();
                goToTopicSearchResults(input.value);
            }
        }
    });

    panel.addEventListener('mousedown', function(e) {
        // Prevent blur-before-click when choosing a result.
        e.preventDefault();
    });

    panel.addEventListener('click', function(e) {
        var link = e.target.closest('.nav-search-item');
        if (!link) return;
        window.location.href = link.getAttribute('href');
    });

    document.addEventListener('click', function(e) {
        if (!wrap.contains(e.target)) closePanel();
    });

    setTimeout(syncStickyNavHeight, 0);
    syncCreateButton();
    if (input.value.trim()) applyLocalTopicCardFilter(input.value);
}

initTopicQuickSearch();
initUiScaleControls();
initWeeklySidebarCollapse();
initTheme();

/* ── Export functions ── */
function showMd(topicId) {
    document.getElementById('mdOverlay').classList.add('active');
    document.getElementById('mdContent').textContent = 'Loading...';
    fetch('/api/topic/' + topicId + '/markdown')
        .then(function(r) { return r.text(); })
        .then(function(md) { document.getElementById('mdContent').textContent = md; });
}
function closeMd() {
    document.getElementById('mdOverlay').classList.remove('active');
}
function copyMd() {
    var text = document.getElementById('mdContent').textContent;
    var btn = document.getElementById('copyBtn');
    navigator.clipboard.writeText(text).then(function() {
        btn.textContent = 'Copied!';
        btn.style.background = 'var(--success)';
        document.getElementById('mdStatus').textContent = 'Markdown copied to clipboard';
        setTimeout(function() {
            btn.textContent = 'Copy to Clipboard';
            btn.style.background = '';
            document.getElementById('mdStatus').textContent = '';
        }, 2000);
    });
}
function quickCopy(topicId, btn) {
    var origText = btn.textContent;
    btn.textContent = '...';
    fetch('/api/topic/' + topicId + '/markdown')
        .then(function(r) { return r.text(); })
        .then(function(md) {
            navigator.clipboard.writeText(md).then(function() {
                btn.textContent = 'Copied!';
                btn.style.color = 'var(--success)';
                setTimeout(function() {
                    btn.textContent = origText;
                    btn.style.color = '';
                }, 1500);
            });
        })
        .catch(function() {
            btn.textContent = origText;
        });
}

function _isMobileSwipeVoteEnabled() {
    if (window.innerWidth > SWIPE_VOTE_MOBILE_MAX_WIDTH) return false;
    var coarse = false;
    try {
        coarse = !!(window.matchMedia && window.matchMedia('(pointer: coarse)').matches);
    } catch (e) {}
    if (!coarse) {
        var mtp = (typeof navigator !== 'undefined' && navigator && typeof navigator.maxTouchPoints === 'number')
            ? navigator.maxTouchPoints
            : 0;
        coarse = ('ontouchstart' in window) || mtp > 0;
    }
    return coarse;
}

function _isSwipeVoteTemporarilyDisabled() {
    return (typeof MERGE_MODE_ACTIVE !== 'undefined' && !!MERGE_MODE_ACTIVE);
}

function _isSwipeInteractiveStartTarget(node, container) {
    if (!node || !container) return false;
    var probe = node.nodeType === 1 ? node : node.parentElement;
    if (!probe || !probe.closest) return false;
    var interactive = probe.closest('button, input, textarea, select, summary, details, [role="button"], .section-action');
    if (!interactive) return false;
    if (interactive === container && !container.matches('button, input, textarea, select, summary, details, [role="button"]')) {
        return false;
    }
    return true;
}

function _findTouchById(touchList, id) {
    if (!touchList || !touchList.length) return null;
    for (var i = 0; i < touchList.length; i++) {
        if (touchList[i].identifier === id) return touchList[i];
    }
    return null;
}

function _getSwipeVoteFromDelta(dx, commit, bulletArmed) {
    var sidePx = commit ? SWIPE_VOTE_SIDE_COMMIT_PX : SWIPE_VOTE_SIDE_PREVIEW_PX;
    if (dx >= sidePx) return bulletArmed ? 'bullet' : 'slide';
    if (dx <= -sidePx) return 'skip';
    return '';
}

function _swipeVoteLabel(voteType) {
    if (voteType === 'slide') return 'Slide';
    if (voteType === 'skip') return 'Skip';
    if (voteType === 'bullet') return 'Bullet';
    return '';
}

function _clearSwipeVotePreviewClasses(targetEl) {
    if (!targetEl) return;
    targetEl.classList.remove('swipe-preview-slide');
    targetEl.classList.remove('swipe-preview-skip');
    targetEl.classList.remove('swipe-preview-bullet');
}

function _clearSwipeLongPressTimer(state) {
    if (!state || !state.longPressTimer) return;
    clearTimeout(state.longPressTimer);
    state.longPressTimer = null;
}

function _armSwipeBulletMode(targetEl, state) {
    if (!targetEl || !state) return;
    if (!state.active || state.dragging || state.bulletArmed) return;
    state.bulletArmed = true;
    targetEl.classList.add('swipe-bullet-armed');
    _setSwipeVotePreview(targetEl, 'bullet', 0.4);
    targetEl.setAttribute('data-swipe-preview-label', 'Bullet armed - swipe right');
}

function _setSwipeVotePreview(targetEl, voteType, progress) {
    if (!targetEl) return;
    _clearSwipeVotePreviewClasses(targetEl);
    targetEl.style.setProperty('--swipe-progress', String(Math.max(0, Math.min(1, progress || 0))));
    if (!voteType) {
        targetEl.removeAttribute('data-swipe-preview-label');
        return;
    }
    targetEl.classList.add('swipe-preview-' + voteType);
    targetEl.setAttribute('data-swipe-preview-label', _swipeVoteLabel(voteType));
}

function _findSwipeVoteButton(sourceEl, topicId, voteType) {
    if (!topicId || !voteType) return null;
    var selectors = [];
    if (sourceEl && sourceEl.matches && sourceEl.matches('.vote-row[data-topic-id]')) {
        selectors.push('.vote-btn[data-vote="' + voteType + '"]');
    }
    if (sourceEl) {
        selectors.push('.vote-row[data-topic-id="' + topicId + '"] .vote-btn[data-vote="' + voteType + '"]');
    }
    selectors.push('#weeklySectionsContainer .topic-card-sm[data-topic-id="' + topicId + '"] .vote-btn[data-vote="' + voteType + '"]');
    selectors.push('.topic-header .vote-row[data-topic-id="' + topicId + '"]:not(.vote-row-compact) .vote-btn[data-vote="' + voteType + '"]');
    selectors.push('.vote-row[data-topic-id="' + topicId + '"]:not(.vote-row-compact) .vote-btn[data-vote="' + voteType + '"]');

    for (var i = 0; i < selectors.length; i++) {
        var sel = selectors[i];
        if (!sel) continue;
        var btn = sourceEl && sourceEl.querySelector ? sourceEl.querySelector(sel) : null;
        if (!btn) btn = document.querySelector(sel);
        if (btn) return btn;
    }
    return null;
}

function handleSwipeVote(topicId, voteType, sourceEl) {
    if (!topicId || !voteType) return false;
    if (typeof castVote !== 'function') return false;
    if (typeof _isVoteRequestInFlight === 'function' && _isVoteRequestInFlight(topicId)) return false;
    var btn = _findSwipeVoteButton(sourceEl, topicId, voteType);
    if (!btn) return false;
    castVote(topicId, voteType, btn);
    return true;
}

function _detachSwipeHandlers(targetEl) {
    if (!targetEl || !targetEl.__xfiSwipeHandlers) return;
    var handlers = targetEl.__xfiSwipeHandlers;
    var state = targetEl.__xfiSwipeState;
    _clearSwipeLongPressTimer(state);
    targetEl.removeEventListener('touchstart', handlers.onStart);
    targetEl.removeEventListener('touchmove', handlers.onMove);
    targetEl.removeEventListener('touchend', handlers.onEnd);
    targetEl.removeEventListener('touchcancel', handlers.onCancel);
    targetEl.removeEventListener('click', handlers.onClickCapture, true);
    targetEl.classList.remove('swipe-vote-enabled');
    targetEl.classList.remove('swipe-dragging');
    targetEl.classList.remove('swipe-commit');
    targetEl.classList.remove('swipe-bullet-armed');
    _clearSwipeVotePreviewClasses(targetEl);
    targetEl.removeAttribute('data-swipe-preview-label');
    targetEl.style.removeProperty('--swipe-progress');
    targetEl.style.transform = '';
    targetEl.style.transition = '';
    delete targetEl.__xfiSwipeHandlers;
    delete targetEl.__xfiSwipeState;
    targetEl.removeAttribute('data-swipe-bound');
}

function _finishSwipeDrag(targetEl, state, animated) {
    if (!targetEl || !state) return;
    _clearSwipeLongPressTimer(state);
    state.active = false;
    state.dragging = false;
    state.axis = '';
    state.touchId = null;
    state.bulletArmed = false;
    if (state.resetTimer) {
        clearTimeout(state.resetTimer);
        state.resetTimer = null;
    }
    targetEl.classList.remove('swipe-dragging');
    targetEl.classList.remove('swipe-commit');
    targetEl.classList.remove('swipe-bullet-armed');
    _clearSwipeVotePreviewClasses(targetEl);
    targetEl.removeAttribute('data-swipe-preview-label');
    targetEl.style.removeProperty('--swipe-progress');
    if (!animated) {
        targetEl.style.transition = '';
        targetEl.style.transform = '';
        return;
    }
    targetEl.style.transition = 'transform 180ms cubic-bezier(0.22, 1, 0.36, 1), box-shadow 180ms ease';
    targetEl.style.transform = 'translate3d(0, 0, 0)';
    state.resetTimer = setTimeout(function() {
        targetEl.style.transition = '';
        targetEl.style.transform = '';
        state.resetTimer = null;
    }, 190);
}

function _attachSwipeHandlers(targetEl, topicId, context) {
    if (!targetEl || !topicId || targetEl.__xfiSwipeHandlers) return;
    targetEl.classList.add('swipe-vote-enabled');
    targetEl.setAttribute('data-swipe-bound', '1');
    targetEl.setAttribute('data-swipe-left-label', 'Skip');
    targetEl.setAttribute('data-swipe-right-label', 'Slide');
    var state = {
        topicId: topicId,
        context: context || '',
        active: false,
        dragging: false,
        axis: '',
        touchId: null,
        startX: 0,
        startY: 0,
        dx: 0,
        dy: 0,
        resetTimer: null,
        longPressTimer: null,
        bulletArmed: false
    };
    targetEl.__xfiSwipeState = state;

    function onStart(e) {
        if (!_isMobileSwipeVoteEnabled()) return;
        if (_isSwipeVoteTemporarilyDisabled()) return;
        if (!e.touches || e.touches.length !== 1) return;
        if (_isSwipeInteractiveStartTarget(e.target, targetEl)) return;
        if (typeof _isVoteRequestInFlight === 'function' && _isVoteRequestInFlight(topicId)) return;
        var t = e.touches[0];
        state.active = true;
        state.dragging = false;
        state.axis = '';
        state.touchId = t.identifier;
        state.startX = t.clientX;
        state.startY = t.clientY;
        state.dx = 0;
        state.dy = 0;
        state.bulletArmed = false;
        _clearSwipeLongPressTimer(state);
        targetEl.classList.remove('swipe-commit');
        targetEl.classList.remove('swipe-dragging');
        targetEl.classList.remove('swipe-bullet-armed');
        _clearSwipeVotePreviewClasses(targetEl);
        targetEl.removeAttribute('data-swipe-preview-label');
        targetEl.style.removeProperty('--swipe-progress');
        targetEl.style.transition = '';
        state.longPressTimer = setTimeout(function() {
            _armSwipeBulletMode(targetEl, state);
        }, SWIPE_VOTE_LONG_PRESS_MS);
    }

    function onMove(e) {
        if (!state.active) return;
        var t = _findTouchById(e.touches, state.touchId);
        if (!t) return;
        state.dx = t.clientX - state.startX;
        state.dy = t.clientY - state.startY;
        var absX = Math.abs(state.dx);
        var absY = Math.abs(state.dy);
        if (Math.max(absX, absY) > SWIPE_VOTE_LONG_PRESS_SLOP_PX) {
            _clearSwipeLongPressTimer(state);
        }
        if (!state.axis) {
            if (absX < SWIPE_VOTE_DRAG_START_PX && absY < SWIPE_VOTE_DRAG_START_PX) return;
            if (absY > absX * 1.15) {
                _finishSwipeDrag(targetEl, state, false);
                return;
            }
            state.axis = 'side';
        }
        state.dragging = true;
        e.preventDefault();

        var tx = 0;
        var ty = 0;
        var rot = 0;
        tx = Math.max(-130, Math.min(130, state.dx));
        ty = Math.max(-16, Math.min(16, state.dy * 0.16));
        if (state.context === 'card') rot = tx / 24;

        var previewVote = _getSwipeVoteFromDelta(state.dx, false, state.bulletArmed);
        var previewProgress = 0;
        if (previewVote === 'slide' || previewVote === 'bullet') previewProgress = state.dx / SWIPE_VOTE_SIDE_COMMIT_PX;
        else if (previewVote === 'skip') previewProgress = Math.abs(state.dx) / SWIPE_VOTE_SIDE_COMMIT_PX;
        if (!previewVote && state.bulletArmed) {
            _clearSwipeVotePreviewClasses(targetEl);
            targetEl.style.setProperty('--swipe-progress', '0');
            targetEl.setAttribute('data-swipe-preview-label', 'Bullet armed - swipe right');
        } else {
            _setSwipeVotePreview(targetEl, previewVote, previewProgress);
        }
        targetEl.classList.add('swipe-dragging');
        targetEl.style.transform = 'translate3d(' + tx + 'px, ' + ty + 'px, 0) rotate(' + rot + 'deg)';
    }

    function onEnd(e) {
        if (!state.active) return;
        _clearSwipeLongPressTimer(state);
        var t = _findTouchById(e.changedTouches, state.touchId);
        if (t) {
            state.dx = t.clientX - state.startX;
            state.dy = t.clientY - state.startY;
        }
        var movedDist = Math.max(Math.abs(state.dx), Math.abs(state.dy));
        var voteType = _getSwipeVoteFromDelta(state.dx, true, state.bulletArmed);
        if (!state.dragging) {
            if (movedDist >= SWIPE_VOTE_CANCEL_TAP_PX) {
                targetEl.__xfiSwipeSuppressClickUntil = Date.now() + 320;
            }
            _finishSwipeDrag(targetEl, state, movedDist >= SWIPE_VOTE_CANCEL_TAP_PX);
            return;
        }
        e.preventDefault();
        if (!voteType) {
            targetEl.__xfiSwipeSuppressClickUntil = Date.now() + 360;
            _finishSwipeDrag(targetEl, state, true);
            return;
        }

        var tx = (voteType === 'skip') ? -96 : 96;
        var ty = 0;
        targetEl.classList.add('swipe-commit');
        _setSwipeVotePreview(targetEl, voteType, 1);
        targetEl.setAttribute('data-swipe-preview-label', _swipeVoteLabel(voteType) + ' saved');
        targetEl.style.transition = 'transform 130ms cubic-bezier(0.16, 1, 0.3, 1), box-shadow 130ms ease';
        targetEl.style.transform = 'translate3d(' + tx + 'px, ' + ty + 'px, 0)';
        targetEl.__xfiSwipeSuppressClickUntil = Date.now() + 460;
        handleSwipeVote(topicId, voteType, targetEl);
        setTimeout(function() {
            _finishSwipeDrag(targetEl, state, true);
        }, 130);
    }

    function onCancel() {
        if (!state.active) return;
        _finishSwipeDrag(targetEl, state, false);
    }

    function onClickCapture(e) {
        var suppressUntil = targetEl.__xfiSwipeSuppressClickUntil || 0;
        if (suppressUntil > Date.now()) {
            e.preventDefault();
            e.stopPropagation();
        }
    }

    targetEl.__xfiSwipeHandlers = {
        onStart: onStart,
        onMove: onMove,
        onEnd: onEnd,
        onCancel: onCancel,
        onClickCapture: onClickCapture
    };
    targetEl.addEventListener('touchstart', onStart, { passive: true });
    targetEl.addEventListener('touchmove', onMove, { passive: false });
    targetEl.addEventListener('touchend', onEnd, { passive: false });
    targetEl.addEventListener('touchcancel', onCancel, { passive: true });
    targetEl.addEventListener('click', onClickCapture, true);
}

function _collectSwipeTargets(scope) {
    var root = (scope && scope.querySelectorAll) ? scope : document;
    var targets = [];

    var weeklyContainer = null;
    if (root.id === 'weeklySectionsContainer') {
        weeklyContainer = root;
    } else {
        weeklyContainer = root.querySelector('#weeklySectionsContainer');
    }
    if (weeklyContainer) {
        weeklyContainer.querySelectorAll('.topic-card-sm[data-topic-id]').forEach(function(card) {
            if (card.closest('#reportedGrid')) return;
            if (card.closest('#triageCenterCol')) return;
            var topicId = parseInt(card.getAttribute('data-topic-id') || '0', 10);
            if (!topicId) return;
            targets.push({ el: card, topicId: topicId, context: 'card' });
        });
    }

    root.querySelectorAll('.topic-header .vote-row[data-topic-id]:not(.vote-row-compact)').forEach(function(row) {
        var topicId = parseInt(row.getAttribute('data-topic-id') || '0', 10);
        if (!topicId) return;
        targets.push({ el: row, topicId: topicId, context: 'row' });
    });

    return targets;
}

function bindSwipeToVoteTargets(scope) {
    if (!_isMobileSwipeVoteEnabled()) return;
    if (_isSwipeVoteTemporarilyDisabled()) return;
    var targets = _collectSwipeTargets(scope);
    targets.forEach(function(entry) {
        _attachSwipeHandlers(entry.el, entry.topicId, entry.context);
    });
}

function _clearSwipeBindings(scope) {
    var root = (scope && scope.querySelectorAll) ? scope : document;
    root.querySelectorAll('[data-swipe-bound="1"]').forEach(function(el) {
        _detachSwipeHandlers(el);
    });
}

function _dismissSwipeVoteHint() {
    if (!_SWIPE_VOTE_HINT_EL) return;
    var el = _SWIPE_VOTE_HINT_EL;
    _SWIPE_VOTE_HINT_EL = null;
    el.classList.remove('visible');
    setTimeout(function() {
        if (el.parentNode) el.parentNode.removeChild(el);
    }, 220);
}

function showSwipeOnboardingIfNeeded() {
    if (!_isMobileSwipeVoteEnabled()) return;
    if (_isSwipeVoteTemporarilyDisabled()) return;
    if (_SWIPE_VOTE_HINT_SHOWN_IN_PAGE) return;
    if (!document.querySelector('[data-swipe-bound="1"]')) return;
    try {
        if (localStorage.getItem(SWIPE_VOTE_HINT_KEY) === '1') return;
        localStorage.setItem(SWIPE_VOTE_HINT_KEY, '1');
    } catch (e) {}

    var host = document.getElementById('weeklyMain') ||
        document.querySelector('.topic-header') ||
        document.querySelector('.main-wide') ||
        document.querySelector('.main');
    if (!host) return;

    var hint = document.createElement('div');
    hint.className = 'swipe-vote-hint';
    hint.innerHTML = '<span class="swipe-vote-hint-text">Swipe right to Slide, left to Skip, hold then swipe right for Bullet</span>' +
        '<button type="button" class="swipe-vote-hint-close" aria-label="Dismiss swipe hint">&times;</button>';
    if (host.firstChild) host.insertBefore(hint, host.firstChild);
    else host.appendChild(hint);
    _SWIPE_VOTE_HINT_EL = hint;
    _SWIPE_VOTE_HINT_SHOWN_IN_PAGE = true;
    setTimeout(function() {
        if (!_SWIPE_VOTE_HINT_EL) return;
        _SWIPE_VOTE_HINT_EL.classList.add('visible');
    }, 20);
    var closeBtn = hint.querySelector('.swipe-vote-hint-close');
    if (closeBtn) {
        closeBtn.addEventListener('click', function(e) {
            e.preventDefault();
            _dismissSwipeVoteHint();
        });
    }
    setTimeout(_dismissSwipeVoteHint, 7000);
}

function refreshMobileSwipeVoting(scope) {
    if (!_isMobileSwipeVoteEnabled() || _isSwipeVoteTemporarilyDisabled()) {
        _clearSwipeBindings(document);
        _dismissSwipeVoteHint();
        return;
    }
    bindSwipeToVoteTargets(scope || document);
    showSwipeOnboardingIfNeeded();
}

function initMobileSwipeVoting() {
    refreshMobileSwipeVoting(document);
}

/* ── Voting / Triage ── */

// Current user from session (set by server, not localStorage)
var CURRENT_USER = (window.XFI_BOOTSTRAP && window.XFI_BOOTSTRAP.currentUser) || "";
var SKIP_REASON_NOT_GOOD_FIT = 'not_good_fit';
var SKIP_REASON_ALREADY_COVERED = 'already_covered';
var _MANUAL_TRIAGE_KEY = CURRENT_USER ? ('xfi_manual_triage_' + CURRENT_USER.replace(/\s+/g, '_')) : null;
initMobileSwipeVoting();

function _normalizeSkipReason(reason) {
    var txt = String(reason || '').trim().toLowerCase();
    if (txt === SKIP_REASON_ALREADY_COVERED) return SKIP_REASON_ALREADY_COVERED;
    if (txt === SKIP_REASON_NOT_GOOD_FIT) return SKIP_REASON_NOT_GOOD_FIT;
    return SKIP_REASON_NOT_GOOD_FIT;
}

function _voteMatchesButton(vote, voteType, buttonSkipReason) {
    if (!vote || String(vote.vote_type || '') !== String(voteType || '')) return false;
    if (String(voteType || '') !== 'skip') return true;
    return _normalizeSkipReason(vote.skip_reason) === _normalizeSkipReason(buttonSkipReason);
}

function _ensureSkipReasonButtons(scope) {
    var root = (scope && scope.querySelectorAll) ? scope : document;
    root.querySelectorAll('.vote-row[data-topic-id]').forEach(function(row) {
        var skipButtons = row.querySelectorAll('.vote-btn[data-vote="skip"]');
        if (!skipButtons.length) return;

        var defaultSkipBtn = null;
        var coveredSkipBtn = null;
        skipButtons.forEach(function(btn) {
            var reason = _normalizeSkipReason(btn.getAttribute('data-vote-skip-reason'));
            if (reason === SKIP_REASON_ALREADY_COVERED && btn.hasAttribute('data-vote-skip-reason')) {
                coveredSkipBtn = btn;
                return;
            }
            if (!defaultSkipBtn) defaultSkipBtn = btn;
        });
        if (!defaultSkipBtn) return;

        defaultSkipBtn.setAttribute('data-vote-skip-reason', SKIP_REASON_NOT_GOOD_FIT);

        if (coveredSkipBtn) {
            coveredSkipBtn.classList.add('vote-btn-skip-covered');
            coveredSkipBtn.setAttribute('type', 'button');
            return;
        }

        var topicId = parseInt(row.getAttribute('data-topic-id') || '0', 10);
        if (!topicId) return;
        var isCompact = row.classList.contains('vote-row-compact');

        coveredSkipBtn = document.createElement('button');
        coveredSkipBtn.type = 'button';
        coveredSkipBtn.className = 'vote-btn vote-btn-skip-covered';
        coveredSkipBtn.setAttribute('data-vote', 'skip');
        coveredSkipBtn.setAttribute('data-vote-skip-reason', SKIP_REASON_ALREADY_COVERED);
        coveredSkipBtn.textContent = isCompact ? 'C' : 'Covered';
        coveredSkipBtn.title = 'Skip (Already Covered)';
        coveredSkipBtn.addEventListener('click', function(e) {
            e.preventDefault();
            e.stopPropagation();
            castVote(topicId, 'skip', coveredSkipBtn);
        });

        if (defaultSkipBtn.nextSibling) {
            defaultSkipBtn.parentNode.insertBefore(coveredSkipBtn, defaultSkipBtn.nextSibling);
        } else {
            defaultSkipBtn.parentNode.appendChild(coveredSkipBtn);
        }
    });
}

function _getManualTriageActions() {
    if (!_MANUAL_TRIAGE_KEY) return {};
    try {
        var raw = localStorage.getItem(_MANUAL_TRIAGE_KEY);
        if (!raw) return {};
        var parsed = JSON.parse(raw);
        return (parsed && typeof parsed === 'object') ? parsed : {};
    } catch (e) {
        return {};
    }
}

function _markManualTriageAction(topicId, action) {
    if (!_MANUAL_TRIAGE_KEY || !topicId) return;
    var data = _getManualTriageActions();
    data[String(topicId)] = { action: action || 'manual', at: new Date().toISOString() };
    try { localStorage.setItem(_MANUAL_TRIAGE_KEY, JSON.stringify(data)); } catch (e) {}
}

// Initialize on page load
(function() {
    highlightMyVotes();
    applyVoteConsensusColors();
    if (CURRENT_USER) logImpressions(CURRENT_USER);
})();

function highlightMyVotes() {
    if (typeof INITIAL_VOTES === 'undefined') return;
    _ensureSkipReasonButtons(document);
    var seen = {};
    document.querySelectorAll('.vote-row[data-topic-id]').forEach(function(row) {
        var topicId = parseInt(row.getAttribute('data-topic-id') || '0', 10);
        if (!topicId || seen[topicId]) return;
        seen[topicId] = true;
        updateVoteRow(topicId, INITIAL_VOTES[topicId] || []);
    });
}

function applyVoteConsensusColors() {
    if (typeof INITIAL_VOTES === 'undefined') return;
    var cardClasses = [
        'vote-slide-1','vote-slide-2','vote-slide-3',
        'vote-bullet-1','vote-bullet-2','vote-bullet-3',
        'vote-skip-1','vote-skip-2','vote-skip-3'
    ];
    var btnClasses = [
        'vote-btn-slide-1','vote-btn-slide-2','vote-btn-slide-3',
        'vote-btn-bullet-1','vote-btn-bullet-2','vote-btn-bullet-3'
    ];
    // Only apply to main weekly grid cards, NOT triage clones
    var cards = document.querySelectorAll('#weeklyMain .topic-card-sm, .topic-grid-dense .topic-card-sm');
    cards.forEach(function(card) {
        if (card.closest('#reportedGrid')) return;
        if (card.closest('#triageCenterCol')) return;
        cardClasses.forEach(function(c) { card.classList.remove(c); });
        // Clean vote button tier classes
        card.querySelectorAll('.vote-btn').forEach(function(b) {
            btnClasses.forEach(function(c) { b.classList.remove(c); });
        });
        var topicId = parseInt(card.getAttribute('data-topic-id'));
        if (!topicId) {
            var row = card.querySelector('.vote-row[data-topic-id]');
            if (row) topicId = parseInt(row.getAttribute('data-topic-id'));
        }
        if (!topicId) return;
        var topicVotes = INITIAL_VOTES[topicId] || [];
        // Auto-ranked unvoted treatment: red border + label when no votes cast
        var selReason = card.getAttribute('data-selection-reason') || '';
        if (selReason === 'auto_promoted_rank') {
            if (!topicVotes.length) {
                card.classList.add('auto-ranked-unvoted');
            } else {
                card.classList.remove('auto-ranked-unvoted');
            }
        }
        if (!topicVotes.length) return;
        // Don't override flagged cards
        if (card.classList.contains('flagged')) return;
        var slideCount = 0, bulletCount = 0, skipCount = 0;
        topicVotes.forEach(function(v) {
            if (v.vote_type === 'slide') slideCount++;
            else if (v.vote_type === 'bullet') bulletCount++;
            else if (v.vote_type === 'skip') skipCount++;
        });
        // Card border — section determines palette, votes determine tier
        var section = card.getAttribute('data-weekly-section') || '';
        var cardType, cardTier;
        if (section === 'slide') {
            // Slide section: use slide vote count (warm)
            if (slideCount > 0) { cardType = 'slide'; cardTier = Math.min(slideCount, 3); }
            else if (skipCount > 0 && bulletCount === 0) { cardType = 'skip'; cardTier = Math.min(skipCount, 3); }
        } else if (section === 'bullet') {
            // Bullet section: use bullet vote count (cool)
            if (bulletCount > 0) { cardType = 'bullet'; cardTier = Math.min(bulletCount, 3); }
            else if (skipCount > 0 && slideCount === 0) { cardType = 'skip'; cardTier = Math.min(skipCount, 3); }
        } else {
            // Candidate / deprioritized: dominant vote type wins
            var dominant = 'skip';
            if (slideCount >= bulletCount && slideCount >= skipCount && slideCount > 0) dominant = 'slide';
            else if (bulletCount >= slideCount && bulletCount >= skipCount && bulletCount > 0) dominant = 'bullet';
            var count = dominant === 'slide' ? slideCount : dominant === 'bullet' ? bulletCount : skipCount;
            if (count > 0) { cardType = dominant; cardTier = Math.min(count, 3); }
        }
        if (cardType && cardTier) card.classList.add('vote-' + cardType + '-' + cardTier);
        // Vote button tiers — visible to all viewers in every section
        if (slideCount > 0) {
            var slideBtn = card.querySelector('.vote-btn[data-vote="slide"]');
            if (slideBtn) slideBtn.classList.add('vote-btn-slide-' + Math.min(slideCount, 3));
        }
        if (bulletCount > 0) {
            var bulletBtn = card.querySelector('.vote-btn[data-vote="bullet"]');
            if (bulletBtn) bulletBtn.classList.add('vote-btn-bullet-' + Math.min(bulletCount, 3));
        }
    });
}

function castVote(topicId, voteType, btn, opts) {
    var voter = CURRENT_USER;
    if (!voter) return;
    if (_isVoteRequestInFlight(topicId)) return;

    var skipReason = null;
    if (voteType === 'skip') {
        skipReason = _normalizeSkipReason(
            (opts && opts.skipReason) || (btn && btn.getAttribute('data-vote-skip-reason'))
        );
    }

    var isActive = !!(btn && btn.classList && btn.classList.contains('active'));
    var method = isActive ? 'DELETE' : 'POST';
    var payload = { topic_id: topicId, vote_type: voteType };
    if (voteType === 'skip') {
        payload.skip_reason = skipReason;
    }
    var body = isActive
        ? JSON.stringify({ topic_id: topicId })
        : JSON.stringify(payload);
    var prevVotes = _getTopicVotesLocal(topicId);
    var optimisticVotes = _computeOptimisticVotes(topicId, voteType, isActive, voter, skipReason);
    var reqToken = _beginVoteRequest(topicId);
    var moveSnapshot = null;

    _setVoteRowBusy(topicId, true);
    _setTopicVotesLocal(topicId, optimisticVotes);
    updateVoteRow(topicId, optimisticVotes);
    if (_isWeeklyPage()) {
        moveSnapshot = _optimisticRepositionWeeklyCard(topicId, optimisticVotes);
    }
    applyVoteConsensusColors();
    if (!isActive && (voteType === 'slide' || voteType === 'bullet')) {
        _markManualTriageAction(topicId, voteType);
    }
    if (typeof TRIAGE_ACTIVE !== 'undefined' && TRIAGE_ACTIVE) {
        applyTriageMode();
    }

    fetch('/api/vote', {
        method: method,
        headers: { 'Content-Type': 'application/json' },
        body: body
    })
    .then(function(r) {
        return r.json().catch(function() { return {}; }).then(function(data) {
            return { ok: r.ok, data: data || {} };
        });
    })
    .then(function(resp) {
        if (!_isCurrentVoteRequest(topicId, reqToken)) return;
        if (!resp.ok || !resp.data || !resp.data.ok) {
            throw new Error((resp.data && resp.data.error) || 'Vote request failed');
        }
        _setTopicVotesLocal(topicId, resp.data.votes || []);
        updateVoteRow(topicId, resp.data.votes || []);
        applyVoteConsensusColors();
        if (typeof TRIAGE_ACTIVE !== 'undefined' && TRIAGE_ACTIVE) {
            applyTriageMode();
        }
        if (document.getElementById('weeklyLayout')) {
            _queueWeeklySectionsReconcile('vote:' + voteType);
        }
    })
    .catch(function() {
        if (!_isCurrentVoteRequest(topicId, reqToken)) return;
        _setTopicVotesLocal(topicId, prevVotes);
        _restoreOptimisticWeeklyCardPosition(moveSnapshot);
        updateVoteRow(topicId, prevVotes);
        applyVoteConsensusColors();
        if (typeof TRIAGE_ACTIVE !== 'undefined' && TRIAGE_ACTIVE) {
            applyTriageMode();
        }
        _flashVoteBtnError(btn);
    })
    .finally(function() {
        if (!_isCurrentVoteRequest(topicId, reqToken)) return;
        _endVoteRequest(topicId, reqToken);
        _setVoteRowBusy(topicId, false);
    });
}

function updateVoteRow(topicId, votes) {
    _ensureSkipReasonButtons(document);
    // Handle multiple rows per topic (main grid + sidebar may both show same topic)
    var rows = document.querySelectorAll('.vote-row[data-topic-id="' + topicId + '"]');
    if (!rows.length) return;
    var voter = CURRENT_USER;
    var types = ['slide', 'bullet', 'skip', 'unsure', 'flag'];
    var fullLabels = { slide: 'Slide', bullet: 'Bullet', skip: 'Skip', unsure: '?', flag: '\u26A0' };
    var compactLabels = { slide: 'S', bullet: 'B', skip: 'K', unsure: '?', flag: '\u26A0' };

    rows.forEach(function(row) {
        var isCompact = row.classList.contains('vote-row-compact');
        var labels = isCompact ? compactLabels : fullLabels;

        types.forEach(function(vt) {
            var btns = row.querySelectorAll('.vote-btn[data-vote="' + vt + '"]');
            if (!btns.length) return;

            btns.forEach(function(btn) {
                var buttonSkipReason = btn.getAttribute('data-vote-skip-reason');
                var votersForType = votes.filter(function(v) {
                    return _voteMatchesButton(v, vt, buttonSkipReason);
                });
                var count = votersForType.length;
                var names = votersForType.map(function(v) { return v.voter_name; });
                var isMyVote = voter && names.indexOf(voter) !== -1;
                var label = labels[vt];
                var titleLabel = fullLabels[vt];

                if (vt === 'skip' && _normalizeSkipReason(buttonSkipReason) === SKIP_REASON_ALREADY_COVERED) {
                    label = isCompact ? 'C' : 'Covered';
                    titleLabel = 'Skip (Already Covered)';
                } else if (vt === 'skip') {
                    titleLabel = 'Skip (Not Good Fit)';
                }

                btn.innerHTML = label + (count ? '<span class="vote-count">' + count + '</span>' : '');
                btn.title = titleLabel + (names.length ? ' \u00b7 ' + names.join(', ') : '');

                if (isMyVote) {
                    btn.classList.add('active');
                } else {
                    btn.classList.remove('active');
                }
            });
        });
    });
}

var _voteRequestSeq = 0;
var _voteRequestByTopic = {};
var _weeklyReconcileTimer = null;
var _weeklyReconcileInFlight = false;
var _weeklyReconcileRerun = false;

function _isWeeklyPage() {
    return !!document.getElementById('weeklyLayout');
}

function _ensureVotesStore() {
    if (typeof window.INITIAL_VOTES === 'undefined' || !window.INITIAL_VOTES) {
        window.INITIAL_VOTES = {};
    }
    return window.INITIAL_VOTES;
}

function _cloneVotes(votes) {
    if (!Array.isArray(votes)) return [];
    return votes.map(function(v) {
        var out = {};
        if (!v || typeof v !== 'object') return out;
        Object.keys(v).forEach(function(k) { out[k] = v[k]; });
        return out;
    });
}

function _getTopicVotesLocal(topicId) {
    var store = _ensureVotesStore();
    return _cloneVotes(store[topicId] || []);
}

function _setTopicVotesLocal(topicId, votes) {
    var store = _ensureVotesStore();
    store[topicId] = _cloneVotes(votes || []);
}

function _nowVoteIso() {
    return new Date().toISOString().slice(0, 19).replace('T', ' ');
}

function _computeOptimisticVotes(topicId, voteType, isDelete, voterName, skipReason) {
    var prev = _getTopicVotesLocal(topicId);
    var next = prev.filter(function(v) { return v && v.voter_name !== voterName; });
    if (!isDelete) {
        var vote = {
            topic_id: topicId,
            voter_name: voterName,
            vote_type: voteType,
            voted_at: _nowVoteIso(),
        };
        if (voteType === 'skip') {
            vote.skip_reason = _normalizeSkipReason(skipReason);
        }
        next.push(vote);
    }
    return next;
}

function _deriveOptimisticWeeklySection(votes) {
    if (!Array.isArray(votes) || !votes.length) return null;
    var typeSet = {};
    votes.forEach(function(v) {
        var vt = String((v && v.vote_type) || '').toLowerCase();
        if (vt) typeSet[vt] = true;
    });
    if (typeSet.slide) return 'slide';
    if (typeSet.bullet) return 'bullet';
    if (typeSet.unsure && !typeSet.slide && !typeSet.bullet) return 'unsure';
    if (!typeSet.skip) return null;
    if (typeSet.unsure || typeSet.slide || typeSet.bullet) return null;
    var keys = Object.keys(typeSet);
    var skipOnly = keys.every(function(k) { return k === 'skip' || k === 'flag'; });
    return skipOnly ? 'deprioritized' : null;
}

function _weeklyGridForSection(section) {
    if (section === 'slide') return document.getElementById('weeklySlideGrid');
    if (section === 'bullet') return document.getElementById('weeklyBulletGrid');
    if (section === 'unsure') return document.getElementById('weeklyUnsureGrid');
    if (section === 'candidate') return document.getElementById('weeklyCandidateGrid');
    if (section === 'deprioritized') return document.getElementById('weeklyDeprioritizedGrid');
    return null;
}

function _getWeeklyMainCardForTopic(topicId) {
    var cards = document.querySelectorAll('#weeklySectionsContainer .topic-card-sm[data-topic-id="' + topicId + '"]');
    for (var i = 0; i < cards.length; i++) {
        var card = cards[i];
        if (card.closest('#reportedGrid')) continue;
        return card;
    }
    return null;
}

function _optimisticRepositionWeeklyCard(topicId, votes) {
    if (!_isWeeklyPage()) return null;
    var targetSection = _deriveOptimisticWeeklySection(votes);
    if (!targetSection) return null;

    var card = _getWeeklyMainCardForTopic(topicId);
    if (!card) return null;
    var targetGrid = _weeklyGridForSection(targetSection);
    if (!targetGrid) return null; // section may not be rendered when currently empty

    var parent = card.parentElement;
    if (!parent) return null;
    var nextSibling = card.nextSibling;
    var prevSection = card.getAttribute('data-weekly-section') || '';

    // If already first in target section with matching section attr, no-op.
    if (parent === targetGrid && targetGrid.firstElementChild === card && prevSection === targetSection) {
        return {
            card: card,
            parent: parent,
            nextSibling: nextSibling,
            prevSection: prevSection,
            moved: false,
        };
    }

    card.setAttribute('data-weekly-section', targetSection);
    card.classList.add('vote-pending-move');
    targetGrid.insertBefore(card, targetGrid.firstElementChild || null);

    if (typeof TRIAGE_ACTIVE !== 'undefined' && TRIAGE_ACTIVE) {
        applyTriageMode();
    }
    requestAnimationFrame(function() { reapplyFading(); });

    return {
        card: card,
        parent: parent,
        nextSibling: nextSibling,
        prevSection: prevSection,
        moved: true,
    };
}

function _restoreOptimisticWeeklyCardPosition(snapshot) {
    if (!snapshot || !snapshot.card || !snapshot.moved) return;
    var card = snapshot.card;
    if (snapshot.prevSection != null) {
        card.setAttribute('data-weekly-section', snapshot.prevSection);
    }
    if (snapshot.parent) {
        if (snapshot.nextSibling && snapshot.nextSibling.parentNode === snapshot.parent) {
            snapshot.parent.insertBefore(card, snapshot.nextSibling);
        } else {
            snapshot.parent.appendChild(card);
        }
    }
    card.classList.remove('vote-pending-move');
    if (typeof TRIAGE_ACTIVE !== 'undefined' && TRIAGE_ACTIVE) {
        applyTriageMode();
    }
    requestAnimationFrame(function() { reapplyFading(); });
}

function _isVoteRequestInFlight(topicId) {
    return !!_voteRequestByTopic[String(topicId)];
}

function _beginVoteRequest(topicId) {
    var token = ++_voteRequestSeq;
    _voteRequestByTopic[String(topicId)] = token;
    return token;
}

function _isCurrentVoteRequest(topicId, token) {
    return _voteRequestByTopic[String(topicId)] === token;
}

function _endVoteRequest(topicId, token) {
    if (_isCurrentVoteRequest(topicId, token)) {
        delete _voteRequestByTopic[String(topicId)];
    }
}

function _setVoteRowBusy(topicId, busy) {
    var rows = document.querySelectorAll('.vote-row[data-topic-id="' + topicId + '"]');
    rows.forEach(function(row) {
        row.setAttribute('data-busy', busy ? '1' : '0');
        row.querySelectorAll('.vote-btn[data-vote]').forEach(function(btn) {
            var vt = btn.getAttribute('data-vote');
            if (!vt) return; // skip copy/edit buttons that use empty data-vote
            btn.disabled = !!busy;
        });
    });
}

function _flashVoteBtnError(btn) {
    if (!btn) return;
    btn.classList.add('vote-btn-error');
    var prevStyle = btn.style.color;
    btn.style.color = '#e87474';
    setTimeout(function() {
        btn.classList.remove('vote-btn-error');
        btn.style.color = prevStyle;
    }, 900);
}

function _getWeeklySortModeForRefresh() {
    var sel = document.querySelector('#weeklyMainToolbar .sort-select');
    if (sel && sel.value) return sel.value;
    try {
        var params = new URLSearchParams(window.location.search || '');
        var v = params.get('wsort');
        return v || 'ranked';
    } catch (e) {
        return 'ranked';
    }
}

function _getActiveWeeklyVoteFilter() {
    var active = document.querySelector('#weeklyMainToolbar .vote-filter-btn.active:not(#triageModeBtn)');
    if (!active) return 'all';
    var onclick = active.getAttribute('onclick') || '';
    var m = onclick.match(/filterByVote\('([^']+)'/);
    if (m && m[1]) return m[1];
    var txt = (active.textContent || '').trim().toLowerCase();
    if (txt === 'slide' || txt === 'bullet' || txt === 'skip' || txt === 'flagged' || txt === 'unvoted') {
        return txt === 'flagged' ? 'flag' : txt;
    }
    return 'all';
}

function _restoreWeeklyUiStateAfterPatch(state) {
    if (!_isWeeklyPage()) return;

    if (typeof highlightMyVotes === 'function') {
        highlightMyVotes();
    }
    applyVoteConsensusColors();

    if (state && state.voteFilter) {
        var btn = Array.prototype.slice.call(document.querySelectorAll('#weeklyMainToolbar .vote-filter-btn'))
            .find(function(b) {
                if (b.id === 'triageModeBtn') return false;
                var onclick = b.getAttribute('onclick') || '';
                return onclick.indexOf("filterByVote('" + state.voteFilter + "'") !== -1;
            });
        if (btn) {
            filterByVote(state.voteFilter, btn);
        }
    } else {
        reapplyFading();
    }

    if (typeof TRIAGE_ACTIVE !== 'undefined' && TRIAGE_ACTIVE) {
        applyTriageMode();
    }

    if (typeof MERGE_MODE_ACTIVE !== 'undefined' && MERGE_MODE_ACTIVE) {
        var weeklyMain = document.getElementById('weeklyMain');
        if (weeklyMain) {
            weeklyMain.classList.add('merge-mode');
            weeklyMain.querySelectorAll('.topic-card-sm').forEach(function(card) {
                card.removeEventListener('click', mergeCardClickHandler);
                card.addEventListener('click', mergeCardClickHandler);
                var tid = parseInt(card.getAttribute('data-topic-id') || '0');
                card.classList.toggle('merge-selected', MERGE_SELECTED_IDS.indexOf(tid) !== -1);
            });
        }
        if (typeof updateMergeActionBar === 'function') updateMergeActionBar();
    }

    // Re-apply collapse state for collapsible sections after DOM replacement.
    if (typeof initBulletCollapse === 'function') initBulletCollapse();
    if (typeof initUnsureCollapse === 'function') initUnsureCollapse();

    requestAnimationFrame(function() { reapplyFading(); });
    setTimeout(function() { reapplyFading(); }, 90);

    if (typeof _topicSearchQueryValue === 'function') {
        var q = _topicSearchQueryValue();
        if (q && q.length >= 2) {
            applyLocalTopicCardFilter(q);
        }
    }

    if (typeof refreshMobileSwipeVoting === 'function') {
        var swipeScope = document.getElementById('weeklySectionsContainer') || document;
        refreshMobileSwipeVoting(swipeScope);
    }
}

function _refreshWeeklySectionsPartial() {
    if (!_isWeeklyPage()) return Promise.resolve();
    var container = document.getElementById('weeklySectionsContainer');
    if (!container) return Promise.resolve();

    var uiState = {
        voteFilter: _getActiveWeeklyVoteFilter(),
    };
    var wsort = _getWeeklySortModeForRefresh();
    var url = '/api/weekly/sections?wsort=' + encodeURIComponent(wsort || 'ranked');

    return fetch(url, { headers: { 'X-Requested-With': 'XMLHttpRequest' } })
        .then(function(r) {
            return r.json().catch(function() { return {}; }).then(function(data) {
                return { ok: r.ok, data: data || {} };
            });
        })
        .then(function(resp) {
            if (!resp.ok || !resp.data || !resp.data.ok) {
                throw new Error((resp.data && resp.data.error) || 'Weekly refresh failed');
            }
            if (resp.data.votes) {
                window.INITIAL_VOTES = resp.data.votes;
            } else {
                _ensureVotesStore();
            }
            container.innerHTML = resp.data.html || '';
            _restoreWeeklyUiStateAfterPatch(uiState);
        });
}

function _queueWeeklySectionsReconcile(reason) {
    if (!_isWeeklyPage()) return;
    if (_weeklyReconcileTimer) clearTimeout(_weeklyReconcileTimer);
    _weeklyReconcileTimer = setTimeout(function() {
        _weeklyReconcileTimer = null;
        if (_weeklyReconcileInFlight) {
            _weeklyReconcileRerun = true;
            return;
        }
        _weeklyReconcileInFlight = true;
        _refreshWeeklySectionsPartial()
            .catch(function(err) {
                console.warn('Weekly partial refresh failed', reason || '', err);
            })
            .finally(function() {
                _weeklyReconcileInFlight = false;
                if (_weeklyReconcileRerun) {
                    _weeklyReconcileRerun = false;
                    _queueWeeklySectionsReconcile('rerun');
                }
            });
    }, 40);
}

/* ── Vote filter ── */
function filterByVote(filter, btn) {
    // Update active pill (but don't deactivate the triage toggle)
    document.querySelectorAll('.vote-filter-btn').forEach(function(b) {
        if (b.id !== 'triageModeBtn') b.classList.remove('active');
    });
    btn.classList.add('active');

    // Filter ALL cards in main grid — completely independent of triage mode
    // Triage center column stays untouched; a card can appear in both places
    var cards = document.querySelectorAll('#weeklyMain .topic-grid-dense .topic-card-sm');
    if (typeof INITIAL_VOTES === 'undefined') {
        cards.forEach(function(card) { card.style.display = ''; });
        return;
    }

    cards.forEach(function(card) {
        if (card.closest('#reportedGrid')) return;
        var row = card.querySelector('.vote-row');
        if (!row) { card.style.display = ''; return; }
        var topicId = parseInt(row.getAttribute('data-topic-id'));
        var topicVotes = INITIAL_VOTES[topicId] || [];
        var voteTypes = topicVotes.map(function(v) { return v.vote_type; });

        if (filter === 'all') {
            card.style.display = '';
        } else if (filter === 'unvoted') {
            card.style.display = topicVotes.length === 0 ? '' : 'none';
        } else {
            card.style.display = voteTypes.indexOf(filter) !== -1 ? '' : 'none';
        }
    });

    // Re-calculate fading for newly visible card layout
    reapplyFading();
}

/* ── Export voted topics ── */
function exportVoted() {
    fetch('/api/export/voted')
        .then(function(r) { return r.text(); })
        .then(function(md) {
            navigator.clipboard.writeText(md).then(function() {
                var btn = document.getElementById('exportVotedBtn');
                if (btn) {
                    var orig = btn.textContent;
                    btn.textContent = 'Copied!';
                    btn.style.color = 'var(--success)';
                    setTimeout(function() {
                        btn.textContent = orig;
                        btn.style.color = '';
                    }, 1500);
                }
            });
        });
}

function exportBullets() {
    fetch('/api/export/bullets')
        .then(function(r) { return r.text(); })
        .then(function(md) {
            navigator.clipboard.writeText(md).then(function() {
                var btn = document.querySelector('a[onclick*="exportBullets"]');
                if (btn) {
                    var orig = btn.textContent;
                    btn.textContent = 'Copied!';
                    btn.style.color = 'var(--success)';
                    setTimeout(function() {
                        btn.textContent = orig;
                        btn.style.color = '';
                    }, 1500);
                }
            });
        });
}

/* ── Training: Log impressions ── */
var _impressionsLogged = false;
function logImpressions(voter) {
    if (_impressionsLogged) return;
    if (typeof INITIAL_VOTES === 'undefined') return;
    if (!voter) return;
    var topicIds = [];
    var seen = {};
    document.querySelectorAll('.vote-row[data-topic-id]').forEach(function(row) {
        var tid = parseInt(row.getAttribute('data-topic-id'));
        if (!tid || seen[tid]) return;
        seen[tid] = true;
        topicIds.push(tid);
    });
    if (topicIds.length === 0) return;
    _impressionsLogged = true;
    fetch('/api/impression', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ topic_ids: topicIds })
    }).catch(function() {});
}

/* ── Flagged card indicators ── */
(function() {
    if (typeof INITIAL_VOTES === 'undefined') return;
    document.querySelectorAll('.vote-row[data-topic-id]').forEach(function(row) {
        var topicId = parseInt(row.getAttribute('data-topic-id'));
        var topicVotes = INITIAL_VOTES[topicId] || [];
        var hasFlagVote = topicVotes.some(function(v) { return v.vote_type === 'flag'; });
        if (hasFlagVote) {
            var card = row.closest('.topic-card-sm');
            if (card) card.classList.add('flagged');
        }
    });
})();

/* ── Fade-on-scroll for weekly topic cards ── */
/* Inline script above applies .faded based on actual grid column count.    */
/* This observer reveals .faded cards as the user scrolls them into view.   */
/* reapplyFading() re-calculates after filter pills hide/show cards.        */
var _fadeObserver = null;
var _fadeScrollStarted = false;

(function() {
    var grid = document.querySelector('.topic-grid-dense');
    if (!grid) return;

    if ('IntersectionObserver' in window) {
        _fadeObserver = new IntersectionObserver(function(entries) {
            if (!_fadeScrollStarted) return;
            entries.forEach(function(e) {
                if (e.isIntersecting) {
                    e.target.classList.add('revealed');
                    _fadeObserver.unobserve(e.target);
                }
            });
        }, { rootMargin: '50px', threshold: 0.15 });

        // Initial observe
        grid.querySelectorAll('.topic-card-sm.faded').forEach(function(card) {
            _fadeObserver.observe(card);
        });

        var onFirstScroll = function() {
            _fadeScrollStarted = true;
            window.removeEventListener('scroll', onFirstScroll);
            var vh = window.innerHeight;
            grid.querySelectorAll('.topic-card-sm.faded:not(.revealed)').forEach(function(card) {
                var rect = card.getBoundingClientRect();
                if (rect.top < vh + 50 && rect.bottom > -50) {
                    card.classList.add('revealed');
                    _fadeObserver.unobserve(card);
                }
            });
        };
        window.addEventListener('scroll', onFirstScroll, { passive: true });

        // Re-apply on resize
        var resizeTimer;
        window.addEventListener('resize', function() {
            clearTimeout(resizeTimer);
            resizeTimer = setTimeout(function() { reapplyFading(); }, 200);
        });
    }
})();

function reapplyFading() {
    var grid = document.querySelector('.topic-grid-dense');
    if (!grid) return;

    var allCards = grid.querySelectorAll('.topic-card-sm');
    var visibleCards = [];
    allCards.forEach(function(card) {
        if (getComputedStyle(card).display !== 'none') visibleCards.push(card);
    });

    // Reset all cards
    allCards.forEach(function(card) {
        card.classList.remove('faded');
        card.classList.remove('revealed');
        if (_fadeObserver) _fadeObserver.unobserve(card);
    });

    if (!visibleCards.length) return;

    // Compute actual row breaks from layout positions instead of inferring column count.
    // This is more reliable after sidebar width/layout changes.
    var rowTops = [];
    var rowTolerance = 3;
    visibleCards.forEach(function(card) {
        var top = card.offsetTop || 0;
        var existing = rowTops.find(function(rt) { return Math.abs(rt - top) <= rowTolerance; });
        if (existing == null) rowTops.push(top);
    });
    rowTops.sort(function(a, b) { return a - b; });
    var secondRowTop = rowTops.length >= 2 ? rowTops[1] : null;
    var keepCutoffTop = rowTops.length >= 3 ? rowTops[1] + rowTolerance : null;

    // Apply fading to visible cards beyond row 2
    var vh = window.innerHeight;
    visibleCards.forEach(function(card) {
        var inFirstTwoRows = (keepCutoffTop == null) ? true : ((card.offsetTop || 0) <= keepCutoffTop);
        if (inFirstTwoRows) {
            card.classList.add('revealed');
        } else {
            card.classList.add('faded');
            if (_fadeScrollStarted) {
                var rect = card.getBoundingClientRect();
                if (rect.top < vh + 50 && rect.bottom > -50) {
                    card.classList.add('revealed');
                } else if (_fadeObserver) {
                    _fadeObserver.observe(card);
                }
            } else if (_fadeObserver) {
                _fadeObserver.observe(card);
            }
        }
    });
}

/* ══════════════════════════════════════════════════════════════════ */
/* ── Triage Mode ── */
/* ══════════════════════════════════════════════════════════════════ */
var TRIAGE_ACTIVE = false;

function initTriageMode() {
    var voter = CURRENT_USER;
    var btn = document.getElementById('triageModeBtn');
    if (!btn) return;

    // Show toggle only when a voter is selected
    btn.style.display = voter ? '' : 'none';

    // ON by default — only OFF if user explicitly toggled it off
    if (voter && localStorage.getItem('xfi_triage_mode') !== 'off') {
        TRIAGE_ACTIVE = true;
        btn.classList.add('active');
        applyTriageMode();
    }
}

function toggleTriageMode() {
    var voter = CURRENT_USER;
    if (!voter) return;

    TRIAGE_ACTIVE = !TRIAGE_ACTIVE;
    var btn = document.getElementById('triageModeBtn');

    if (TRIAGE_ACTIVE) {
        localStorage.setItem('xfi_triage_mode', 'on');
        if (btn) btn.classList.add('active');
        applyTriageMode();
    } else {
        localStorage.setItem('xfi_triage_mode', 'off');
        if (btn) btn.classList.remove('active');
        removeTriageMode();
    }
}

function applyTriageMode() {
    if (typeof INITIAL_VOTES === 'undefined') return;
    var voter = CURRENT_USER;
    if (!voter) return;

    var layout = document.getElementById('weeklyLayout');
    var center = document.getElementById('weeklyCenter');
    if (!layout || !center) return;

    // Activate 3-column layout
    layout.classList.add('triage-active');
    center.style.display = '';

    // Classify each card across all weekly sections (slide/bullet/candidate)
    var cards = document.querySelectorAll('#weeklyMain .topic-grid-dense .topic-card-sm');
    if (!cards.length) return;
    var votedItems = [];
    var flaggedItems = [];
    var manualActions = _getManualTriageActions();

    cards.forEach(function(card) {
        if (card.closest('#reportedGrid')) return;
        var row = card.querySelector('.vote-row');
        if (!row) return;
        var topicId = parseInt(row.getAttribute('data-topic-id'));
        var topicVotes = INITIAL_VOTES[topicId] || [];

        // Find current voter's vote only
        var myVote = null;
        for (var i = 0; i < topicVotes.length; i++) {
            if (topicVotes[i].voter_name === voter) {
                myVote = topicVotes[i];
                break;
            }
        }

        if (!myVote) {
            // Not voted by me — no triage action needed
            return;
        }

        if (myVote.vote_type === 'flag') {
            // Flagged — collect for Reported section (card stays visible in main grid)
            flaggedItems.push({ element: card, topicId: topicId });
            return;
        }

        // Voted (slide/bullet/skip/unsure) — collect for center column
        // Card stays visible in main grid (triage is purely additive)
        votedItems.push({
            element: card,
            topicId: topicId,
            voteType: myVote.vote_type,
            votedAt: myVote.voted_at || '',
            sourceSection: card.getAttribute('data-weekly-section') || '',
            topicState: card.getAttribute('data-topic-state') || '',
        });
    });

    cards.forEach(function(card) {
        if (card.closest('#reportedGrid')) return;
        var topicId = parseInt(card.getAttribute('data-topic-id') || '0');
        if (!topicId) return;
        var ma = manualActions[String(topicId)];
        if (!ma) return;
        var alreadyTracked = votedItems.some(function(it) { return it.topicId === topicId; }) ||
                             flaggedItems.some(function(it) { return it.topicId === topicId; });
        if (alreadyTracked) return;
        votedItems.push({
            element: card,
            topicId: topicId,
            voteType: 'triage',
            votedAt: ma.at || '',
            manualAction: ma.action || 'manual',
            sourceSection: card.getAttribute('data-weekly-section') || '',
            topicState: card.getAttribute('data-topic-state') || '',
        });
    });

    // Sort voted items by votedAt descending (most recent first)
    votedItems.sort(function(a, b) {
        return (b.votedAt || '').localeCompare(a.votedAt || '');
    });

    // Build center column content
    var centerList = document.getElementById('myCenterList');
    centerList.innerHTML = '';

    votedItems.forEach(function(item) {
        var card = item.element;
        var topicName = card.querySelector('h3') ? card.querySelector('h3').textContent.trim() : '';
        var badge = card.querySelector('.badge');
        var badgeHTML = badge ? '<span class="' + badge.className + '" style="font-size:8px;padding:1px 5px;">' + badge.textContent + '</span>' : '';
        var topicHref = card.getAttribute('href') || '#';
        var statusBits = [];
        if (item.sourceSection) statusBits.push(item.sourceSection);
        if (item.topicState) statusBits.push(item.topicState);
        if (item.voteType === 'triage' && item.manualAction) statusBits.push(item.manualAction);

        var compactCard = document.createElement('a');
        compactCard.href = topicHref;
        compactCard.className = 'voted-card-compact';
        compactCard.setAttribute('data-triage-topic-id', item.topicId);
        compactCard.innerHTML =
            badgeHTML +
            '<h4>' + _escHtml(topicName) + '</h4>' +
            '<div class="vc-meta">' +
                '<span class="vc-type vc-type-' + (item.voteType === 'triage' ? 'unsure' : item.voteType) + '">' +
                    _escHtml(item.voteType === 'triage' ? (item.manualAction || 'triage') : item.voteType) +
                '</span>' +
                (statusBits.length ? '<span style="font-size:10px;color:var(--text-muted);">' + _escHtml(statusBits.join(' · ')) + '</span>' : '') +
                '<span>' + _fmtVoteTime(item.votedAt) + '</span>' +
                (item.voteType === 'triage'
                    ? '<span class="vc-undo" style="opacity:0.8; cursor:default;">Open</span>'
                    : '<button class="vc-undo" onclick="event.preventDefault();event.stopPropagation();undoVoteFromCenter(' + item.topicId + ')">Undo</button>') +
            '</div>';
        centerList.appendChild(compactCard);
    });

    document.getElementById('myCenterCount').textContent = votedItems.length + ' tracked';

    // Build Reported Errors section
    var reportedSection = document.getElementById('reportedErrors');
    var reportedGrid = document.getElementById('reportedGrid');
    var reportedCount = document.getElementById('reportedCount');
    if (flaggedItems.length > 0) {
        reportedSection.style.display = '';
        reportedGrid.innerHTML = '';
        reportedCount.textContent = flaggedItems.length + ' flagged';
        flaggedItems.forEach(function(item) {
            var clone = item.element.cloneNode(true);
            clone.style.display = '';
            clone.setAttribute('data-flagged-topic-id', item.topicId);
            // Replace the flag button with an Unflag button in the cloned card
            var flagBtn = clone.querySelector('.vote-btn-flag');
            if (flagBtn) {
                flagBtn.textContent = 'Unflag';
                flagBtn.style.cssText = 'color:#f08080;border-color:#f08080;margin-left:auto;font-size:10px;';
                flagBtn.setAttribute('data-vote', '');
                flagBtn.onclick = function(e) {
                    e.preventDefault();
                    e.stopPropagation();
                    unflagTopic(item.topicId);
                };
            }
            reportedGrid.appendChild(clone);
        });
    } else {
        reportedSection.style.display = 'none';
        reportedGrid.innerHTML = '';
        reportedCount.textContent = '';
    }
}

function removeTriageMode() {
    var layout = document.getElementById('weeklyLayout');
    var center = document.getElementById('weeklyCenter');
    if (!layout || !center) return;

    // Remove 3-column layout
    layout.classList.remove('triage-active');
    center.style.display = 'none';

    // Show all cards in all weekly sections
    document.querySelectorAll('#weeklyMain .topic-grid-dense .topic-card-sm').forEach(function(card) {
        if (card.closest('#reportedGrid')) return;
        card.style.display = '';
    });

    // Hide Reported Errors section
    var reportedSection = document.getElementById('reportedErrors');
    if (reportedSection) reportedSection.style.display = 'none';
    var reportedGrid = document.getElementById('reportedGrid');
    if (reportedGrid) reportedGrid.innerHTML = '';
}

function undoVoteFromCenter(topicId) {
    var voter = CURRENT_USER;
    if (!voter) return;

    fetch('/api/vote', {
        method: 'DELETE',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ topic_id: topicId })
    })
    .then(function(r) { return r.json(); })
    .then(function(data) {
        if (data.ok) {
            INITIAL_VOTES[topicId] = data.votes;
            updateVoteRow(topicId, data.votes);
            applyVoteConsensusColors();
            if (TRIAGE_ACTIVE) applyTriageMode();
            if (document.getElementById('weeklyLayout')) _queueWeeklySectionsReconcile('undo-center');
        }
    });
}

function unflagTopic(topicId) {
    var voter = CURRENT_USER;
    if (!voter) return;

    fetch('/api/vote', {
        method: 'DELETE',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ topic_id: topicId })
    })
    .then(function(r) { return r.json(); })
    .then(function(data) {
        if (data.ok) {
            INITIAL_VOTES[topicId] = data.votes;
            updateVoteRow(topicId, data.votes);
            applyVoteConsensusColors();
            if (TRIAGE_ACTIVE) applyTriageMode();
            if (document.getElementById('weeklyLayout')) _queueWeeklySectionsReconcile('unflag');
        }
    });
}

function _escHtml(text) {
    var div = document.createElement('div');
    div.appendChild(document.createTextNode(text));
    return div.innerHTML;
}

function _fmtVoteTime(isoStr) {
    if (!isoStr) return '';
    try {
        var d = new Date(isoStr.replace(' ', 'T') + 'Z');
        var now = new Date();
        var diffMin = Math.round((now - d) / 60000);
        if (diffMin < 1) return 'just now';
        if (diffMin < 60) return diffMin + 'm ago';
        var diffHr = Math.round(diffMin / 60);
        if (diffHr < 24) return diffHr + 'h ago';
        return isoStr.substring(5, 16);
    } catch(e) { return ''; }
}

// Initialize triage mode on page load
initTriageMode();

/* ── Manual Editorial Controls ── */
function _topicEditorialAction(url, body, btn) {
    var orig = btn ? btn.textContent : '';
    if (btn) {
        btn.disabled = true;
        btn.textContent = '...';
    }
    fetch(url, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body || {})
    })
    .then(function(r) { return r.json().then(function(d){ return { ok:r.ok, data:d }; }); })
    .then(function(resp) {
        if (!resp.ok || !resp.data || !resp.data.ok) {
            throw new Error((resp.data && resp.data.error) || 'Request failed');
        }
        if (resp.data.topic && resp.data.topic.id) {
            var action = 'manual';
            if (body && typeof body.promote !== 'undefined') action = body.promote ? 'promote' : 'demote';
            if (body && body.tier) action = body.tier;
            _markManualTriageAction(resp.data.topic.id, action);
        }
        if (btn) {
            btn.textContent = 'Saved';
            btn.style.color = 'var(--success)';
            setTimeout(function() {
                btn.textContent = orig;
                btn.disabled = false;
                btn.style.color = '';
            }, 600);
        }
        if (document.getElementById('weeklyLayout')) {
            _queueWeeklySectionsReconcile('editorial-action');
        } else {
            setTimeout(function() { window.location.reload(); }, 250);
        }
    })
    .catch(function(err) {
        if (btn) {
            btn.textContent = 'Err';
            btn.style.color = '#e87474';
            setTimeout(function() {
                btn.textContent = orig;
                btn.disabled = false;
                btn.style.color = '';
            }, 1200);
        }
        console.warn(err);
    });
}

function setTopicPromoted(topicId, promote, btn) {
    _topicEditorialAction('/api/topics/' + topicId + '/promote', { promote: !!promote }, btn);
}

function setTopicTier(topicId, tier, btn) {
    _topicEditorialAction('/api/topics/' + topicId + '/tier', { tier: tier }, btn);
}

function clearTopicSelection(topicId, btn) {
    var orig = btn ? btn.textContent : '';
    if (btn) {
        btn.disabled = true;
        btn.textContent = '...';
    }

    Promise.allSettled([
        fetch('/api/vote', {
            method: 'DELETE',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ topic_id: topicId })
        }),
        fetch('/api/topics/' + topicId + '/tier', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ tier: 'none' })
        })
    ]).then(function() {
        _markManualTriageAction(topicId, 'clear');
        if (btn) {
            btn.textContent = 'Cleared';
            btn.style.color = 'var(--success)';
        }
        if (document.getElementById('weeklyLayout')) {
            _queueWeeklySectionsReconcile('clear-selection');
        } else {
            setTimeout(function() { window.location.reload(); }, 300);
        }
    }).catch(function() {
        if (btn) {
            btn.textContent = 'Err';
            btn.style.color = '#e87474';
            setTimeout(function() {
                btn.textContent = orig;
                btn.disabled = false;
                btn.style.color = '';
            }, 900);
        }
    });
}

/* ── Topic Creation ── */
var TAXONOMY_DATA = (window.XFI_BOOTSTRAP && window.XFI_BOOTSTRAP.taxonomy) || {};

function renderDuplicateSuggestions(containerId, topics, query) {
    var el = document.getElementById(containerId);
    if (!el) return;
    if (!query || query.trim().length < 3) {
        el.style.display = 'none';
        el.innerHTML = '';
        return;
    }
    topics = topics || [];
    if (!topics.length) {
        el.style.display = '';
        el.innerHTML = '<div style="font-size:11px;color:var(--text-muted);padding:6px 0;">No existing topic match found. Press Create to add a new one.</div>';
        return;
    }
    el.style.display = '';
    el.innerHTML =
        '<div style="font-size:10px;color:var(--text-muted);margin-bottom:4px;">Possible existing topics</div>' +
        topics.slice(0, 5).map(function(t) {
            var state = t.is_promoted ? 'Active' : 'Candidate';
            return (
                '<a href="' + t.url + '" style="display:flex;justify-content:space-between;gap:8px;padding:6px 8px;border:1px solid var(--border);border-radius:8px;text-decoration:none;color:inherit;background:rgba(255,255,255,0.01);margin-bottom:6px;">' +
                    '<span style="min-width:0;"><span style="color:var(--white);font-size:12px;">' + _escHtml(t.name) + '</span>' +
                    (t.description ? '<span style="display:block;font-size:10px;color:var(--text-muted);margin-top:2px;">' + _escHtml(t.description.slice(0, 120)) + '</span>' : '') +
                    '</span>' +
                    '<span style="white-space:nowrap;font-size:10px;color:var(--text-muted);">' + state + ' · ' + (t.post_count || 0) + ' posts</span>' +
                '</a>'
            );
        }).join('');
}

function attachDuplicateChecker(inputId, containerId) {
    var input = document.getElementById(inputId);
    var container = document.getElementById(containerId);
    if (!input || !container) return;
    var state = { timer: null, seq: 0, controller: null };
    input.addEventListener('input', function() {
        clearTimeout(state.timer);
        var q = input.value.trim();
        if (q.length < 3) {
            _abortTopicSearchController(state);
            renderDuplicateSuggestions(containerId, [], '');
            return;
        }
        state.timer = setTimeout(function() {
            var mySeq = ++state.seq;
            _fetchTopicSearchJson(q, 6, state)
                .then(function(data) {
                    if (mySeq !== state.seq) return;
                    renderDuplicateSuggestions(containerId, (data && data.topics) || [], q);
                })
                .catch(function(err) {
                    if (mySeq !== state.seq) return;
                    if (err && err.name === 'AbortError') return;
                    container.style.display = '';
                    container.innerHTML = '<div style="font-size:11px;color:#e87474;">Topic search unavailable right now.</div>';
                });
        }, 180);
    });
}

attachDuplicateChecker('tcName', 'tcDupes');
attachDuplicateChecker('tcpName', 'tcpDupes');

(function initCreatePagePrefill() {
    var pageName = document.getElementById('tcpName');
    if (!pageName) return;
    var v = pageName.value.trim();
    if (!v) return;
    setTimeout(function() {
        pageName.dispatchEvent(new Event('input'));
        pageName.focus();
        pageName.setSelectionRange(pageName.value.length, pageName.value.length);
    }, 0);
})();

function openTopicForm() {
    document.getElementById('topicCreateOverlay').classList.add('active');
    document.getElementById('tcName').value = '';
    document.getElementById('tcDesc').value = '';
    document.getElementById('tcCategory').value = '';
    document.getElementById('tcSubcategory').innerHTML = '<option value="">-- Select category first --</option>';
    document.getElementById('tcPostUrls').value = '';
    var srcEl = document.getElementById('tcSourceUrls');
    if (srcEl) srcEl.value = '';
    document.getElementById('tcStatus').textContent = '';
    renderDuplicateSuggestions('tcDupes', [], '');
}
function closeTopicForm() {
    document.getElementById('topicCreateOverlay').classList.remove('active');
}
function updateSubcats() {
    var cat = document.getElementById('tcCategory').value;
    var sel = document.getElementById('tcSubcategory');
    sel.innerHTML = '<option value="">-- Select --</option>';
    if (cat && TAXONOMY_DATA[cat]) {
        var subs = TAXONOMY_DATA[cat].subcategories || {};
        for (var key in subs) {
            var opt = document.createElement('option');
            opt.value = key;
            opt.textContent = key.replace(/_/g, ' ').toLowerCase().replace(/\b\w/g, function(l) { return l.toUpperCase(); });
            sel.appendChild(opt);
        }
    }
}
function submitTopic() {
    var name = document.getElementById('tcName').value.trim();
    if (!name) { document.getElementById('tcStatus').textContent = 'Name is required'; return; }

    var postUrls = document.getElementById('tcPostUrls').value.trim().split('\n').filter(function(u) { return u.trim(); });
    var sourceUrls = (document.getElementById('tcSourceUrls') ? document.getElementById('tcSourceUrls').value : '')
        .trim().split('\n').filter(function(u) { return u.trim(); });
    document.getElementById('tcStatus').textContent = 'Creating topic...';

    fetch('/api/topics', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
            name: name,
            description: document.getElementById('tcDesc').value.trim(),
            category: document.getElementById('tcCategory').value || null,
            subcategory: document.getElementById('tcSubcategory').value || null,
            post_urls: postUrls,
            source_urls: sourceUrls,
            trigger_transcription: true
        })
    })
    .then(function(r) { return r.json(); })
    .then(function(data) {
        if (data.ok) {
            document.getElementById('tcStatus').textContent = data.transcription_queued
                ? 'Topic created as candidate; transcription queued (10-20 min). Redirecting...'
                : 'Topic created! Redirecting...';
            document.getElementById('tcStatus').style.color = 'var(--success)';
            setTimeout(function() {
                closeTopicForm();
                window.location.href = '/topics/' + data.topic_id;
            }, 1000);
        } else {
            document.getElementById('tcStatus').textContent = 'Error: ' + (data.error || 'Unknown error');
            document.getElementById('tcStatus').style.color = '#e87474';
        }
    })
    .catch(function(e) {
        document.getElementById('tcStatus').textContent = 'Error: ' + e.message;
        document.getElementById('tcStatus').style.color = '#e87474';
    });
}

/* ── Topic Editing (inline on detail page) ── */
function toggleEditTopic() {
    var editSection = document.getElementById('editInline');
    if (!editSection) return;
    editSection.classList.toggle('active');
}
function updateEditSubcats() {
    var cat = document.getElementById('editCategory').value;
    var sel = document.getElementById('editSubcategory');
    sel.innerHTML = '<option value="">-- Select --</option>';
    if (cat && TAXONOMY_DATA[cat]) {
        var subs = TAXONOMY_DATA[cat].subcategories || {};
        for (var key in subs) {
            var opt = document.createElement('option');
            opt.value = key;
            opt.textContent = key.replace(/_/g, ' ').toLowerCase().replace(/\b\w/g, function(l) { return l.toUpperCase(); });
            sel.appendChild(opt);
        }
    }
}
function saveTopicEdit(topicId) {
    var updates = {};
    var nameEl = document.getElementById('editName');
    var descEl = document.getElementById('editDesc');
    var catEl = document.getElementById('editCategory');
    var subEl = document.getElementById('editSubcategory');
    var ktEl = document.getElementById('editKeyTakeaways');
    var bulletsEl = document.getElementById('editBullets');

    if (nameEl) updates.name = nameEl.value.trim();
    if (descEl) updates.description = descEl.value.trim();
    if (catEl) updates.category = catEl.value || null;
    if (subEl) updates.subcategory = subEl.value || null;
    if (ktEl) {
        var ktLines = ktEl.value.split('\n').map(function(l) { return l.trim(); }).filter(Boolean).slice(0, 2);
        updates.summary_key_takeaways = JSON.stringify(ktLines);
    }
    if (bulletsEl) {
        var lines = bulletsEl.value.split('\n').map(function(l) { return l.trim(); }).filter(Boolean);
        updates.summary_bullets = JSON.stringify(lines);
    }

    fetch('/api/topics/' + topicId, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(updates)
    })
    .then(function(r) { return r.json(); })
    .then(function(data) {
        if (data.ok) {
            window.location.reload();
        } else {
            alert('Error: ' + (data.error || 'Unknown'));
        }
    });
}

/* ── Edit Modal (from weekly cards) ── */
function openEditModal(topicId) {
    document.getElementById('teTopicId').value = topicId;
    document.getElementById('teStatus').textContent = 'Loading...';
    document.getElementById('teStatus').style.color = 'var(--text-muted)';
    document.getElementById('topicEditOverlay').classList.add('active');

    fetch('/api/topics/' + topicId)
        .then(function(r) { return r.json(); })
        .then(function(data) {
            if (data.topic) {
                var t = data.topic;
                document.getElementById('teName').value = t.name || '';
                document.getElementById('teDesc').value = t.description || '';
                // Populate key takeaways
                var ktTextarea = document.getElementById('teKeyTakeaways');
                if (ktTextarea) {
                    var kt = [];
                    if (t.summary_key_takeaways) {
                        try { kt = JSON.parse(t.summary_key_takeaways); } catch(e) {}
                    }
                    ktTextarea.value = Array.isArray(kt) ? kt.join('\n') : '';
                }
                // Populate summary bullets
                var bulletsTextarea = document.getElementById('teBullets');
                if (bulletsTextarea) {
                    var bullets = [];
                    if (t.summary_bullets) {
                        try { bullets = JSON.parse(t.summary_bullets); } catch(e) {}
                    }
                    bulletsTextarea.value = Array.isArray(bullets) ? bullets.join('\n') : '';
                }
                document.getElementById('teCategory').value = t.category || '';
                updateEditModalSubcats();
                if (t.subcategory) {
                    document.getElementById('teSubcategory').value = t.subcategory;
                }
                document.getElementById('teStatus').textContent = '';
            } else {
                document.getElementById('teStatus').textContent = 'Error loading topic';
                document.getElementById('teStatus').style.color = '#e87474';
            }
        })
        .catch(function(e) {
            document.getElementById('teStatus').textContent = 'Error: ' + e.message;
            document.getElementById('teStatus').style.color = '#e87474';
        });
}

function closeEditModal() {
    document.getElementById('topicEditOverlay').classList.remove('active');
}

function updateEditModalSubcats() {
    var cat = document.getElementById('teCategory').value;
    var sel = document.getElementById('teSubcategory');
    sel.innerHTML = '<option value="">-- Select --</option>';
    if (cat && TAXONOMY_DATA[cat]) {
        var subs = TAXONOMY_DATA[cat].subcategories || {};
        for (var key in subs) {
            var opt = document.createElement('option');
            opt.value = key;
            opt.textContent = key.replace(/_/g, ' ').toLowerCase().replace(/\b\w/g, function(l) { return l.toUpperCase(); });
            sel.appendChild(opt);
        }
    }
}

function submitEditModal() {
    var topicId = parseInt(document.getElementById('teTopicId').value);
    var ktEl = document.getElementById('teKeyTakeaways');
    var ktLines = [];
    if (ktEl && ktEl.value.trim()) {
        ktLines = ktEl.value.split('\n').map(function(l) { return l.trim(); }).filter(Boolean).slice(0, 2);
    }
    var bulletsEl = document.getElementById('teBullets');
    var bulletLines = [];
    if (bulletsEl && bulletsEl.value.trim()) {
        bulletLines = bulletsEl.value.split('\n').map(function(l) { return l.trim(); }).filter(Boolean);
    }
    var updates = {
        name: document.getElementById('teName').value.trim(),
        description: document.getElementById('teDesc').value.trim(),
        category: document.getElementById('teCategory').value || null,
        subcategory: document.getElementById('teSubcategory').value || null,
        summary_key_takeaways: JSON.stringify(ktLines),
        summary_bullets: JSON.stringify(bulletLines)
    };
    if (!updates.name) {
        document.getElementById('teStatus').textContent = 'Name is required';
        document.getElementById('teStatus').style.color = '#e87474';
        return;
    }
    document.getElementById('teStatus').textContent = 'Saving...';
    document.getElementById('teStatus').style.color = 'var(--text-muted)';

    fetch('/api/topics/' + topicId, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(updates)
    })
    .then(function(r) { return r.json(); })
    .then(function(data) {
        if (data.ok) {
            document.getElementById('teStatus').textContent = 'Saved! Refreshing...';
            document.getElementById('teStatus').style.color = 'var(--success)';
            setTimeout(function() {
                closeEditModal();
                window.location.reload();
            }, 800);
        } else {
            document.getElementById('teStatus').textContent = 'Error: ' + (data.error || 'Unknown');
            document.getElementById('teStatus').style.color = '#e87474';
        }
    })
    .catch(function(e) {
        document.getElementById('teStatus').textContent = 'Error: ' + e.message;
        document.getElementById('teStatus').style.color = '#e87474';
    });
}

/* ── Create Page (dedicated tab) ── */
function updateCreatePageSubcats() {
    var cat = document.getElementById('tcpCategory').value;
    var sel = document.getElementById('tcpSubcategory');
    sel.innerHTML = '<option value="">-- Select --</option>';
    if (cat && TAXONOMY_DATA[cat]) {
        var subs = TAXONOMY_DATA[cat].subcategories || {};
        for (var key in subs) {
            var opt = document.createElement('option');
            opt.value = key;
            opt.textContent = key.replace(/_/g, ' ').toLowerCase().replace(/\b\w/g, function(l) { return l.toUpperCase(); });
            sel.appendChild(opt);
        }
    }
}

function submitCreatePage() {
    var name = document.getElementById('tcpName').value.trim();
    if (!name) {
        document.getElementById('tcpStatus').textContent = 'Name is required';
        document.getElementById('tcpStatus').style.color = '#e87474';
        return;
    }
    var postUrls = document.getElementById('tcpPostUrls').value.trim()
        .split('\n').filter(function(u) { return u.trim(); });
    var sourceUrls = (document.getElementById('tcpSourceUrls') ? document.getElementById('tcpSourceUrls').value : '')
        .trim().split('\n').filter(function(u) { return u.trim(); });
    document.getElementById('tcpStatus').textContent = 'Creating topic...';
    document.getElementById('tcpStatus').style.color = 'var(--text-muted)';

    fetch('/api/topics', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
            name: name,
            description: document.getElementById('tcpDesc').value.trim(),
            category: document.getElementById('tcpCategory').value || null,
            subcategory: document.getElementById('tcpSubcategory').value || null,
            post_urls: postUrls,
            source_urls: sourceUrls,
            trigger_transcription: true
        })
    })
    .then(function(r) { return r.json(); })
    .then(function(data) {
        if (data.ok) {
            document.getElementById('tcpStatus').textContent = data.transcription_queued
                ? 'Topic created as candidate; transcription queued (10-20 min). Redirecting...'
                : 'Topic created! Redirecting...';
            document.getElementById('tcpStatus').style.color = 'var(--success)';
            setTimeout(function() {
                window.location.href = '/topics/' + data.topic_id;
            }, 1000);
        } else {
            document.getElementById('tcpStatus').textContent = 'Error: ' + (data.error || 'Unknown');
            document.getElementById('tcpStatus').style.color = '#e87474';
        }
    })
    .catch(function(e) {
        document.getElementById('tcpStatus').textContent = 'Error: ' + e.message;
        document.getElementById('tcpStatus').style.color = '#e87474';
    });
}

/* ── Pipeline: Retry failed topic ── */
function retryTopic(utId, btn) {
    btn.textContent = 'Retrying...';
    btn.disabled = true;
    fetch('/api/topics/retry/' + utId, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' }
    })
    .then(function(r) { return r.json(); })
    .then(function(data) {
        if (data.ok) {
            btn.textContent = 'Success!';
            btn.style.color = 'var(--success)';
            btn.style.borderColor = 'var(--success)';
            setTimeout(function() { window.location.reload(); }, 1000);
        } else {
            btn.textContent = 'Failed: ' + (data.error || 'Unknown');
            btn.style.color = '#e87474';
            btn.disabled = false;
        }
    })
    .catch(function(e) {
        btn.textContent = 'Error';
        btn.disabled = false;
    });
}

/* ── Inline Tweet Embeds ── */
var EMBED_LAZY_OBSERVER = null;

function _extractEmbedTweetIdFromButton(btn) {
    if (!btn) return null;
    var onclickAttr = btn.getAttribute('onclick') || '';
    var match = onclickAttr.match(/toggleEmbed\('([^']+)'/);
    return match ? match[1] : null;
}

function _disconnectLazyEmbedObserver() {
    if (EMBED_LAZY_OBSERVER) {
        EMBED_LAZY_OBSERVER.disconnect();
        EMBED_LAZY_OBSERVER = null;
    }
}

function renderEmbed(tweetId, container) {
    if (!container) return;
    if (container.dataset.embedLoading === '1') return;
    container.dataset.embedLoading = '1';
    container.dataset.embedLoaded = '0';
    container.innerHTML = '<span style="font-size:11px;color:var(--text-muted);">Loading embed...</span>';
    if (typeof twttr !== 'undefined' && twttr.widgets) {
        container.innerHTML = '';
        twttr.widgets.createTweet(tweetId, container, {
            theme: typeof getEmbedTheme === 'function' ? getEmbedTheme() : 'dark',
            dnt: true,
            width: 500
        }).then(function(el) {
            container.dataset.embedLoading = '0';
            container.dataset.embedLoaded = el ? '1' : '0';
            if (!el) {
                container.innerHTML = '<span style="font-size:11px;color:var(--text-muted);">Embed unavailable (tweet may be private)</span>';
            }
        }).catch(function() {
            container.dataset.embedLoading = '0';
            container.dataset.embedLoaded = '0';
            container.innerHTML = '<span style="font-size:11px;color:var(--text-muted);">Embed unavailable right now</span>';
        });
    } else {
        // Fallback if widgets.js hasn't loaded yet
        container.dataset.embedLoading = '0';
        container.dataset.embedLoaded = '1';
        var embedTheme = typeof getEmbedTheme === 'function' ? getEmbedTheme() : 'dark';
        container.innerHTML = '<iframe src="https://platform.twitter.com/embed/Tweet.html?id=' + tweetId +
            '&theme=' + embedTheme + '" style="width:100%;min-height:250px;border:none;border-radius:8px;" allowfullscreen></iframe>';
    }
}

function toggleEmbed(tweetId, btn) {
    var container = document.getElementById('embed-' + tweetId);
    if (!container) return;
    if (EMBED_LAZY_OBSERVER && btn) {
        try { EMBED_LAZY_OBSERVER.unobserve(btn); } catch (e) {}
    }

    // Toggle off
    if (container.hasChildNodes()) {
        container.innerHTML = '';
        btn.classList.remove('active');
        container.dataset.embedLoaded = '0';
        container.dataset.embedLoading = '0';
        container.dataset.autoEmbedDisabled = '1';
        return;
    }

    // Toggle on
    btn.classList.add('active');
    container.dataset.autoEmbedDisabled = '0';
    renderEmbed(tweetId, container);
}

function _lazyLoadEmbedForButton(btn) {
    var tweetId = _extractEmbedTweetIdFromButton(btn);
    if (!tweetId) return;
    var container = document.getElementById('embed-' + tweetId);
    if (!container) return;
    if (container.dataset.autoEmbedDisabled === '1') return;
    if (container.hasChildNodes() || container.dataset.embedLoading === '1') return;
    btn.classList.add('active');
    renderEmbed(tweetId, container);
}

/* Lazy auto-load embeds on topic detail page (viewport-driven) */
function autoLoadEmbeds() {
    var buttons = document.querySelectorAll('.embed-toggle');
    if (!buttons.length) return;
    _disconnectLazyEmbedObserver();

    if ('IntersectionObserver' in window) {
        EMBED_LAZY_OBSERVER = new IntersectionObserver(function(entries, observer) {
            entries.forEach(function(entry) {
                if (!entry.isIntersecting) return;
                observer.unobserve(entry.target);
                _lazyLoadEmbedForButton(entry.target);
            });
        }, {
            root: null,
            rootMargin: '350px 0px',
            threshold: 0.01
        });

        buttons.forEach(function(btn) {
            EMBED_LAZY_OBSERVER.observe(btn);
        });
        return;
    }

    // Fallback: stagger auto-loads instead of firing all embeds at once.
    buttons.forEach(function(btn, idx) {
        setTimeout(function() {
            _lazyLoadEmbedForButton(btn);
        }, idx * 120);
    });
}

// Fire auto-load when twttr is ready, or after a short delay
if (typeof twttr !== 'undefined' && twttr.events) {
    twttr.ready(function() { autoLoadEmbeds(); });
} else {
    // widgets.js may still be loading — wait for it
    window.addEventListener('load', function() {
        setTimeout(autoLoadEmbeds, 1500);
    });
}

/* ── Topic Merge Mode ── */
var MERGE_MODE_ACTIVE = false;
var MERGE_SELECTED_IDS = [];
var MERGE_TOPIC_DATA = {};

function toggleMergeMode() {
    MERGE_MODE_ACTIVE = !MERGE_MODE_ACTIVE;
    var weeklyMain = document.getElementById('weeklyMain');
    var btn = document.getElementById('mergeModeBtn');
    var actionBar = document.getElementById('mergeActionBar');
    if (!weeklyMain) return;

    if (MERGE_MODE_ACTIVE) {
        weeklyMain.classList.add('merge-mode');
        if (btn) btn.classList.add('active');
        if (actionBar) actionBar.style.display = 'flex';
        MERGE_SELECTED_IDS = [];
        updateMergeActionBar();

        // Add click handlers to cards (prevent navigation)
        weeklyMain.querySelectorAll('.topic-card-sm').forEach(function(card) {
            card.addEventListener('click', mergeCardClickHandler);
        });
    } else {
        weeklyMain.classList.remove('merge-mode');
        if (btn) btn.classList.remove('active');
        if (actionBar) actionBar.style.display = 'none';
        MERGE_SELECTED_IDS = [];

        // Remove click handlers and selection
        weeklyMain.querySelectorAll('.topic-card-sm').forEach(function(card) {
            card.classList.remove('merge-selected');
            card.removeEventListener('click', mergeCardClickHandler);
        });
    }
    if (typeof refreshMobileSwipeVoting === 'function') refreshMobileSwipeVoting(document);
}

function mergeCardClickHandler(e) {
    if (!MERGE_MODE_ACTIVE) return;
    e.preventDefault();
    e.stopPropagation();
    var card = e.currentTarget;
    var tid = parseInt(card.getAttribute('data-topic-id'));
    if (!tid) return;

    if (card.classList.contains('merge-selected')) {
        card.classList.remove('merge-selected');
        MERGE_SELECTED_IDS = MERGE_SELECTED_IDS.filter(function(id) { return id !== tid; });
    } else {
        card.classList.add('merge-selected');
        MERGE_SELECTED_IDS.push(tid);
    }
    updateMergeActionBar();
}

function updateMergeActionBar() {
    var countEl = document.getElementById('mergeSelectedCount');
    if (countEl) countEl.textContent = MERGE_SELECTED_IDS.length + ' selected';
    var submitBtn = document.querySelector('#mergeActionBar .tf-btn-primary');
    if (submitBtn) submitBtn.disabled = MERGE_SELECTED_IDS.length < 2;
}

function getSelectedMergeIds() {
    return MERGE_SELECTED_IDS.slice();
}

function openMergeModal(topicIds) {
    if (!topicIds || topicIds.length < 1) return;
    MERGE_TOPIC_DATA = {};
    document.getElementById('mergeSourceList').innerHTML = 'Loading...';
    document.getElementById('mergeWinnerRadios').innerHTML = '';
    document.getElementById('mergeName').value = '';
    document.getElementById('mergeDesc').value = '';
    var mergeKtEl = document.getElementById('mergeKeyTakeaways');
    if (mergeKtEl) mergeKtEl.value = '';
    document.getElementById('mergeBullets').value = '';
    document.getElementById('mergeCategory').value = '';
    document.getElementById('mergeSubcategory').innerHTML = '<option value="">-- Select --</option>';
    document.getElementById('mergeStatus').textContent = '';
    document.getElementById('mergeSuggestStatus').textContent = '';
    document.getElementById('mergeSearchInput').value = '';
    document.getElementById('mergeSearchResults').classList.remove('active');
    document.getElementById('mergeTopicOverlay').classList.add('active');

    // Fetch each topic's data
    var loaded = 0;
    topicIds.forEach(function(tid) {
        fetch('/api/topics/' + tid)
            .then(function(r) { return r.json(); })
            .then(function(data) {
                if (data.topic) {
                    MERGE_TOPIC_DATA[tid] = data.topic;
                }
                loaded++;
                if (loaded === topicIds.length) renderMergeSourceList();
            })
            .catch(function() {
                loaded++;
                if (loaded === topicIds.length) renderMergeSourceList();
            });
    });
}

function renderMergeSourceList() {
    var listEl = document.getElementById('mergeSourceList');
    var radioEl = document.getElementById('mergeWinnerRadios');
    listEl.innerHTML = '';
    radioEl.innerHTML = '';
    var ids = Object.keys(MERGE_TOPIC_DATA);

    ids.forEach(function(tid, i) {
        var t = MERGE_TOPIC_DATA[tid];
        // Chip
        var chip = document.createElement('span');
        chip.className = 'merge-source-chip';
        chip.setAttribute('data-tid', tid);
        chip.innerHTML = '<strong>' + (t.name || 'Topic #' + tid) + '</strong>'
            + ' <span style="color:var(--text-muted);">(' + (t.post_count || 0) + ' posts)</span>'
            + ' <span class="chip-remove" onclick="removeMergeCandidate(' + tid + ')">&times;</span>';
        listEl.appendChild(chip);

        // Winner radio
        var label = document.createElement('label');
        label.style.cssText = 'display:flex;align-items:center;gap:6px;font-size:12px;margin-bottom:4px;cursor:pointer;';
        var radio = document.createElement('input');
        radio.type = 'radio';
        radio.name = 'mergeWinner';
        radio.value = tid;
        if (i === 0) radio.checked = true;
        label.appendChild(radio);
        label.appendChild(document.createTextNode(t.name || 'Topic #' + tid));
        radioEl.appendChild(label);
    });
}

function removeMergeCandidate(tid) {
    delete MERGE_TOPIC_DATA[tid];
    if (Object.keys(MERGE_TOPIC_DATA).length < 2) {
        closeMergeModal();
        return;
    }
    renderMergeSourceList();
}

function addMergeCandidate(tid) {
    if (MERGE_TOPIC_DATA[tid]) return;
    fetch('/api/topics/' + tid)
        .then(function(r) { return r.json(); })
        .then(function(data) {
            if (data.topic) {
                MERGE_TOPIC_DATA[tid] = data.topic;
                renderMergeSourceList();
            }
        });
    document.getElementById('mergeSearchInput').value = '';
    document.getElementById('mergeSearchResults').classList.remove('active');
}

var mergeSearchTimeout = null;
function searchMergeCandidate(query) {
    var resultsEl = document.getElementById('mergeSearchResults');
    if (!query || query.length < 2) {
        resultsEl.classList.remove('active');
        return;
    }
    clearTimeout(mergeSearchTimeout);
    mergeSearchTimeout = setTimeout(function() {
        fetch('/api/topics/search?q=' + encodeURIComponent(query))
            .then(function(r) { return r.json(); })
            .then(function(data) {
                resultsEl.innerHTML = '';
                var topics = data.topics || data.results || [];
                if (!topics.length) {
                    resultsEl.innerHTML = '<div class="merge-search-item" style="color:var(--text-muted);">No results</div>';
                    resultsEl.classList.add('active');
                    return;
                }
                topics.forEach(function(t) {
                    if (MERGE_TOPIC_DATA[t.id]) return; // already in merge set
                    var item = document.createElement('div');
                    item.className = 'merge-search-item';
                    item.textContent = t.name + ' (' + (t.post_count || 0) + ' posts)';
                    item.onclick = function() { addMergeCandidate(t.id); };
                    resultsEl.appendChild(item);
                });
                resultsEl.classList.add('active');
            });
    }, 300);
}

function updateMergeSubcats() {
    var cat = document.getElementById('mergeCategory').value;
    var sel = document.getElementById('mergeSubcategory');
    sel.innerHTML = '<option value="">-- Select --</option>';
    if (cat && typeof TAXONOMY_DATA !== 'undefined' && TAXONOMY_DATA[cat]) {
        var subs = TAXONOMY_DATA[cat].subcategories || {};
        for (var key in subs) {
            var opt = document.createElement('option');
            opt.value = key;
            opt.textContent = key.replace(/_/g, ' ').toLowerCase().replace(/\b\w/g, function(l) { return l.toUpperCase(); });
            sel.appendChild(opt);
        }
    }
}

function generateMergeSuggestion() {
    var ids = Object.keys(MERGE_TOPIC_DATA).map(Number);
    if (ids.length < 2) return;
    var statusEl = document.getElementById('mergeSuggestStatus');
    var btn = document.getElementById('mergeSuggestBtn');
    statusEl.textContent = 'Generating...';
    statusEl.style.color = 'var(--text-muted)';
    btn.disabled = true;

    fetch('/api/topics/merge/suggest', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ topic_ids: ids })
    })
    .then(function(r) { return r.json(); })
    .then(function(data) {
        btn.disabled = false;
        if (data.ok && data.suggestion) {
            var s = data.suggestion;
            document.getElementById('mergeName').value = s.name || '';
            document.getElementById('mergeDesc').value = s.description || '';
            var mergeKtEl2 = document.getElementById('mergeKeyTakeaways');
            if (mergeKtEl2 && s.key_takeaways && Array.isArray(s.key_takeaways)) {
                mergeKtEl2.value = s.key_takeaways.join('\n');
            }
            if (s.bullets && Array.isArray(s.bullets)) {
                document.getElementById('mergeBullets').value = s.bullets.join('\n');
            }
            if (s.category) {
                document.getElementById('mergeCategory').value = s.category;
                updateMergeSubcats();
                if (s.subcategory) {
                    document.getElementById('mergeSubcategory').value = s.subcategory;
                }
            }
            statusEl.textContent = 'Suggestion applied';
            statusEl.style.color = 'var(--success)';
        } else {
            statusEl.textContent = 'Error: ' + (data.error || 'Unknown');
            statusEl.style.color = '#e87474';
        }
    })
    .catch(function(e) {
        btn.disabled = false;
        statusEl.textContent = 'Error: ' + e.message;
        statusEl.style.color = '#e87474';
    });
}

function submitMerge() {
    var winnerRadio = document.querySelector('input[name="mergeWinner"]:checked');
    if (!winnerRadio) {
        document.getElementById('mergeStatus').textContent = 'Select a winner topic';
        document.getElementById('mergeStatus').style.color = '#e87474';
        return;
    }
    var winnerId = parseInt(winnerRadio.value);
    var allIds = Object.keys(MERGE_TOPIC_DATA).map(Number);
    var loserIds = allIds.filter(function(id) { return id !== winnerId; });

    var name = document.getElementById('mergeName').value.trim();
    if (!name) {
        document.getElementById('mergeStatus').textContent = 'Name is required';
        document.getElementById('mergeStatus').style.color = '#e87474';
        return;
    }

    var ktText = '';
    var mergeKtEl3 = document.getElementById('mergeKeyTakeaways');
    if (mergeKtEl3) ktText = mergeKtEl3.value.trim();
    var ktLines = ktText ? ktText.split('\n').map(function(l) { return l.trim(); }).filter(Boolean).slice(0, 2) : [];

    var bulletsText = document.getElementById('mergeBullets').value.trim();
    var bulletLines = bulletsText ? bulletsText.split('\n').map(function(l) { return l.trim(); }).filter(Boolean) : [];

    var payload = {
        winner_id: winnerId,
        loser_ids: loserIds,
        name: name,
        description: document.getElementById('mergeDesc').value.trim(),
        summary_key_takeaways: JSON.stringify(ktLines),
        summary_bullets: JSON.stringify(bulletLines),
        category: document.getElementById('mergeCategory').value || null,
        subcategory: document.getElementById('mergeSubcategory').value || null
    };

    document.getElementById('mergeStatus').textContent = 'Merging...';
    document.getElementById('mergeStatus').style.color = 'var(--text-muted)';
    document.getElementById('mergeSubmitBtn').disabled = true;

    fetch('/api/topics/merge', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload)
    })
    .then(function(r) { return r.json(); })
    .then(function(data) {
        if (data.ok) {
            document.getElementById('mergeStatus').textContent = 'Merged! Reloading...';
            document.getElementById('mergeStatus').style.color = 'var(--success)';
            setTimeout(function() {
                closeMergeModal();
                window.location.reload();
            }, 800);
        } else {
            document.getElementById('mergeSubmitBtn').disabled = false;
            document.getElementById('mergeStatus').textContent = 'Error: ' + (data.error || 'Unknown');
            document.getElementById('mergeStatus').style.color = '#e87474';
        }
    })
    .catch(function(e) {
        document.getElementById('mergeSubmitBtn').disabled = false;
        document.getElementById('mergeStatus').textContent = 'Error: ' + e.message;
        document.getElementById('mergeStatus').style.color = '#e87474';
    });
}

function closeMergeModal() {
    document.getElementById('mergeTopicOverlay').classList.remove('active');
}

/* ── Quick Create ── */
var QC_TWEET_ID = null;

function submitQuickCreate() {
    var url = document.getElementById('qcUrl').value.trim();
    if (!url) return;
    var statusEl = document.getElementById('qcStatus');
    var btn = document.getElementById('qcGenerateBtn');
    statusEl.textContent = 'Generating suggestion...';
    statusEl.style.color = 'var(--text-muted)';
    btn.disabled = true;

    fetch('/api/topics/quick-create', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ url: url })
    })
    .then(function(r) { return r.json(); })
    .then(function(data) {
        btn.disabled = false;
        if (data.ok && data.suggestion) {
            statusEl.textContent = '';
            QC_TWEET_ID = data.tweet_id;
            var s = data.suggestion;
            document.getElementById('qcName').value = s.name || '';
            document.getElementById('qcDesc').value = s.description || '';
            document.getElementById('qcBullets').value = (s.bullets || []).join('\n');
            if (s.category) {
                document.getElementById('qcCategory').value = s.category;
                updateQcSubcats();
                if (s.subcategory) {
                    setTimeout(function() {
                        document.getElementById('qcSubcategory').value = s.subcategory;
                    }, 50);
                }
            }
            document.getElementById('qcPreview').style.display = 'block';
        } else {
            statusEl.textContent = 'Error: ' + (data.error || 'Unknown');
            statusEl.style.color = '#e87474';
        }
    })
    .catch(function(e) {
        btn.disabled = false;
        statusEl.textContent = 'Error: ' + e.message;
        statusEl.style.color = '#e87474';
    });
}

function updateQcSubcats() {
    var cat = document.getElementById('qcCategory').value;
    var sub = document.getElementById('qcSubcategory');
    sub.innerHTML = '<option value="">-- Select --</option>';
    if (!cat || !window.XFI_BOOTSTRAP || !window.XFI_BOOTSTRAP.taxonomy) return;
    var info = window.XFI_BOOTSTRAP.taxonomy[cat];
    if (!info || !info.subcategories) return;
    Object.keys(info.subcategories).forEach(function(k) {
        var opt = document.createElement('option');
        opt.value = k;
        opt.textContent = k.replace(/_/g, ' ').replace(/\b\w/g, function(c) { return c.toUpperCase(); });
        sub.appendChild(opt);
    });
}

function confirmQuickCreate() {
    var name = document.getElementById('qcName').value.trim();
    if (!name) { alert('Name is required'); return; }
    var bulletsText = document.getElementById('qcBullets').value.trim();
    var bulletLines = bulletsText ? bulletsText.split('\n').map(function(l) { return l.trim(); }).filter(Boolean) : [];
    var postUrls = QC_TWEET_ID ? ['https://x.com/i/status/' + QC_TWEET_ID] : [];

    var payload = {
        name: name,
        description: document.getElementById('qcDesc').value.trim(),
        category: document.getElementById('qcCategory').value || null,
        subcategory: document.getElementById('qcSubcategory').value || null,
        post_urls: postUrls,
        summary_bullets: bulletLines
    };

    var btn = document.getElementById('qcCreateBtn');
    btn.disabled = true;
    btn.textContent = 'Creating...';

    fetch('/api/topics', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload)
    })
    .then(function(r) { return r.json(); })
    .then(function(data) {
        if (data.ok) {
            btn.textContent = 'Created!';
            btn.style.color = 'var(--success)';
            setTimeout(function() { window.location.href = '/topics/' + data.topic_id; }, 600);
        } else {
            btn.disabled = false;
            btn.textContent = 'Create Topic';
            document.getElementById('qcStatus').textContent = 'Error: ' + (data.error || 'Unknown');
            document.getElementById('qcStatus').style.color = '#e87474';
        }
    })
    .catch(function(e) {
        btn.disabled = false;
        btn.textContent = 'Create Topic';
        document.getElementById('qcStatus').textContent = 'Error: ' + e.message;
        document.getElementById('qcStatus').style.color = '#e87474';
    });
}

function autoQuickCreate() {
    var url = document.getElementById('qcUrl').value.trim();
    if (!url) {
        document.getElementById('qcStatus').textContent = 'Paste an X post URL first';
        document.getElementById('qcStatus').style.color = '#e87474';
        return;
    }
    var autoBtn = document.getElementById('qcAutoBtn');
    var genBtn = document.getElementById('qcGenerateBtn');
    var status = document.getElementById('qcStatus');
    autoBtn.disabled = true;
    genBtn.disabled = true;
    status.textContent = 'Creating topic... (AI analysis may take a few seconds)';
    status.style.color = 'var(--text-muted)';

    fetch('/api/topics/quick-create-auto', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ url: url })
    })
    .then(function(r) { return r.json(); })
    .then(function(data) {
        if (data.ok) {
            status.textContent = 'Created: ' + data.name;
            status.style.color = 'var(--success)';
            autoBtn.textContent = 'Created!';
            autoBtn.style.color = 'var(--success)';
            setTimeout(function() { window.location.href = '/topics/' + data.topic_id; }, 600);
        } else {
            autoBtn.disabled = false;
            genBtn.disabled = false;
            status.textContent = 'Error: ' + (data.error || 'Unknown');
            status.style.color = '#e87474';
        }
    })
    .catch(function(e) {
        autoBtn.disabled = false;
        genBtn.disabled = false;
        status.textContent = 'Error: ' + e.message;
        status.style.color = '#e87474';
    });
}

/* ── Topic Split (Multi-Group) ── */
var SPLIT_POSTS = [];
var SPLIT_GROUPS = {};       /* tweetId -> groupIndex (0=source, 1..N=new topics) */
var SPLIT_GROUP_COUNT = 1;   /* number of new-topic groups (starts at 1) */
var SPLIT_GROUP_META = {};   /* groupIndex -> { category, subcategory } from topic info */
var SPLIT_GROUP_COLORS = ['var(--text-muted)', 'var(--accent)', '#a78bfa', '#6ec47a', '#d4a55a', '#e87474'];
var SPLIT_GROUP_LABELS = ['Source', 'New 1', 'New 2', 'New 3', 'New 4', 'New 5'];
var SPLIT_MAX_GROUPS = 5;

function openSplitModal(topicId) {
    topicId = parseInt(topicId);
    if (!topicId) return;
    document.getElementById('splitSourceId').value = topicId;
    SPLIT_POSTS = [];
    SPLIT_GROUPS = {};
    SPLIT_GROUP_COUNT = 1;
    SPLIT_GROUP_META = {};
    document.getElementById('splitPostList').innerHTML = '<span style="font-size:11px;color:var(--text-muted);">Loading posts...</span>';
    document.getElementById('splitGroupTabs').innerHTML = '';
    document.getElementById('splitGroupForms').innerHTML = '';
    document.getElementById('splitStatus').textContent = '';
    document.getElementById('splitSuggestStatus').textContent = '';
    document.getElementById('splitTopicOverlay').classList.add('active');

    // Fetch topic info for source prefill
    fetch('/api/topics/' + topicId)
        .then(function(r) { return r.json(); })
        .then(function(resp) {
            var data = resp.topic || resp;
            if (data.name) document.getElementById('splitSourceName').textContent = data.name;
            SPLIT_GROUP_META[0] = {
                description: data.description || '',
                key_takeaways: '',
                bullets: '',
                category: data.category || '',
                subcategory: data.subcategory || ''
            };
            if (data.summary_key_takeaways) {
                try {
                    var skt = JSON.parse(data.summary_key_takeaways);
                    if (Array.isArray(skt)) SPLIT_GROUP_META[0].key_takeaways = skt.join('\n');
                } catch(e) {}
            }
            if (data.summary_bullets) {
                try {
                    var bl = JSON.parse(data.summary_bullets);
                    if (Array.isArray(bl)) SPLIT_GROUP_META[0].bullets = bl.join('\n');
                } catch(e) {}
            }
            // Default new group 1 to same category
            if (data.category) {
                SPLIT_GROUP_META[1] = { category: data.category, subcategory: data.subcategory || '' };
            }
            renderSplitGroups();
        })
        .catch(function() {});

    // Fetch posts
    fetch('/api/topics/' + topicId + '/posts')
        .then(function(r) { return r.json(); })
        .then(function(data) {
            if (data.ok && data.posts) {
                SPLIT_POSTS = data.posts;
                // All posts start in source (group 0)
                SPLIT_POSTS.forEach(function(p) { SPLIT_GROUPS[p.tweet_id] = 0; });
                renderSplitGroups();
            }
        })
        .catch(function() {
            document.getElementById('splitPostList').innerHTML = '<span style="color:#e87474;">Failed to load posts</span>';
        });
}

function closeSplitModal() {
    document.getElementById('splitTopicOverlay').classList.remove('active');
}

function renderSplitGroups() {
    renderSplitGroupTabs();
    renderSplitPostList();
    renderSplitGroupForms();
    // Show/hide add group button
    var addBtn = document.getElementById('splitAddGroupBtn');
    if (addBtn) addBtn.style.display = SPLIT_GROUP_COUNT >= SPLIT_MAX_GROUPS ? 'none' : '';
}

function renderSplitGroupTabs() {
    var container = document.getElementById('splitGroupTabs');
    container.innerHTML = '';
    for (var i = 0; i <= SPLIT_GROUP_COUNT; i++) {
        var count = 0;
        Object.keys(SPLIT_GROUPS).forEach(function(tid) { if (SPLIT_GROUPS[tid] === i) count++; });
        var pill = document.createElement('span');
        pill.style.cssText = 'display:inline-flex; align-items:center; gap:4px; padding:3px 10px; border-radius:12px; font-size:11px; font-weight:600; border:2px solid ' + SPLIT_GROUP_COLORS[i] + '; color:' + SPLIT_GROUP_COLORS[i] + ';';
        pill.textContent = SPLIT_GROUP_LABELS[i] + ' (' + count + ')';
        container.appendChild(pill);
    }
}

function renderSplitPostList() {
    var container = document.getElementById('splitPostList');
    container.innerHTML = '';
    SPLIT_POSTS.forEach(function(p) {
        var grp = SPLIT_GROUPS[p.tweet_id] || 0;
        var div = document.createElement('div');
        div.className = 'split-post-item';
        div.style.cssText = 'padding:6px 8px; margin-bottom:4px; border-radius:6px; cursor:pointer; font-size:11px; border:1px solid var(--border); border-left:3px solid ' + SPLIT_GROUP_COLORS[grp] + '; transition:all 0.15s;';
        div.innerHTML = '<span style="font-size:9px; font-weight:700; color:' + SPLIT_GROUP_COLORS[grp] + '; margin-right:6px;">' + SPLIT_GROUP_LABELS[grp] + '</span>'
            + '<strong style="color:var(--accent);">@' + _topicSearchEsc(p.author_username) + '</strong> '
            + '<span style="color:var(--text-muted);">' + (p.created_at || '').substring(0, 10) + '</span><br>'
            + '<span style="color:var(--text-secondary);">' + _topicSearchEsc((p.text || '').substring(0, 150)) + '</span>';
        div.onclick = (function(tid) { return function() { cycleSplitGroup(tid); }; })(p.tweet_id);
        container.appendChild(div);
    });
}

function cycleSplitGroup(tweetId) {
    var current = SPLIT_GROUPS[tweetId] || 0;
    SPLIT_GROUPS[tweetId] = (current + 1) % (SPLIT_GROUP_COUNT + 1);
    renderSplitGroupTabs();
    renderSplitPostList();
}

function renderSplitGroupForms() {
    var container = document.getElementById('splitGroupForms');
    // Save current form values before re-rendering
    var saved = {};
    for (var s = 0; s <= SPLIT_GROUP_COUNT; s++) {
        saved[s] = _readSplitGroupForm(s);
    }
    container.innerHTML = '';

    // Source form (group 0)
    container.innerHTML += _buildGroupFormHtml(0, 'Source Topic Updates', false);

    // New topic forms
    for (var i = 1; i <= SPLIT_GROUP_COUNT; i++) {
        container.innerHTML += _buildGroupFormHtml(i, SPLIT_GROUP_LABELS[i], true);
    }

    // Restore saved values
    for (var r = 0; r <= SPLIT_GROUP_COUNT; r++) {
        _writeSplitGroupForm(r, saved[r] || SPLIT_GROUP_META[r] || {});
    }

    // Set up subcategory dropdowns
    for (var u = 0; u <= SPLIT_GROUP_COUNT; u++) {
        updateSplitSubcatsForGroup(u);
    }
}

function _readSplitGroupForm(idx) {
    var prefix = 'splitGrp' + idx;
    var result = {};
    var descEl = document.getElementById(prefix + 'Desc');
    if (descEl) result.description = descEl.value;
    var ktEl = document.getElementById(prefix + 'KeyTakeaways');
    if (ktEl) result.key_takeaways = ktEl.value;
    var blEl = document.getElementById(prefix + 'Bullets');
    if (blEl) result.bullets = blEl.value;
    var nameEl = document.getElementById(prefix + 'Name');
    if (nameEl) result.name = nameEl.value;
    var catEl = document.getElementById(prefix + 'Category');
    if (catEl) result.category = catEl.value;
    var subEl = document.getElementById(prefix + 'Subcategory');
    if (subEl) result.subcategory = subEl.value;
    return result;
}

function _writeSplitGroupForm(idx, vals) {
    if (!vals) return;
    var prefix = 'splitGrp' + idx;
    var descEl = document.getElementById(prefix + 'Desc');
    if (descEl && vals.description !== undefined) descEl.value = vals.description;
    var ktEl = document.getElementById(prefix + 'KeyTakeaways');
    if (ktEl && vals.key_takeaways !== undefined) ktEl.value = vals.key_takeaways;
    var blEl = document.getElementById(prefix + 'Bullets');
    if (blEl && vals.bullets !== undefined) blEl.value = vals.bullets;
    var nameEl = document.getElementById(prefix + 'Name');
    if (nameEl && vals.name !== undefined) nameEl.value = vals.name;
    var catEl = document.getElementById(prefix + 'Category');
    if (catEl && vals.category !== undefined) catEl.value = vals.category;
    var subEl = document.getElementById(prefix + 'Subcategory');
    if (subEl && vals.subcategory !== undefined) {
        // Subcategory needs options populated first
        updateSplitSubcatsForGroup(idx);
        setTimeout(function() {
            var el = document.getElementById(prefix + 'Subcategory');
            if (el) el.value = vals.subcategory;
        }, 30);
    }
}

function _buildGroupFormHtml(idx, title, includeNameAndCat) {
    var prefix = 'splitGrp' + idx;
    var color = SPLIT_GROUP_COLORS[idx];
    var removeBtn = (idx > 1 && includeNameAndCat)
        ? ' <button class="tf-btn tf-btn-cancel" style="font-size:10px;padding:2px 8px;margin-left:auto;" onclick="removeSplitGroup(' + idx + ')">Remove</button>'
        : '';
    var catOptions = '<option value="">-- Select --</option>';
    if (window.XFI_BOOTSTRAP && window.XFI_BOOTSTRAP.taxonomy) {
        Object.keys(window.XFI_BOOTSTRAP.taxonomy).forEach(function(k) {
            var info = window.XFI_BOOTSTRAP.taxonomy[k];
            catOptions += '<option value="' + k + '">' + (info.icon || '') + ' ' + (info.label || k) + '</option>';
        });
    }

    var html = '<div style="border-left:3px solid ' + color + '; padding-left:12px; margin-bottom:16px;">'
        + '<div style="display:flex; align-items:center; font-size:11px; font-weight:600; color:' + color + '; margin-bottom:6px;">' + _topicSearchEsc(title) + removeBtn + '</div>';

    if (includeNameAndCat) {
        html += '<div class="tf-row"><label class="tf-label">Name</label><input class="tf-input" id="' + prefix + 'Name"></div>';
    }
    html += '<div class="tf-row"><label class="tf-label">Description</label><textarea class="tf-textarea" id="' + prefix + 'Desc" rows="2"></textarea></div>';
    html += '<div class="tf-row"><label class="tf-label">Key Takeaways <span style="color:var(--text-muted);font-weight:400;">(one per line)</span></label><textarea class="tf-textarea" id="' + prefix + 'KeyTakeaways" rows="2"></textarea></div>';
    html += '<div class="tf-row"><label class="tf-label">Summary Bullets <span style="color:var(--text-muted);font-weight:400;">(one per line)</span></label><textarea class="tf-textarea" id="' + prefix + 'Bullets" rows="3"></textarea></div>';

    if (includeNameAndCat) {
        html += '<div class="tf-row controls-inline">'
            + '<div style="flex:1;"><label class="tf-label">Category</label>'
            + '<select class="tf-select" id="' + prefix + 'Category" onchange="updateSplitSubcatsForGroup(' + idx + ')">'
            + catOptions + '</select></div>'
            + '<div style="flex:1;"><label class="tf-label">Subcategory</label>'
            + '<select class="tf-select" id="' + prefix + 'Subcategory"><option value="">-- Select --</option></select></div>'
            + '</div>';
    }
    html += '</div>';
    return html;
}

function updateSplitSubcatsForGroup(idx) {
    var prefix = 'splitGrp' + idx;
    var catEl = document.getElementById(prefix + 'Category');
    var subEl = document.getElementById(prefix + 'Subcategory');
    if (!catEl || !subEl) return;
    var cat = catEl.value;
    subEl.innerHTML = '<option value="">-- Select --</option>';
    if (!cat || !window.XFI_BOOTSTRAP || !window.XFI_BOOTSTRAP.taxonomy) return;
    var info = window.XFI_BOOTSTRAP.taxonomy[cat];
    if (!info || !info.subcategories) return;
    Object.keys(info.subcategories).forEach(function(k) {
        var opt = document.createElement('option');
        opt.value = k;
        opt.textContent = k.replace(/_/g, ' ').replace(/\b\w/g, function(c) { return c.toUpperCase(); });
        subEl.appendChild(opt);
    });
}

function addSplitGroup() {
    if (SPLIT_GROUP_COUNT >= SPLIT_MAX_GROUPS) return;
    SPLIT_GROUP_COUNT++;
    renderSplitGroups();
}

function removeSplitGroup(idx) {
    if (idx <= 1 || idx > SPLIT_GROUP_COUNT) return;
    // Reassign posts from removed group back to source
    Object.keys(SPLIT_GROUPS).forEach(function(tid) {
        if (SPLIT_GROUPS[tid] === idx) SPLIT_GROUPS[tid] = 0;
        else if (SPLIT_GROUPS[tid] > idx) SPLIT_GROUPS[tid]--;
    });
    // Shift saved meta down
    for (var i = idx; i < SPLIT_GROUP_COUNT; i++) {
        SPLIT_GROUP_META[i] = SPLIT_GROUP_META[i + 1] || {};
    }
    delete SPLIT_GROUP_META[SPLIT_GROUP_COUNT];
    SPLIT_GROUP_COUNT--;
    renderSplitGroups();
}

function generateSplitSuggestion() {
    var topicId = document.getElementById('splitSourceId').value;
    // Build groups array (only non-source groups with posts)
    var groups = [];
    for (var i = 1; i <= SPLIT_GROUP_COUNT; i++) {
        var ids = [];
        Object.keys(SPLIT_GROUPS).forEach(function(tid) { if (SPLIT_GROUPS[tid] === i) ids.push(tid); });
        if (ids.length === 0) {
            document.getElementById('splitSuggestStatus').textContent = SPLIT_GROUP_LABELS[i] + ' has no posts assigned';
            document.getElementById('splitSuggestStatus').style.color = '#e87474';
            return;
        }
        groups.push({ group_index: i, post_ids: ids });
    }
    var btn = document.getElementById('splitSuggestBtn');
    var status = document.getElementById('splitSuggestStatus');
    btn.disabled = true;
    status.textContent = 'Generating... (may take a few seconds)';
    status.style.color = 'var(--text-muted)';

    fetch('/api/topics/' + topicId + '/split/suggest', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ groups: groups })
    })
    .then(function(r) { return r.json(); })
    .then(function(data) {
        btn.disabled = false;
        if (data.ok && data.suggestion) {
            status.textContent = 'Done';
            status.style.color = 'var(--success)';
            var s = data.suggestion;
            // Apply source suggestion
            if (s.source) {
                var srcMeta = {};
                if (s.source.description) srcMeta.description = s.source.description;
                if (s.source.key_takeaways) srcMeta.key_takeaways = s.source.key_takeaways.join('\n');
                if (s.source.bullets) srcMeta.bullets = s.source.bullets.join('\n');
                _writeSplitGroupForm(0, srcMeta);
            }
            // Apply new topic suggestions
            var newTopics = s.new_topics || (s.new_topic ? [s.new_topic] : []);
            newTopics.forEach(function(nt, idx) {
                var grpIdx = idx + 1;
                if (grpIdx > SPLIT_GROUP_COUNT) return;
                var meta = {};
                if (nt.name) meta.name = nt.name;
                if (nt.description) meta.description = nt.description;
                if (nt.key_takeaways) meta.key_takeaways = nt.key_takeaways.join('\n');
                if (nt.bullets) meta.bullets = nt.bullets.join('\n');
                if (nt.category) meta.category = nt.category;
                if (nt.subcategory) meta.subcategory = nt.subcategory;
                _writeSplitGroupForm(grpIdx, meta);
            });
        } else {
            status.textContent = 'Error: ' + (data.error || 'Unknown');
            status.style.color = '#e87474';
        }
    })
    .catch(function(e) {
        btn.disabled = false;
        status.textContent = 'Error: ' + e.message;
        status.style.color = '#e87474';
    });
}

function _parseLinesFromField(elId, maxLines) {
    var el = document.getElementById(elId);
    if (!el) return [];
    var text = el.value.trim();
    if (!text) return [];
    var lines = text.split('\n').map(function(l) { return l.trim(); }).filter(Boolean);
    return maxLines ? lines.slice(0, maxLines) : lines;
}

function submitSplit() {
    var topicId = document.getElementById('splitSourceId').value;
    var statusEl = document.getElementById('splitStatus');

    // Build new_topics array
    var newTopics = [];
    for (var i = 1; i <= SPLIT_GROUP_COUNT; i++) {
        var prefix = 'splitGrp' + i;
        var nameEl = document.getElementById(prefix + 'Name');
        var name = nameEl ? nameEl.value.trim() : '';
        if (!name) {
            statusEl.textContent = SPLIT_GROUP_LABELS[i] + ' name is required';
            statusEl.style.color = '#e87474';
            return;
        }
        var ids = [];
        Object.keys(SPLIT_GROUPS).forEach(function(tid) { if (SPLIT_GROUPS[tid] === i) ids.push(tid); });
        if (!ids.length) {
            statusEl.textContent = SPLIT_GROUP_LABELS[i] + ' has no posts assigned';
            statusEl.style.color = '#e87474';
            return;
        }
        newTopics.push({
            post_ids: ids,
            name: name,
            description: (document.getElementById(prefix + 'Desc') || {}).value || '',
            summary_key_takeaways: _parseLinesFromField(prefix + 'KeyTakeaways', 2),
            summary_bullets: _parseLinesFromField(prefix + 'Bullets'),
            category: (document.getElementById(prefix + 'Category') || {}).value || null,
            subcategory: (document.getElementById(prefix + 'Subcategory') || {}).value || null
        });
    }

    // Source updates
    var sourceUpdates = {
        description: (document.getElementById('splitGrp0Desc') || {}).value || '',
        summary_key_takeaways: _parseLinesFromField('splitGrp0KeyTakeaways', 2),
        summary_bullets: _parseLinesFromField('splitGrp0Bullets')
    };

    var payload = { new_topics: newTopics, source_updates: sourceUpdates };

    var btn = document.getElementById('splitSubmitBtn');
    btn.disabled = true;
    statusEl.textContent = 'Splitting...';
    statusEl.style.color = 'var(--text-muted)';

    fetch('/api/topics/' + topicId + '/split', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload)
    })
    .then(function(r) { return r.json(); })
    .then(function(data) {
        if (data.ok) {
            statusEl.textContent = 'Split complete! Reloading...';
            statusEl.style.color = 'var(--success)';
            setTimeout(function() {
                closeSplitModal();
                window.location.reload();
            }, 800);
        } else {
            btn.disabled = false;
            statusEl.textContent = 'Error: ' + (data.error || 'Unknown');
            statusEl.style.color = '#e87474';
        }
    })
    .catch(function(e) {
        btn.disabled = false;
        statusEl.textContent = 'Error: ' + e.message;
        statusEl.style.color = '#e87474';
    });
}

/* ── Bullet Watchlist collapse/expand ── */
function toggleBulletSection() {
    var grid = document.getElementById('weeklyBulletGrid');
    var btn = document.getElementById('bulletExpandBtn');
    if (!grid || !btn) return;
    var collapsed = grid.classList.toggle('bullet-collapsed');
    var count = grid.querySelectorAll('.topic-card-sm').length;
    btn.innerHTML = collapsed ? '&#9662; Show all ' + count : '&#9652; Collapse';
    localStorage.setItem('xfi_bullet_collapsed', collapsed ? '1' : '0');
    if (typeof reapplyFading === 'function') setTimeout(reapplyFading, 80);
}

function initBulletCollapse() {
    var grid = document.getElementById('weeklyBulletGrid');
    var btn = document.getElementById('bulletExpandBtn');
    if (!grid || !btn) return;
    var pref = localStorage.getItem('xfi_bullet_collapsed');
    if (pref !== '0') {
        grid.classList.add('bullet-collapsed');
        var count = grid.querySelectorAll('.topic-card-sm').length;
        btn.innerHTML = '&#9662; Show all ' + count;
    } else {
        btn.innerHTML = '&#9652; Collapse';
    }
}
initBulletCollapse();

/* ── Needs Discussion collapse/expand ── */
function toggleUnsureSection() {
    var grid = document.getElementById('weeklyUnsureGrid');
    var btn = document.getElementById('unsureExpandBtn');
    if (!grid || !btn) return;
    var collapsed = grid.classList.toggle('unsure-collapsed');
    var count = grid.querySelectorAll('.topic-card-sm').length;
    btn.innerHTML = collapsed ? '&#9662; Show all ' + count : '&#9652; Collapse';
    localStorage.setItem('xfi_unsure_collapsed', collapsed ? '1' : '0');
    if (typeof reapplyFading === 'function') setTimeout(reapplyFading, 80);
}

function initUnsureCollapse() {
    var grid = document.getElementById('weeklyUnsureGrid');
    var btn = document.getElementById('unsureExpandBtn');
    if (!grid || !btn) return;
    var pref = localStorage.getItem('xfi_unsure_collapsed');
    if (pref !== '0') {
        grid.classList.add('unsure-collapsed');
        var count = grid.querySelectorAll('.topic-card-sm').length;
        btn.innerHTML = '&#9662; Show all ' + count;
    } else {
        btn.innerHTML = '&#9652; Collapse';
    }
}
initUnsureCollapse();

/* ── Cost Tracker toggle ── */
function showHistoryView(view) {
    var fetchEl = document.getElementById('fetch-view');
    var costEl = document.getElementById('cost-view');
    if (!fetchEl || !costEl) return;
    fetchEl.style.display = view === 'fetches' ? '' : 'none';
    costEl.style.display = view === 'costs' ? '' : 'none';
    document.querySelectorAll('.cost-pill').forEach(function(p) {
        var isFetches = p.textContent.trim().toLowerCase() === 'fetches';
        p.classList.toggle('active', (view === 'fetches') === isFetches);
    });
}

/* ── Customs dashboard charts ── */
function _customsEsc(text) {
    var div = document.createElement('div');
    div.appendChild(document.createTextNode(text == null ? '' : String(text)));
    return div.innerHTML;
}

function _customsFormatAxisBillions(value) {
    if (!isFinite(value)) return '0';
    return '$' + value.toFixed(1) + 'B';
}

function _customsParseChartSpec(container) {
    if (!container) return null;
    var sourceId = container.getAttribute('data-customs-chart-source');
    if (!sourceId) return null;
    var script = document.getElementById(sourceId);
    if (!script) return null;
    try {
        return JSON.parse(script.textContent || '{}');
    } catch (_err) {
        return null;
    }
}

function _customsRenderEmpty(container, message) {
    if (!container) return;
    container.innerHTML = '<div class="customs-chart-empty">' + _customsEsc(message || 'No chart data available.') + '</div>';
}

function _customsTooltip(shell) {
    var tip = shell.querySelector('.customs-chart-tooltip');
    if (tip) return tip;
    tip = document.createElement('div');
    tip.className = 'customs-chart-tooltip';
    tip.style.display = 'none';
    shell.appendChild(tip);
    return tip;
}

function _customsPositionTooltip(tip, shell, evt) {
    var shellRect = shell.getBoundingClientRect();
    var left = evt.clientX - shellRect.left;
    var top = evt.clientY - shellRect.top;
    tip.style.left = left + 'px';
    tip.style.top = top + 'px';
}

function _customsShowTooltip(shell, evt, point, seriesName) {
    var tip = _customsTooltip(shell);
    tip.innerHTML =
        '<div class="customs-chart-tooltip-title">' + _customsEsc(point.label || point.period || '') + '</div>' +
        '<div class="customs-chart-tooltip-value">' + _customsEsc(point.display_value || '') + '</div>' +
        '<div class="customs-chart-tooltip-series">' + _customsEsc(seriesName || '') + '</div>';
    tip.style.display = 'block';
    _customsPositionTooltip(tip, shell, evt);
}

function _customsHideTooltip(shell) {
    var tip = shell.querySelector('.customs-chart-tooltip');
    if (!tip) return;
    tip.style.display = 'none';
}

function renderCustomsLineChart(container) {
    var spec = _customsParseChartSpec(container);
    if (!spec || !spec.series || !spec.series.length) {
        _customsRenderEmpty(container, 'No chart data available.');
        return;
    }

    var firstSeries = spec.series[0] || {};
    var points = firstSeries.points || [];
    if (!points.length) {
        _customsRenderEmpty(container, 'No chart points available.');
        return;
    }

    container.innerHTML = '';

    var shell = document.createElement('div');
    shell.className = 'customs-chart-shell';
    container.appendChild(shell);

    var width = Math.max(360, container.clientWidth || 720);
    var height = 286;
    var margin = { top: 18, right: 18, bottom: 42, left: 58 };
    var innerWidth = Math.max(40, width - margin.left - margin.right);
    var innerHeight = Math.max(80, height - margin.top - margin.bottom);
    var yMax = 0;

    spec.series.forEach(function(series) {
        (series.points || []).forEach(function(point) {
            if (typeof point.value === 'number' && isFinite(point.value)) {
                yMax = Math.max(yMax, point.value);
            }
        });
    });
    if (yMax <= 0) yMax = 1;
    yMax = yMax * 1.08;

    function xFor(index) {
        if (points.length <= 1) return margin.left + innerWidth / 2;
        return margin.left + (index * innerWidth / (points.length - 1));
    }

    function yFor(value) {
        var safe = isFinite(value) ? Math.max(0, value) : 0;
        return margin.top + (innerHeight - ((safe / yMax) * innerHeight));
    }

    var svg = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
    svg.setAttribute('viewBox', '0 0 ' + width + ' ' + height);
    svg.setAttribute('width', '100%');
    svg.setAttribute('height', String(height));
    svg.setAttribute('role', 'img');
    svg.setAttribute('aria-label', spec.title || 'Customs chart');
    shell.appendChild(svg);

    var gridCount = 4;
    for (var i = 0; i <= gridCount; i++) {
        var ratio = i / gridCount;
        var y = margin.top + (ratio * innerHeight);
        var line = document.createElementNS('http://www.w3.org/2000/svg', 'line');
        line.setAttribute('x1', String(margin.left));
        line.setAttribute('x2', String(width - margin.right));
        line.setAttribute('y1', String(y));
        line.setAttribute('y2', String(y));
        line.setAttribute('stroke', 'rgba(255,255,255,0.08)');
        line.setAttribute('stroke-width', '1');
        svg.appendChild(line);

        var label = document.createElementNS('http://www.w3.org/2000/svg', 'text');
        label.setAttribute('x', String(margin.left - 8));
        label.setAttribute('y', String(y + 4));
        label.setAttribute('text-anchor', 'end');
        label.setAttribute('font-size', '10');
        label.setAttribute('fill', 'currentColor');
        label.setAttribute('opacity', '0.72');
        label.textContent = _customsFormatAxisBillions(yMax * (1 - ratio));
        svg.appendChild(label);
    }

    var step = Math.max(1, Math.ceil(points.length / 6));
    points.forEach(function(point, index) {
        if ((index % step) !== 0 && index !== points.length - 1) return;
        var x = xFor(index);
        var text = document.createElementNS('http://www.w3.org/2000/svg', 'text');
        text.setAttribute('x', String(x));
        text.setAttribute('y', String(height - 12));
        text.setAttribute('text-anchor', 'middle');
        text.setAttribute('font-size', '10');
        text.setAttribute('fill', 'currentColor');
        text.setAttribute('opacity', '0.72');
        text.textContent = point.label || point.period || '';
        svg.appendChild(text);
    });

    spec.series.forEach(function(series) {
        var seriesPoints = series.points || [];
        if (!seriesPoints.length) return;
        var d = '';
        seriesPoints.forEach(function(point, index) {
            var x = xFor(index);
            var y = yFor(point.value || 0);
            d += (index === 0 ? 'M' : ' L') + x + ' ' + y;
        });

        var path = document.createElementNS('http://www.w3.org/2000/svg', 'path');
        path.setAttribute('d', d);
        path.setAttribute('fill', 'none');
        path.setAttribute('stroke', series.color || '#c4956a');
        path.setAttribute('stroke-width', '2.5');
        path.setAttribute('stroke-linecap', 'round');
        path.setAttribute('stroke-linejoin', 'round');
        svg.appendChild(path);

        seriesPoints.forEach(function(point, index) {
            var circle = document.createElementNS('http://www.w3.org/2000/svg', 'circle');
            circle.setAttribute('cx', String(xFor(index)));
            circle.setAttribute('cy', String(yFor(point.value || 0)));
            circle.setAttribute('r', '3.7');
            circle.setAttribute('fill', series.color || '#c4956a');
            circle.setAttribute('stroke', 'rgba(26,24,22,0.85)');
            circle.setAttribute('stroke-width', '1.5');
            circle.style.cursor = 'pointer';
            circle.addEventListener('mouseenter', function(evt) {
                _customsShowTooltip(shell, evt, point, series.name);
            });
            circle.addEventListener('mousemove', function(evt) {
                _customsPositionTooltip(_customsTooltip(shell), shell, evt);
            });
            circle.addEventListener('mouseleave', function() {
                _customsHideTooltip(shell);
            });
            svg.appendChild(circle);
        });
    });

    var legend = document.createElement('div');
    legend.className = 'customs-chart-legend';
    spec.series.forEach(function(series) {
        var item = document.createElement('div');
        item.className = 'customs-chart-legend-item';
        item.innerHTML =
            '<span class="customs-chart-swatch" style="background:' + _customsEsc(series.color || '#c4956a') + ';"></span>' +
            '<span>' + _customsEsc(series.name || '') + '</span>';
        legend.appendChild(item);
    });
    container.appendChild(legend);
}

function initCustomsDashboard() {
    var charts = document.querySelectorAll('[data-customs-chart-source]');
    if (!charts.length) return;
    charts.forEach(function(chart) {
        renderCustomsLineChart(chart);
    });
}

window.addEventListener('load', function() {
    setTimeout(initCustomsDashboard, 0);
});

window.addEventListener('resize', function() {
    if (!document.querySelector('[data-customs-chart-source]')) return;
    clearTimeout(window.__xfiCustomsChartResizeTimer);
    window.__xfiCustomsChartResizeTimer = setTimeout(initCustomsDashboard, 100);
});
