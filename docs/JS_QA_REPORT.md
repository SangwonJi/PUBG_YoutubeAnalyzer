# JavaScript QA Report — index.html

**Date:** 2025-03-18  
**Scope:** Full JavaScript correctness and runtime error audit

---

## 1. Null Reference Errors

### CRITICAL — No null guard, will throw

| Line | Code | Element ID/Selector | Impact |
|------|------|--------------------|--------|
| 2745 | `document.getElementById('collabTab').classList.add('active')` | `collabTab` | Throws if element missing. Called from `switchGame()` when exiting search mode. |
| 2855 | `document.getElementById('searchTab').classList.add('active')` | `searchTab` | Throws if element missing. Called from `switchToSearch()`. |
| 2980-2982 | `searchInput.cloneNode(true)` / `searchInput.parentNode.replaceChild(...)` | `searchInput` | Throws if `#searchInput` is null. Called from `initDashboard()`. |
| 3036-3038 | `document.querySelector('#collabVideos').closest('.stat-card').querySelector('.stat-label')` | `#collabVideos` | **Throws** if `#collabVideos` is null. If `.stat-card` or `.stat-label` is missing, `.querySelector('.stat-label')` could return null → `videosLabel.textContent` throws. |
| 3198-3216 | `document.getElementById('emptyState')`, `detailContent`, `detailTitle`, etc. | Multiple | **Throws** if any is null. `selectPartner()` assumes DOM structure. |
| 3247 | `list.innerHTML = videos.map(...)` | `videosList` | `list` from `document.getElementById('videosList')` — if null, throws. |
| 3293 | `container.innerHTML = html` | `contentGroups` | `container` from `document.getElementById('contentGroups')` — if null, throws. |
| 3383-3391 | `detail.classList.add`, `document.getElementById('noncollabDetailTitle')`, etc. | `noncollabDetail`, `noncollabDetailTitle`, etc. | **Throws** if null. `selectContentType()` assumes DOM. |
| 3391 | `list.innerHTML = ...` | `noncollabVideosList` | If `list` is null, throws. |
| 3869 | `document.getElementById('topPartnersChart').getContext('2d')` | `topPartnersChart` | **Throws** if canvas is null. Only reached when `top10.length > 0`. |
| 3960 | `new Chart(document.getElementById('collabRatioChart'), ...)` | `collabRatioChart` | **Throws** if null. |
| 4032 | `new Chart(document.getElementById('categoryChart'), ...)` | `categoryChart` | **Throws** if null. |
| 4083 | `new Chart(document.getElementById('contentTypeChart'), ...)` | `contentTypeChart` | **Throws** if null. Inside `if (othersData && othersData.content_types)` but no canvas null check. |
| 4905 | `ac.classList.remove('open'); ac.innerHTML = ''` | `searchAutocomplete` | **Throws** if `ac` is null. In `onSearchInput()` early return path. |
| 4941-4942 | `ac.querySelectorAll('.search-ac-item')` | `searchAutocomplete` | **Throws** if `ac` is null. In `onSearchKeydown()`. |
| 4957-4960 | `ac.classList.remove('open')`, `document.getElementById('globalSearchInput').value = ...` | `searchAutocomplete`, `globalSearchInput` | **Throws** if null. In `selectSearchPartner()`. |
| 4989-5043 | Multiple `document.getElementById(...)` in `renderSearchResult()` | `searchResult`, `searchEmpty`, `searchResultTitle`, `srTotalViews`, etc. | **Throws** if any is null. |
| 5127-5131 | `document.getElementById('chatMessages')`, `chatWelcome`, `chatModelBadge`, `chatInput` | Chat elements | **Throws** if null. In `clearChat()`. |
| 5137 | `document.getElementById('chatInput').value = text` | `chatInput` | **Throws** if null. In `sendQuickQ()`. |
| 5164-5166 | `chatWelcome`, `chatMessages`, `chatTyping` | Chat elements | **Throws** if null. In `appendChatMsg()`. |
| 5281-5282 | `chatSendBtn`, `chatTyping` | Chat elements | **Throws** if null. In `sendChat()`. |
| 3443-3472 | `badge.style.display`, `textEl.textContent` | `lastUpdatedBadge`, `lastUpdatedText` | **Throws** if null. In `updateLastUpdated()`. |

### HIGH — `document.querySelector('.logo')` no null guard

| Line | Code | Risk |
|------|------|------|
| 2459 | `document.querySelector('.logo').textContent = gameName` | Throws if no `.logo` element |
| 2532 | Same | Same |

### MEDIUM — Optional chaining or guard present (OK)

- 2295: `theme-toggle` — `if (btn)` guarded
- 2461, 2534: `.subtitle` — `if (subtitleEl && channel.dateRange)` guarded
- 2620-2627: breadcrumb/logo — guarded
- 2685-2690: `sidebarCompareSection`, `sidebarCompareBtn` — guarded
- 2754: `headerLogoImg` — guarded
- 2805: `tab + 'Tab'` — `if (target)` guarded
- 2837: `chatInput` — optional chaining
- 3433-3438: `navSidebar`, `sidebarOverlay` — optional chaining
- 3534-3536: `heatmapContainer`, `heatmapSummary` — guarded
- 3641, 3650: `heatmapTooltip` — guarded
- 3656: `treemapContainer` — guarded
- 4190-4191: `timelineChart` — `if (!canvas) return`
- 4442-4454: Compare tab elements — guarded
- 4508, 4544: `partnerViewsChart`, `partnerVideosChart` — `?.getContext('2d')`
- 5356, 5361, 5367: Chat elements — optional chaining / `if (sendBtn)`

---

## 2. Function References

All `onclick` handlers and `setTimeout` callbacks reference existing functions. No undefined function calls found.

| onclick handler | Function exists |
|-----------------|-----------------|
| `closeMobileSidebar()`, `openMobileSidebar()` | ✓ |
| `switchGame()`, `switchTab()`, `switchPlatform()`, `switchPlatformType()` | ✓ |
| `setLanguage()`, `toggleTheme()`, `filterCategory()` | ✓ |
| `switchTimelineMode()`, `setCompareTopN()`, `switchCompareMetric()` | ✓ |
| `clearChat()`, `sendQuickQ()`, `sendChat()` | ✓ |
| `selectPartner()`, `toggleFavorite()`, `selectContentType()` | ✓ |
| `selectComparePartner()`, `selectSearchPartner()` | ✓ |

---

## 3. Variable Scoping

- **`partnersData`**, **`othersData`**: Top-level. ✓
- **`_searchHighlight`**: Used in `onSearchKeydown` before possible assignment. If user presses ArrowDown before typing, `_searchHighlight` may be undefined. `Math.min(undefined + 1, ...)` → `NaN`. **LOW** — edge case.
- **`_searchIndex`**, **`_searchCanonical`**: Set in `initSearchTab()` before search is used. ✓

---

## 4. compareTabBtn Removal Impact

**No references to `compareTabBtn` found.** Compare functionality uses `sidebarCompareBtn` (id `sidebarCompareBtn`) in the sidebar. No orphaned references. ✓

---

## 5. Event Listener Issues

| Issue | Location | Severity |
|------|----------|----------|
| **Duplicate listeners** | `initDashboard()` clones `searchInput` and replaces it, then adds a new `input` listener. Old listener is removed with the old node. ✓ |
| **`document.addEventListener('click', ...)` in `initPartnerSearch()`** | Line ~4802. Listener added once. No cleanup on tab switch — stays attached. **LOW** — minor memory/behavior impact. |
| **`document.addEventListener('click', ...)` for search autocomplete** | Line 5105. Same — no cleanup. **LOW** |
| **`globalSearchInput` listeners** | Added in IIFE at 2786-2792. `onSearchInput` and `onSearchKeydown` may run before `initSearchTab` — `_searchIndex` is null, early return. ✓ |
| **Listeners on possibly null elements** | `inp` at 2787: `if (inp)` guards. ✓ |

---

## 6. Race Conditions

| Scenario | Handling |
|----------|----------|
| **Data load after tab switch** | `loadRequestId` and `thisRequestId !== loadRequestId` guard in `loadChannelData` (2524). ✓ |
| **Async `initCompareTab` after tab switch** | No guard. If user switches away before `initCompareTab` completes, it may update DOM for a tab that is no longer visible. **MEDIUM** — no throw, but possible stale UI. |
| **`initSearchTab` after tab switch** | Same pattern. **MEDIUM** |
| **Chat stream after tab switch** | `msgEl` and `assistantText` updated in stream. If user switches tabs, updates continue. **LOW** — no crash. |

---

## 7. Edge Cases

| Edge case | Behavior | Severity |
|-----------|----------|----------|
| **All data files fail to load** | `loadChannelData` catch sets `partnersData = []`, `othersData = null`, calls `initDashboard()` and `showToast()`. Dashboard renders with empty state. ✓ |
| **0 partners** | `partnersData.length === 0` — `updateCollabStats` animates 0; `initCharts` returns early when `top10.length === 0`; `renderPartnersList` shows empty list. ✓ |
| **Chat worker down** | `sendChat` catch appends error message, re-enables send button. ✓ |
| **Division by zero** | `likeRate` (3213): `partner.total_views > 0` guard. ✓ |
| | `collabPct` (3950): `totalAll > 0` guard. ✓ |
| | Tooltip (4000): `(ctx.raw / totalAll) * 100` — if `totalAll === 0`, `0/0` → `NaN`, tooltip shows "NaN". **LOW** |
| | Content type chart (4116): `(ctx.raw / total) * 100` — if `total === 0`, same. **LOW** |
| | `getLevel` in heatmap (3577): `c / maxCount` — when `maxCount === 0`, `if (maxCount <= 4) return 'l' + Math.min(c, 4)` runs first, so no division. ✓ |
| **`othersData` null** | `updateNoncollabStats` returns early (3054). `renderContentGroups` returns early (3291). `selectContentType` uses `(othersData.content_types \|\| [])`. ✓ |
| **`perPlat` empty in `renderPartnerCompareChart`** | `perPlat.reduce(..., perPlat[0])` — if `allKeys` is empty, `perPlat` is empty, `perPlat[0]` is `undefined`. `bestPlat.views` would throw. But `allKeys.length < 2` causes early return in `initCompareTab`, and `renderPartnerCompareChart` is only called with valid partner. **LOW** — theoretical. |

---

## Summary

| Severity | Count | Visible to user? |
|----------|-------|------------------|
| **CRITICAL** | 22+ | Yes — uncaught TypeError, page break |
| **HIGH** | 2 | Yes — `.logo` null |
| **MEDIUM** | 2 | Possible — race conditions |
| **LOW** | 5+ | Minor — NaN in tooltips, listener cleanup |

**Recommendation:** Add null guards for all `document.getElementById` / `document.querySelector` results before property access, especially in `selectPartner`, `selectContentType`, `updateCollabStats`, `onSearchInput`, `onSearchKeydown`, `selectSearchPartner`, `renderSearchResult`, chat functions, and `updateLastUpdated`. The DOM structure appears stable, so these are defensive fixes rather than fixes for known missing elements.
