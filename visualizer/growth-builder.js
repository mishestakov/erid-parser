(() => {
  "use strict";

  async function fetchJson(url, options) {
    const res = await fetch(url, options);
    const text = await res.text();
    if (!res.ok) throw new Error(`Request failed: ${res.status}`);
    return text ? JSON.parse(text) : {};
  }

  const newBtn = document.getElementById("new-search-btn");
  const savedListEl = document.getElementById("saved-list");
  const builderTitle = document.getElementById("builder-title");
  const searchNameInput = document.getElementById("search-name");
  const idsInput = document.getElementById("ids-input");
  const dateFromInput = document.getElementById("date-from");
  const dateToInput = document.getElementById("date-to");
  const viewsMinInput = document.getElementById("views-min");
  const viewsMaxInput = document.getElementById("views-max");
  const limitInput = document.getElementById("limit-input");
  const sortPatternSelect = document.getElementById("sort-pattern-select");
  const minPointsInput = document.getElementById("min-points");
  const saveOpenBtn = document.getElementById("save-open-btn");
  const openSavedBtn = document.getElementById("open-saved-btn");
  const previewTotalEl = document.getElementById("preview-total");
  const statusBar = document.getElementById("status-bar");
  const dbStatsEl = document.getElementById("db-stats");
  const refreshStatsBtn = document.getElementById("refresh-stats-btn");

  let savedSets = [];
  let activeSavedId = null;
  let patternTags = [];

  function setStatus(msg) {
    if (statusBar) statusBar.textContent = msg || "";
  }

  function formatBytes(bytes) {
    if (!Number.isFinite(bytes) || bytes < 0) return "—";
    const units = ["Б", "КБ", "МБ", "ГБ", "ТБ"];
    let val = bytes;
    let idx = 0;
    while (val >= 1000 && idx < units.length - 1) {
      val /= 1024;
      idx += 1;
    }
    const precision = val >= 10 || idx === 0 ? 0 : 1;
    return `${val.toFixed(precision)} ${units[idx]}`;
  }

  function parseIdsTextarea() {
    const raw = (idsInput?.value || "").trim();
    if (!raw) return [];
    const lines = raw.split(/\r?\n/);
    const res = [];
    for (const line of lines) {
      const parts = line.trim().split(/\s+/);
      if (parts.length < 2) continue;
      const chatId = Number(parts[0]);
      const messageId = Number(parts[1]);
      if (Number.isFinite(chatId) && Number.isFinite(messageId)) {
        res.push({ chat_id: chatId, message_id: messageId });
      }
    }
    return res;
  }

  function readFilters() {
    const ids = parseIdsTextarea();
    const fromDate = dateFromInput.value ? Math.floor(new Date(dateFromInput.value).getTime() / 1000) : undefined;
    const toDate = dateToInput.value ? Math.floor(new Date(dateToInput.value).getTime() / 1000) : undefined;
    const minViewsRaw = Number(viewsMinInput.value);
    const maxViewsRaw = Number(viewsMaxInput.value);
    const min_views = Number.isFinite(minViewsRaw) && minViewsRaw > 0 ? minViewsRaw : undefined;
    const max_views = Number.isFinite(maxViewsRaw) && maxViewsRaw > 0 ? maxViewsRaw : undefined;
    const limitVal = Number(limitInput.value);
    const limit = Number.isFinite(limitVal) && limitVal > 0 ? Math.min(limitVal, 5000) : 5000;
    const sort_pattern = sortPatternSelect.value || null;
    const min_points_raw = Number(minPointsInput.value);
    const min_points = Number.isFinite(min_points_raw) && min_points_raw > 0 ? min_points_raw : undefined;
    return { ids, from_date: fromDate, to_date: toDate, min_views, max_views, limit, sort_pattern, min_points };
  }

  function fillFilters(filters, disabled) {
    idsInput.value = (filters.ids || []).map((p) => `${p.chat_id}\t${p.message_id}`).join("\n");
    dateFromInput.value = filters.from_date ? new Date(filters.from_date * 1000).toISOString().slice(0, 16) : "";
    dateToInput.value = filters.to_date ? new Date(filters.to_date * 1000).toISOString().slice(0, 16) : "";
    viewsMinInput.value = filters.min_views ?? "";
    viewsMaxInput.value = filters.max_views ?? "";
    limitInput.value = filters.limit ?? 5000;
    sortPatternSelect.value = filters.sort_pattern || "";
    minPointsInput.value = filters.min_points ?? "";
    [searchNameInput, idsInput, dateFromInput, dateToInput, viewsMinInput, viewsMaxInput, limitInput, sortPatternSelect, minPointsInput].forEach((el) => {
      if (el) el.disabled = Boolean(disabled);
    });
    saveOpenBtn.disabled = Boolean(disabled);
    openSavedBtn.disabled = !disabled;
  }

  function resetForm() {
    activeSavedId = null;
    builderTitle.textContent = "Новая подборка";
    fillFilters({}, false);
    searchNameInput.value = "";
    previewTotalEl.textContent = "";
    openSavedBtn.disabled = true;
    saveOpenBtn.disabled = false;
    setStatus("");
  }

  async function loadPatternTags() {
    try {
      const data = await fetchJson("/api/pattern-tags");
      patternTags = Array.isArray(data.tags) ? data.tags : [];
      sortPatternSelect.innerHTML = ["", ...patternTags]
        .map((opt) => `<option value="${opt}">${opt || "Исходный порядок"}</option>`)
        .join("");
    } catch (err) {
      console.error("pattern tags failed", err);
    }
  }

  async function loadSavedSets() {
    try {
      const data = await fetchJson("/api/sets");
      savedSets = Array.isArray(data.sets) ? data.sets : [];
      renderSaved();
    } catch (err) {
      console.error(err);
      savedSets = [];
      renderSaved();
    }
  }

  function renderSaved() {
    savedListEl.innerHTML = "";
    if (!savedSets.length) {
      savedListEl.innerHTML = '<div class="muted">Нет сохранённых подборок</div>';
      return;
    }
    savedSets
      .slice()
      .sort((a, b) => (b.created_at || "").localeCompare(a.created_at || ""))
      .forEach((s) => {
        const row = document.createElement("div");
        row.className = "saved-item";
        row.innerHTML = `
          <div class="saved-main">
            <div class="saved-name">${s.name || "(без названия)"}</div>
            <div class="muted mini">${s.total ?? "?"} постов</div>
          </div>
          <div class="saved-actions">
            <button data-action="open">Открыть</button>
            <button data-action="delete" class="danger">×</button>
          </div>
        `;
        row.addEventListener("click", async (e) => {
          const action = e.target.getAttribute("data-action");
          if (action === "delete") {
            e.stopPropagation();
            await deleteSet(s.id);
            return;
          }
          selectSaved(s.id);
        });
        savedListEl.appendChild(row);
      });
  }

  async function deleteSet(id) {
    try {
      await fetchJson(`/api/sets/${id}`, { method: "DELETE" });
      savedSets = savedSets.filter((s) => s.id !== id);
      renderSaved();
      if (activeSavedId === id) resetForm();
    } catch (err) {
      console.error(err);
      setStatus("Не удалось удалить подборку");
    }
  }

  async function selectSaved(id) {
    const found = savedSets.find((s) => s.id === id);
    if (!found) return;
    try {
      setStatus("Загружаю подборку…");
      const data = await fetchJson(`/api/sets/${id}`);
      const set = data.set;
      activeSavedId = id;
      builderTitle.textContent = set?.name || "Подборка";
      searchNameInput.value = set?.name || "";
      fillFilters(set?.filters || {}, true);
      previewTotalEl.textContent = `Постов: ${set?.total ?? "?"}`;
      openSavedBtn.disabled = false;
      saveOpenBtn.disabled = true;
      setStatus("");
    } catch (err) {
      console.error(err);
      setStatus("Не удалось загрузить подборку");
    }
  }

  let previewTimer = null;
  async function runPreview() {
    try {
      const filters = readFilters();
      const result = await fetchJson("/api/list", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ ...filters, limit: 1 })
      });
      previewTotalEl.textContent = `Оценка: ${result.total || 0} постов`;
    } catch (err) {
      console.error(err);
      previewTotalEl.textContent = "Ошибка предпросмотра";
    }
  }

  function schedulePreview() {
    if (previewTimer) clearTimeout(previewTimer);
    previewTotalEl.textContent = "Обновляю…";
    previewTimer = setTimeout(runPreview, 400);
  }

  async function handleSaveAndOpen() {
    const name = (searchNameInput.value || "").trim();
    if (!name) {
      setStatus("Укажите название подборки");
      return;
    }
    try {
      const filters = readFilters();
      setStatus("Строю подборку…");
      const result = await fetchJson("/api/sets", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ name, filters })
      });
      await loadSavedSets();
      setStatus("");
      if (result?.set?.id) {
        window.location.href = `/growth-viewer.html?id=${encodeURIComponent(result.set.id)}`;
      }
    } catch (err) {
      console.error(err);
      setStatus(err.message || "Не удалось сохранить подборку");
    }
  }

  function handleOpenSaved() {
    if (!activeSavedId) return;
    window.location.href = `/growth-viewer.html?id=${encodeURIComponent(activeSavedId)}`;
  }

  async function loadDbStats() {
    if (!dbStatsEl) return;
    dbStatsEl.textContent = "Загружаю…";
    try {
      const data = await fetchJson("/api/db-stats");
      const disk = data.disk || {};
      const db = data.db || {};
      const tables = Array.isArray(data.tables) ? data.tables : [];
      const top = tables
        .slice(0, 3)
        .map((t) => `${t.name}: ${formatBytes(t.size_bytes)}`)
        .join("; ");
      dbStatsEl.textContent = `Диск: ${formatBytes(disk.free_bytes)} из ${formatBytes(disk.total_bytes)} | БД: ${formatBytes(
        db.size_bytes
      )} | Таблицы: ${top || "нет данных"}`;
    } catch (err) {
      console.error(err);
      dbStatsEl.textContent = "Не удалось получить статистику";
    }
  }

  // bind events
  newBtn?.addEventListener("click", resetForm);
  saveOpenBtn?.addEventListener("click", handleSaveAndOpen);
  openSavedBtn?.addEventListener("click", handleOpenSaved);
  refreshStatsBtn?.addEventListener("click", loadDbStats);

  [idsInput, dateFromInput, dateToInput, viewsMinInput, viewsMaxInput, limitInput, sortPatternSelect, searchNameInput]
    .filter(Boolean)
    .forEach((el) => {
      el.addEventListener("input", () => {
        if (activeSavedId) return;
        schedulePreview();
      });
      el.addEventListener("change", () => {
        if (activeSavedId) return;
        schedulePreview();
      });
    });

  // init
  loadPatternTags();
  loadSavedSets();
  resetForm();
  loadDbStats();
})();
