(() => {
  "use strict";

  async function fetchJson(url, options) {
    const res = await fetch(url, options);
    const text = await res.text();
    if (!res.ok) throw new Error(`Request failed: ${res.status}`);
    return text ? JSON.parse(text) : {};
  }

  function toMs(value) {
    if (value === null || value === undefined) return null;
    if (typeof value === "number") {
      if (!Number.isFinite(value)) return null;
      return value > 1e12 ? value : value * 1000;
    }
    if (typeof value === "string") {
      const iso = value.includes("T") ? value : value.replace(" ", "T") + "Z";
      const ms = Date.parse(iso);
      return Number.isNaN(ms) ? null : ms;
    }
    return null;
  }

  const viewerSortSelect = document.getElementById("viewer-sort-select");
  const patternTableEl = document.getElementById("pattern-table");
  const metaEl = document.getElementById("meta");
  const tableBody = document.querySelector("#metrics-table tbody");
  const canvas = document.getElementById("chart");
  const prevBtn = document.getElementById("prev-btn");
  const nextBtn = document.getElementById("next-btn");
  const positionLabel = document.getElementById("position-label");
  const activeInfoEl = document.getElementById("active-info");
  const statusBar = document.getElementById("status-bar");
  const filterBtn = document.getElementById("filter-btn");
  const filterModal = document.getElementById("filter-modal");
  const filterPatternSelect = document.getElementById("filter-pattern");
  const filterModeSelect = document.getElementById("filter-mode");
  const filterApply = document.getElementById("filter-apply");
  const filterClear = document.getElementById("filter-clear");
  const filterClose = document.getElementById("filter-close");
  const filterPill = document.getElementById("filter-pill");

  const patternTags = ["zero_start_late_spike", "linear_growth", "steps", "plateau_after_round", "plus30_pattern", "micro_steps", "other"];
  const MANUAL_KEYS = ["zero_start_late_spike", "linear_growth", "steps", "plateau_after_round", "plus30_pattern", "micro_steps", "other", "ok"];

  let chart;
  let baseSample = [];
  let sampleList = [];
  let sampleIndex = -1;
  let selectedManual = {};
  let currentSet = null;
  let filterState = { pattern: "", mode: "all" };
  let currentChannelId = null;
  let lastChannelId = null;
  let viewYMaxForChannel = null;

  function readParams() {
    const params = new URLSearchParams(window.location.search);
    return {
      id: params.get("id"),
      channel: params.get("channel"),
      sort: params.get("sort") || "",
      pattern: params.get("pattern") || "",
      mode: params.get("mode") || "all",
      idx: Number(params.get("i")) || 0
    };
  }

  function updateUrl() {
    const params = new URLSearchParams();
    if (currentSet?.id) params.set("id", currentSet.id);
    else if (currentChannelId) params.set("channel", currentChannelId);
    const sortVal = viewerSortSelect?.value || "";
    if (sortVal) params.set("sort", sortVal);
    if (filterState.pattern) params.set("pattern", filterState.pattern);
    if (filterState.mode && filterState.mode !== "all") params.set("mode", filterState.mode);
    if (sampleIndex > 0) params.set("i", sampleIndex);
    const qs = params.toString();
    const newUrl = qs ? `${window.location.pathname}?${qs}` : window.location.pathname;
    window.history.replaceState(null, "", newUrl);
  }

  function setStatus(msg) {
    if (statusBar) statusBar.textContent = msg || "";
  }

  function formatNumber(value) {
    if (value === null || value === undefined || value === "") return "";
    return value.toLocaleString("en-US");
  }

  function delta(curr, prev) {
    if (curr === null || curr === undefined || prev === null || prev === undefined) return "";
    const d = curr - prev;
    if (d === 0) return "";
    return d > 0 ? `+${d}` : `${d}`;
  }

  function buildSeries(metrics, baseMs) {
    const viewPoints = [];
    const likePoints = [];
    const forwardPoints = [];
    if (baseMs !== null) {
      viewPoints.push({ x: 0, y: 0 });
      likePoints.push({ x: 0, y: 0 });
      forwardPoints.push({ x: 0, y: 0 });
    }

    for (const m of metrics) {
      if (m.ts_ms === null || m.ts_ms === undefined) continue;
      const deltaMin = baseMs !== null ? Math.max((m.ts_ms - baseMs) / 60000, 0) : 0;
      const xVal = Number.isFinite(deltaMin) ? deltaMin : 0;

      if (m.view_count !== null && m.view_count !== undefined) {
        viewPoints.push({ x: xVal, y: m.view_count });
      }
      if (m.reactions_total !== null && m.reactions_total !== undefined) {
        likePoints.push({ x: xVal, y: m.reactions_total });
      }
      if (m.forward_count !== null && m.forward_count !== undefined) {
        forwardPoints.push({ x: xVal, y: m.forward_count });
      }
    }

    const lastView = viewPoints.length ? viewPoints[viewPoints.length - 1].y : 0;
    const engagementMaxFromViews = lastView ? Math.round(lastView * 0.005) : 0; // 0.5% от финальных просмотров
    const engagementMaxFromData = Math.max(
      0,
      ...likePoints.map((p) => (Number.isFinite(p.y) ? p.y : 0)),
      ...forwardPoints.map((p) => (Number.isFinite(p.y) ? p.y : 0))
    );
    const engagementMax = Math.max(1, engagementMaxFromViews, engagementMaxFromData);

    return { viewPoints, likePoints, forwardPoints, engagementMax };
  }

  function updateChart(series, chatId) {
    const { viewPoints = [], likePoints = [], forwardPoints = [], engagementMax = null } = series || {};
    const ctx = canvas.getContext("2d");
    const targetHeight = Math.max(220, Math.round(window.innerHeight * 0.65));
    canvas.height = targetHeight;
    canvas.style.height = `${targetHeight}px`;
    canvas.style.width = "100%";

    const xs = [...viewPoints, ...likePoints, ...forwardPoints].map((p) => p.x).filter((v) => Number.isFinite(v));
    const domainMax = xs.length ? Math.max(...xs, 1) : 1;

    // Y-scale: keep per-channel; recompute when channel changes
    let yMax = viewYMaxForChannel;
    if (chatId !== lastChannelId) {
      const ys = viewPoints.map((p) => p.y).filter((v) => Number.isFinite(v));
      const maxY = ys.length ? Math.max(...ys) : 0;
      yMax = maxY > 0 ? Math.ceil(maxY * 1.05) : null;
      viewYMaxForChannel = yMax;
      lastChannelId = chatId;
    }

    const engagementMaxVal = Number.isFinite(engagementMax) && engagementMax > 0 ? engagementMax : undefined;

    if (chart) chart.destroy();
    const datasets = [
      {
        label: "Просмотры",
        data: viewPoints,
        borderColor: "#00c2a8",
        tension: 0.25,
        spanGaps: true,
        pointRadius: 0,
        yAxisID: "views"
      },
      {
        label: "Лайки (накоп.)",
        data: likePoints,
        borderColor: "#ff7eb6",
        tension: 0.25,
        spanGaps: true,
        pointRadius: 0,
        yAxisID: "engagement"
      },
      {
        label: "Форварды (накоп.)",
        data: forwardPoints,
        borderColor: "#ffa53d",
        tension: 0.25,
        spanGaps: true,
        pointRadius: 0,
        yAxisID: "engagement"
      }
    ];

    chart = new Chart(ctx, {
      type: "line",
      data: {
        datasets
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        parsing: false,
        animation: false,
        interaction: { mode: "nearest", intersect: false },
        scales: {
          x: {
            type: "linear",
            min: 0,
            max: Math.max(domainMax, 1),
            title: {
              display: true,
              text: "минуты от публикации"
            },
            ticks: {
              maxRotation: 0,
              autoSkip: true,
              callback: (value) => `${value}m`
            },
            offset: false
          },
          views: {
            position: "left",
            beginAtZero: true,
            suggestedMax: yMax || undefined,
            ticks: { callback: (value) => `${value}` }
          },
          engagement: {
            position: "right",
            beginAtZero: true,
            max: engagementMaxVal,
            grid: { drawOnChartArea: false },
            title: { display: true, text: "лайки/репосты в минуту" },
            ticks: { callback: (value) => `${value}` }
          }
        }
      }
    });
  }

  function renderTable(metrics, messageDateMs) {
    tableBody.innerHTML = "";
    if (messageDateMs !== null) {
      const tr0 = document.createElement("tr");
      tr0.innerHTML = `
        <td>publish</td>
        <td>${new Date(messageDateMs).toLocaleString()}</td>
        <td>0</td>
        <td></td><td></td><td></td><td></td><td></td>
      `;
      tableBody.appendChild(tr0);
    }
    let prevViews = null;
    let prevReacts = null;
    for (const row of metrics) {
      const views = row.view_count ?? "";
      const reacts = row.reactions_total ?? "";
      const tr = document.createElement("tr");
      tr.innerHTML = `
        <td>${row.run_id}</td>
        <td>${new Date(row.ts_ms ?? null).toLocaleString()}</td>
        <td>${formatNumber(views)}</td>
        <td>${formatNumber(row.forward_count ?? "")}</td>
        <td>${formatNumber(row.reply_count ?? "")}</td>
        <td>${formatNumber(reacts)}</td>
        <td>${delta(views, prevViews)}</td>
        <td>${delta(reacts, prevReacts)}</td>
      `;
      tableBody.appendChild(tr);
      if (views !== "" && views !== null) prevViews = views;
      if (reacts !== "" && reacts !== null) prevReacts = reacts;
    }
  }

  function scoreColor(score) {
    if (!Number.isFinite(score)) return "#eee";
    if (score >= 0.66) return "#5f1f2a"; // красный
    if (score >= 0.33) return "#c2a500"; // жёлтый
    return "#1f5f3a"; // зелёный
  }

  function buildRow(label, values, isManual) {
    const cells = patternTags
      .map((p) => {
        const val = values[p];
        if (isManual) {
          const checked = val === 1;
          const cls = p === "ok" ? "pattern-check pattern-ok" : "pattern-check";
          return `<td class="check-cell"><input type="checkbox" class="${cls}" data-key="${p}" ${checked ? "checked" : ""}></td>`;
        }
        const color = Number.isFinite(val) ? scoreColor(val) : "transparent";
        const text = Number.isFinite(val) ? Math.round(val * 100) + "%" : "—";
        return `<td style="background:${color};color:${color === "transparent" ? "#e7edf5" : "#0b1b24"}">${text}</td>`;
      })
      .join("");
    return `<tr><td>${label}</td>${cells}</tr>`;
  }

  function renderPatternTable(autoScores, manual) {
    if (!patternTableEl) return;
    const headers = ["<tr><th>Паттерн</th>", ...patternTags.map((p) => `<th>${p}</th>`), "</tr>"].join("");
    const autoRow = buildRow(
      "Авто",
      patternTags.reduce((acc, p) => ({ ...acc, [p]: Number(autoScores?.[p]) || null }), {}),
      false
    );
    const manualRow = buildRow(
      "Ручные",
      patternTags.reduce((acc, p) => ({ ...acc, [p]: manual?.[p] ?? null }), {}),
      true
    );
    patternTableEl.innerHTML = `<table class="pattern-grid">${headers}${autoRow}${manualRow}</table>`;
    patternTableEl.querySelectorAll(".pattern-check").forEach((el) => {
      el.addEventListener("change", () => {
        const key = el.dataset.key;
        const checked = el.checked;
        selectedManual[key] = checked ? 1 : 0;
        saveManual();
      });
    });
  }

  async function loadLabels(chatId, messageId) {
    const data = await fetchJson(`/api/labels?chat_id=${chatId}&message_id=${messageId}`);
    const manual = data.manual || {};
    const autoScores = (data.auto && data.auto.scores_json && JSON.parse(data.auto.scores_json)) || {};
    selectedManual = {};
    for (const key of MANUAL_KEYS) {
      if (manual[key] !== undefined && manual[key] !== null) selectedManual[key] = manual[key];
    }
    return { manual, autoScores };
  }

  async function loadCurrent() {
    if (sampleIndex < 0 || sampleIndex >= sampleList.length) {
      setStatus("Нет данных");
      return;
    }
    const item = sampleList[sampleIndex];
    positionLabel.textContent = `${sampleIndex + 1}/${sampleList.length}`;
    try {
      const data = await fetchJson(`/api/message?chat_id=${item.chat_id}&message_id=${item.message_id}`);
      const metrics = data.metrics
        ? data.metrics.map((m) => ({
            ...m,
            ts_ms: toMs(m.ts || m.started_at)
          }))
        : [];
      const msgDateMs = toMs(data.message?.message_date);
      let baseMs = msgDateMs;
      if (baseMs === null && metrics.length > 0) {
        baseMs = metrics.find((m) => Number.isFinite(m.ts_ms))?.ts_ms ?? null;
      }
      const series = buildSeries(metrics, baseMs);
      const link = data.message?.message_link || `${item.chat_id}/${item.message_id}`;
      metaEl.innerHTML = `<div><strong>${link}</strong></div><div>message_date: ${
        msgDateMs ? new Date(msgDateMs).toLocaleString() : ""
      }</div>`;
      updateChart(series, item.chat_id);
      renderTable(metrics, msgDateMs);
      const labelsInfo = await loadLabels(item.chat_id, item.message_id);
      renderPatternTable(labelsInfo.autoScores, labelsInfo.manual);
      setStatus("");
    } catch (err) {
      console.error(err);
      setStatus("Не удалось загрузить график");
    }
  }

  function showCurrent() {
    if (sampleIndex < 0 || sampleIndex >= sampleList.length) {
      setStatus("Нет данных в подборке");
      positionLabel.textContent = "0/0";
      updateChart({ viewPoints: [], likePoints: [], forwardPoints: [], engagementMax: null }, null);
      tableBody.innerHTML = "";
      return;
    }
    setStatus("");
    loadCurrent();
  }

  function applyFilterAndSort(sortPattern) {
    const params = readParams();
    const pattern = params.pattern || filterState.pattern;
    const mode = params.mode || filterState.mode || "all";
    filterState = { pattern, mode };

    let filtered = [...baseSample];
    if (pattern && mode !== "all") {
      filtered = baseSample.filter((item) => {
        const manual = item.manual || {};
        const flag = manual[pattern] === 1;
        if (mode === "flagged") return flag;
        if (mode === "not_flagged") return !flag;
        return true;
      });
    }

    if (sortPattern) {
      const channelCounts = new Map();
      if (sortPattern === "channel") {
        for (const item of filtered) {
          const prev = channelCounts.get(item.chat_id) || 0;
          channelCounts.set(item.chat_id, prev + 1);
        }
      }
      filtered.sort((a, b) => {
        if (sortPattern === "channel") {
          const ca = channelCounts.get(a.chat_id) || 0;
          const cb = channelCounts.get(b.chat_id) || 0;
          if (cb !== ca) return cb - ca; // больше сообщений выше
          if (a.chat_id !== b.chat_id) return a.chat_id - b.chat_id;
          const ad = Number.isFinite(a.message_date) ? a.message_date : Number(a.message_id) || 0;
          const bd = Number.isFinite(b.message_date) ? b.message_date : Number(b.message_id) || 0;
          return ad - bd; // внутри канала от старых к новым
        }
        const sa = Number.isFinite(a.auto?.[sortPattern]) ? a.auto[sortPattern] : -Infinity;
        const sb = Number.isFinite(b.auto?.[sortPattern]) ? b.auto[sortPattern] : -Infinity;
        if (sb !== sa) return sb - sa;
        return (b.view_count || 0) - (a.view_count || 0);
      });
    }

    sampleList = filtered;
    sampleIndex = sampleList.length > 0 ? 0 : -1;
    updateFilterPill();
    showCurrent();
    updateUrl();
  }

  function populateSortSelects() {
    const opts = ["", "channel", ...patternTags];
    viewerSortSelect.innerHTML = opts
      .map((opt) => {
        if (!opt) return `<option value="">Исходный порядок</option>`;
        if (opt === "channel") return `<option value="channel">По каналу</option>`;
        return `<option value="${opt}">${opt}</option>`;
      })
      .join("");
  }

  function updateFilterPill() {
    if (!filterPill) return;
    if (!filterState.pattern || filterState.mode === "all") {
      filterPill.style.display = "none";
      filterPill.textContent = "";
      return;
    }
    filterPill.style.display = "inline-flex";
    const modeText = filterState.mode === "flagged" ? "отмечен" : "не отмечен";
    filterPill.textContent = `${filterState.pattern}: ${modeText}`;
  }

  async function loadSet() {
    const params = readParams();
    const id = params.id;
    const channelParam = Number(params.channel);
    try {
      setStatus("Загружаю данные…");
      let data = null;
      if (id) {
        data = await fetchJson(`/api/sets/${encodeURIComponent(id)}`);
        currentSet = data.set;
        currentChannelId = null;
      } else if (Number.isFinite(channelParam)) {
        data = await fetchJson(`/api/channel-items?chat_id=${channelParam}`);
        currentSet = data.set;
        currentChannelId = channelParam;
      } else {
        setStatus("Не указан источник данных");
        return;
      }
      const sortParam = params.sort || "";
      filterState = { pattern: params.pattern || "", mode: params.mode || "all" };
      if (filterPatternSelect) filterPatternSelect.value = filterState.pattern || "";
      if (filterModeSelect) filterModeSelect.value = filterState.mode || "all";
      if (sortParam && viewerSortSelect) viewerSortSelect.value = sortParam;
      // enrich items with manual/auto placeholders
      baseSample = (currentSet?.items || []).map((it) => ({
        ...it,
        manual: {},
        auto: {}
      }));
      applyFilterAndSort(sortParam);
      const total = currentSet?.total ?? baseSample.length;
      const name = currentSet?.name || "Подборка";
      activeInfoEl.textContent = `${name} • ${total} постов`;
      setStatus("");
    } catch (err) {
      console.error(err);
      setStatus("Не удалось загрузить подборку");
    }
  }

  async function saveManual() {
    if (sampleIndex < 0 || sampleIndex >= sampleList.length) return;
    const item = sampleList[sampleIndex];
    try {
      const res = await fetch("/api/label", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          chat_id: item.chat_id,
          message_id: item.message_id,
          manual: selectedManual
        })
      });
      const data = await res.json();
      if (!res.ok || data.error) throw new Error(data.error || `status ${res.status}`);
      // update local manual for current item
      item.manual = data.manual || {};
      setStatus("Сохранено");
    } catch (err) {
      console.error("saveManual failed", err);
      setStatus("Не сохранилось");
    }
  }

  function goNext() {
    if (sampleIndex + 1 >= sampleList.length) return;
    sampleIndex += 1;
    showCurrent();
    updateUrl();
  }

  function goPrev() {
    if (sampleIndex - 1 < 0) return;
    sampleIndex -= 1;
    showCurrent();
    updateUrl();
  }

  viewerSortSelect?.addEventListener("change", () => applyFilterAndSort(viewerSortSelect.value || ""));
  prevBtn?.addEventListener("click", goPrev);
  nextBtn?.addEventListener("click", goNext);

  function openFilter() {
    if (filterModal) filterModal.classList.remove("hidden");
  }
  function closeFilter() {
    if (filterModal) filterModal.classList.add("hidden");
  }

  filterBtn?.addEventListener("click", openFilter);
  filterClose?.addEventListener("click", closeFilter);
  filterClear?.addEventListener("click", () => {
    filterState = { pattern: "", mode: "all" };
    if (filterPatternSelect) filterPatternSelect.value = "";
    if (filterModeSelect) filterModeSelect.value = "all";
    applyFilterAndSort(viewerSortSelect?.value || "");
    closeFilter();
  });
  filterApply?.addEventListener("click", () => {
    filterState = {
      pattern: filterPatternSelect?.value || "",
      mode: filterModeSelect?.value || "all"
    };
    applyFilterAndSort(viewerSortSelect?.value || "");
    closeFilter();
  });

  window.addEventListener("keydown", (e) => {
    const tag = document.activeElement?.tagName;
    const inInput = tag === "INPUT" || tag === "TEXTAREA" || tag === "SELECT";
    if (inInput) return;
    if (e.key === "ArrowLeft") {
      e.preventDefault();
      goPrev();
    } else if (e.key === "ArrowRight") {
      e.preventDefault();
      goNext();
    } else if (["1", "2", "3", "4", "5", "6", "7", "8"].includes(e.key)) {
      const idx = Number(e.key) - 1;
      const key = patternTags[idx];
      if (!key) return;
      selectedManual[key] = selectedManual[key] === 1 ? 0 : 1;
      const checkbox = patternTableEl.querySelector(`.pattern-check[data-key="${key}"]`);
      if (checkbox) checkbox.checked = selectedManual[key] === 1;
      saveManual();
    } else if (e.key === "0") {
      selectedManual.ok = selectedManual.ok === 1 ? 0 : 1;
      const checkbox = patternTableEl.querySelector(`.pattern-check[data-key="ok"]`);
      if (checkbox) checkbox.checked = selectedManual.ok === 1;
      saveManual();
    }
  });

  // init
  populateSortSelects();
  loadSet();
})();
