(() => {
  "use strict";

  async function fetchJson(url) {
    const res = await fetch(url);
    if (!res.ok) throw new Error(`Request failed ${res.status}`);
    return res.json();
  }

  const searchInput = document.getElementById("search");
  const tableBody = document.querySelector("#bloggers-table tbody");
  const countLabel = document.getElementById("count");

  let allItems = [];
  let filtered = [];
  let sortState = { key: "p", dir: "desc" };

  function formatNumber(n) {
    if (!Number.isFinite(n)) return "—";
    return n.toLocaleString("en-US");
  }

  function applyFilterAndSort() {
    const q = (searchInput.value || "").trim().toLowerCase();
    filtered = allItems.filter((it) => !q || (it.u || "").toLowerCase().includes(q));

    const { key, dir } = sortState;
    filtered.sort((a, b) => {
      const va = a[key];
      const vb = b[key];
      if (key === "u") {
        return dir === "asc" ? (va || "").localeCompare(vb || "") : (vb || "").localeCompare(va || "");
      }
      const na = Number.isFinite(va) ? va : -Infinity;
      const nb = Number.isFinite(vb) ? vb : -Infinity;
      return dir === "asc" ? na - nb : nb - na;
    });

    render();
  }

  function render() {
    tableBody.innerHTML = "";
    for (const row of filtered) {
      const tr = document.createElement("tr");
      tr.innerHTML = `
        <td>${row.u ? `@${row.u}` : "—"}</td>
        <td>${formatNumber(row.s)}</td>
        <td>${formatNumber(row.p)}</td>
        <td>${formatNumber(row.v)}</td>
      `;
      tr.addEventListener("click", () => {
        if (!row.id) return;
        window.location.href = `/growth-viewer.html?channel=${row.id}`;
      });
      tableBody.appendChild(tr);
    }
    countLabel.textContent = `Показано: ${filtered.length} / ${allItems.length}`;
  }

  function initSortHeaders() {
    document.querySelectorAll("#bloggers-table th").forEach((th) => {
      th.addEventListener("click", () => {
        const key = th.dataset.key;
        const type = th.dataset.type;
        if (!key) return;
        if (sortState.key === key) {
          sortState.dir = sortState.dir === "asc" ? "desc" : "asc";
        } else {
          sortState = { key, dir: type === "text" ? "asc" : "desc" };
        }
        applyFilterAndSort();
      });
    });
  }

  async function load() {
    try {
      const data = await fetchJson("/api/bloggers");
      allItems = Array.isArray(data.items) ? data.items : [];
      applyFilterAndSort();
    } catch (err) {
      console.error(err);
      countLabel.textContent = "Не удалось загрузить";
    }
  }

  searchInput?.addEventListener("input", applyFilterAndSort);
  initSortHeaders();
  load();
})();
