function parseVer(v) {
  const m = (v || "").match(/^(\d+)\.(\d+)\.(\d+)$/);
  return m ? [Number(m[1]), Number(m[2]), Number(m[3])] : null;
}
function cmpVer(a, b) {
  if (!a && !b) return 0;
  if (!a) return -1;
  if (!b) return 1;
  for (let i = 0; i < 3; i++) {
    if (a[i] > b[i]) return 1;
    if (a[i] < b[i]) return -1;
  }
  return 0;
}

function bindMNRefresh() {
  const btn = document.getElementById("mn-refresh");
  if (btn) {
    btn.onclick = () => {
      console.log("[MN] Refresh click");
      loadMNStatus();
    };
  } else {
    console.warn("[MN] #mn-refresh not found");
  }
}

async function loadMNStatus() {
  const info = document.getElementById("mn-info");
  const okTbody = document.querySelector("#mn-table-ok tbody");
  const offTbody = document.querySelector("#mn-table-off tbody");
  if (!info || !okTbody || !offTbody) {
    console.error("[MN] Missing table elements");
    return;
  }

  okTbody.innerHTML = "";
  offTbody.innerHTML = "";
  info.textContent = "Loading nodes_list.json...";
  console.log("[MN] fetching nodes_list.json");

  try {
    const resp = await fetch("nodes_list.json?_=" + Date.now());
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
    const nodes = await resp.json();

    const timeoutMs = 5000;
    const perNodeFetch = async (n) => {
      const url = `${n.protocol}://${n.host}:${n.port}/get-monitoring`;
      const controller = new AbortController();
      const t = setTimeout(() => controller.abort(), timeoutMs);
      try {
        const r = await fetch(url, { signal: controller.signal });
        clearTimeout(t);
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        const j = await r.json();
        const node = j.node || {};
        return {
          ok: true,
          host: n.host,
          protocol: n.protocol,
          port: n.port,
          peer_type: n.peer_type,
          height: node.height,
          version: node.version,
          synced: node.synced
        };
      } catch {
        clearTimeout(t);
        return { ok: false, ...n, height: null, version: null, synced: null };
      }
    };

    const results = await Promise.all(nodes.map(perNodeFetch));
    const ok = results.filter(r => r.ok);
    const off = results.filter(r => !r.ok);

    let latestVerArr = null;
    let latestHeight = 0;
    ok.forEach(n => {
      const pv = parseVer(n.version);
      if (pv && cmpVer(pv, latestVerArr) > 0) latestVerArr = pv;
      if (n.height && n.height > latestHeight) latestHeight = n.height;
    });
    const latestVerStr = latestVerArr ? latestVerArr.join(".") : null;

    // summary
    info.textContent = `Reachable: ${ok.length}/${results.length} nodes` +
      (latestVerStr ? ` · Latest version: v${latestVerStr}` : "") +
      (latestHeight ? ` · Network height: ${latestHeight}` : "");

    // helper render
    const renderRow = (n, i) => {
      const statusBadge = `<span class="badge bg-${n.ok ? "success" : "danger"}">${n.ok ? "OK" : "Offline"}</span>`;

      let verCell = n.version ?? "–";
      if (n.ok && latestVerArr && n.version) {
        const pv = parseVer(n.version);
        const d = cmpVer(pv, latestVerArr);
        if (d === 0) verCell += ' <span class="badge bg-success">✓</span>';
        else if (d < 0) verCell += ' <span class="badge bg-warning text-dark">!</span>';
        else verCell += ' <span class="badge bg-info">↑</span>';
      }

      let heightCell = n.height ?? "–";
      if (n.ok && latestHeight && n.height) {
        if (n.height === latestHeight) {
          heightCell += ' <span class="badge bg-success">✓</span>';
        } else if (n.height < latestHeight - 5) {
          heightCell += ' <span class="badge bg-danger">↓</span>';
        } else if (n.height < latestHeight) {
          heightCell += ' <span class="badge bg-warning text-dark">!</span>';
        }
      }

      const syncedIcon = n.synced === true ? "✅" : (n.synced === false ? "❌" : "–");
      const tr = document.createElement("tr");
      tr.innerHTML = `
        <td>${i}</td>
        <td><a href="${n.protocol}://${n.host}:${n.port}/get-monitoring" target="_blank"><code>${n.host}</code></a></td>
        <td>${n.protocol}</td>
        <td>${n.port}</td>
        <td>${n.peer_type}</td>
        <td>${statusBadge}</td>
        <td>${heightCell}</td>
        <td>${verCell}</td>
        <td>${syncedIcon}</td>
      `;
      return tr;
    };

    ok.forEach((n, idx) => okTbody.appendChild(renderRow(n, idx + 1)));
    off.forEach((n, idx) => offTbody.appendChild(renderRow(n, idx + 1)));

  } catch (err) {
    console.error("[MN] load error:", err);
    info.textContent = `Error loading nodes_list.json: ${err.message}`;
  }
}

function initMNStatus() {
  console.log("[MN] initMNStatus()");
  bindMNRefresh();
  loadMNStatus();
}
