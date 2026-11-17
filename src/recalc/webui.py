"""单文件网页：原生 HTML/JS，无外部依赖。

支持：并行发起多份重算、取消单个作业、修改规则再提交、故障注入
（延迟 / 瞬时失败 / 发布前硬退出），并实时展示作业状态机、产物引用
计数与已发布报表的输入-输出血缘。
"""

INDEX_HTML = r"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>共享清洗重算控制台</title>
<style>
  :root { --bg:#0f1420; --panel:#182032; --line:#2a3550; --txt:#e6ebf5;
          --muted:#8b97ad; --accent:#5b9dff; --ok:#3fbf7f; --warn:#e8a33d;
          --bad:#ef6b6b; --purple:#a48cff; }
  * { box-sizing: border-box; }
  body { margin:0; background:var(--bg); color:var(--txt);
         font:14px/1.55 -apple-system,"Segoe UI",Roboto,"PingFang SC","Microsoft YaHei",sans-serif; }
  header { padding:16px 24px; border-bottom:1px solid var(--line);
           display:flex; align-items:center; gap:16px; }
  header h1 { font-size:17px; margin:0; }
  header .hint { color:var(--muted); font-size:12px; }
  main { padding:20px 24px; display:grid; grid-template-columns: 380px 1fr;
         gap:20px; align-items:start; }
  .panel { background:var(--panel); border:1px solid var(--line);
           border-radius:10px; padding:16px; margin-bottom:16px; }
  .panel h2 { font-size:14px; margin:0 0 12px; color:var(--accent); }
  label { display:block; font-size:12px; color:var(--muted); margin:8px 0 3px; }
  select, input { width:100%; background:#11182a; color:var(--txt);
          border:1px solid var(--line); border-radius:6px; padding:7px 9px; font-size:13px; }
  .row { display:grid; grid-template-columns:1fr 1fr; gap:8px; }
  button { border:0; border-radius:6px; padding:8px 12px; font-size:13px;
           cursor:pointer; color:#fff; background:var(--accent); }
  button.ghost { background:#28344f; }
  button.danger { background:var(--bad); }
  button:disabled { opacity:.45; cursor:not-allowed; }
  .btnrow { display:flex; gap:8px; margin-top:12px; flex-wrap:wrap; }
  details { margin-top:10px; border-top:1px dashed var(--line); padding-top:8px; }
  summary { cursor:pointer; color:var(--muted); font-size:12px; }
  table { width:100%; border-collapse:collapse; font-size:12px; }
  th, td { text-align:left; padding:6px 8px; border-bottom:1px solid var(--line);
           vertical-align:top; word-break:break-all; }
  th { color:var(--muted); font-weight:600; }
  .badge { display:inline-block; padding:2px 8px; border-radius:99px; font-size:11px; }
  .PENDING { background:#33415e; } .COMPUTING { background:#31517d; }
  .PUBLISHING { background:#5a4a8f; } .PUBLISHED { background:#1f5e40; color:#bff3d8; }
  .CANCELED { background:#6b4040; color:#ffd2d2; } .FAILED { background:#7a2f2f; color:#ffd2d2; }
  .mono { font-family:ui-monospace,Menlo,Consolas,monospace; font-size:11px; color:var(--muted); }
  .refs { color:var(--purple); }
  .ok { color:var(--ok); } .warn { color:var(--warn); } .bad { color:var(--bad); }
  .toast { position:fixed; right:20px; bottom:20px; background:#223; border:1px solid var(--line);
           padding:10px 14px; border-radius:8px; opacity:0; transition:opacity .25s; max-width:420px; }
  .toast.show { opacity:1; }
  .kv { display:grid; grid-template-columns:96px 1fr; gap:2px 8px; font-size:12px; }
  .kv .k { color:var(--muted); }
  a { color:var(--accent); }
  .pill { font-size:11px; padding:1px 7px; border:1px solid var(--line); border-radius:99px; color:var(--muted); }
</style>
</head>
<body>
<header>
  <h1>共享清洗 · 多报表重算控制台</h1>
  <span class="hint">复用键 = 输入版哈希 + 规则哈希 + 字段权限哈希（与任务名无关）· 状态每 1.2s 自动刷新</span>
</header>
<main>
  <div>
    <div class="panel">
      <h2>发起重算（可连续并行提交多份）</h2>
      <label>报表</label>
      <select id="report"></select>
      <div class="row">
        <div><label>分组字段 group_by</label><input id="group_by"></div>
        <div><label>最小金额 min_amount</label><input id="min_amount" type="number"></div>
      </div>
      <div class="row">
        <div><label>丢弃负数 drop_negatives</label>
          <select id="drop_negatives"><option value="">沿用配置</option><option value="true">true</option><option value="false">false</option></select>
        </div>
        <div><label>指标 metrics</label><input id="metrics" placeholder="amount"></div>
      </div>
      <details>
        <summary>故障注入 / 进程崩溃验证（不参与复用键）</summary>
        <div class="row">
          <div><label>阶段延迟秒数（制造并行/取消窗口）</label><input id="f_delay" type="number" min="0" value="0"></div>
          <div><label>瞬时失败次数（worker 重试）</label><input id="f_times" type="number" min="0" value="0"></div>
        </div>
        <label>失败发生阶段</label>
        <select id="f_stage"><option value="">不失败</option><option value="clean">clean</option><option value="partition">partition</option></select>
        <label><input type="checkbox" id="f_crash" style="width:auto"> 发布确认前硬退出进程（重启 publisher 后恢复）</label>
      </details>
      <div class="btnrow">
        <button onclick="submitJob(false)">提交重算</button>
        <button class="ghost" onclick="submitJob(true)">保存规则并提交</button>
      </div>
    </div>
    <div class="panel">
      <h2>产物注册表（引用计数 / 租约 / GC 依据）</h2>
      <table><thead><tr><th>产物</th><th>状态</th><th>引用</th><th>尝试</th></tr></thead>
      <tbody id="artifacts"></tbody></table>
    </div>
  </div>
  <div class="panel">
    <h2>作业（每份报表的输入与输出可追溯）</h2>
    <table><thead><tr><th>作业 / 报表</th><th>状态</th><th>复用键与血缘</th><th>操作</th></tr></thead>
    <tbody id="jobs"></tbody></table>
  </div>
</main>
<div class="toast" id="toast"></div>
<script>
const $ = id => document.getElementById(id);
let reports = [];
function toast(msg, bad) {
  const t = $('toast'); t.textContent = msg; t.className = 'toast show ' + (bad ? 'bad' : 'ok');
  setTimeout(() => t.className = 'toast', 3200);
}
async function api(path, opts) {
  const r = await fetch(path, Object.assign({headers:{'Content-Type':'application/json'}}, opts));
  const data = await r.json();
  if (!r.ok) throw new Error(data.error || r.status);
  return data;
}
function ruleOverride() {
  const o = {};
  const gb = $('group_by').value.trim();
  if (gb) o.group_by = gb.split(',').map(s => s.trim()).filter(Boolean);
  if ($('min_amount').value !== '') o.min_amount = Number($('min_amount').value);
  if ($('drop_negatives').value !== '') o.drop_negatives = $('drop_negatives').value === 'true';
  const m = $('metrics').value.trim();
  if (m) o.metrics = m.split(',').map(s => s.trim()).filter(Boolean);
  return o;
}
function faults() {
  const f = { delay_seconds: Number($('f_delay').value || 0),
              fail_times_remaining: Number($('f_times').value || 0),
              fail_stage: $('f_stage').value,
              crash_before_publish: $('f_crash').checked };
  return f;
}
async function submitJob(persistRules) {
  const report_id = $('report').value;
  const rule_override = ruleOverride();
  try {
    if (persistRules && Object.keys(rule_override).length)
      await api('/api/rules', {method:'POST', body: JSON.stringify({report_id, rule_override})});
    const {job} = await api('/api/recompute', {method:'POST',
      body: JSON.stringify({report_id, rule_override, faults: faults()})});
    toast('已提交 ' + job.job_id + '，快照 ' + job.snapshot_id.slice(0, 12));
  } catch (e) { toast(e.message, true); }
  refresh();
}
async function cancelJob(id) {
  try { await api('/api/cancel', {method:'POST', body: JSON.stringify({job_id: id})}); toast('已请求取消 ' + id); }
  catch (e) { toast(e.message, true); }
  refresh();
}
function fillRuleForm() {
  const r = reports.find(x => x.id === $('report').value);
  if (!r) return;
  $('group_by').placeholder = (r.rules.group_by||[]).join(',');
  $('min_amount').placeholder = r.rules.min_amount;
  $('metrics').placeholder = (r.rules.metrics||[]).join(',');
}
function renderReports() {
  const sel = $('report');
  if (!sel.options.length || [...sel.options].map(o=>o.value).join() !== reports.map(r=>r.id).join()) {
    sel.innerHTML = reports.map(r => `<option value="${r.id}">${r.name}（${r.id} / 角色 ${r.permission.role}）</option>`).join('');
    fillRuleForm();
  }
}
function renderArtifacts(artifacts, activeRefs) {
  $('artifacts').innerHTML = artifacts.map(a => `
    <tr>
      <td><span class="pill">${a.kind}</span><div class="mono">${a.artifact_id}</div></td>
      <td class="${a.status==='ready'?'ok':a.status==='failed'?'bad':'warn'}">${a.status}
        ${a.lease ? '<div class="mono">租约 ' + a.lease.owner_boot.slice(0,10) + '</div>' : ''}
        ${a.last_error ? '<div class="bad mono">' + a.last_error + '</div>' : ''}</td>
      <td class="refs">${a.refs.length} 引用<div class="mono">活跃 ${(activeRefs[a.artifact_id]||[]).length}</div></td>
      <td>${a.attempts}</td>
    </tr>`).join('') || '<tr><td colspan="4" class="mono">暂无产物</td></tr>';
}
function renderJobs(jobs) {
  $('jobs').innerHTML = jobs.map(j => {
    const active = ['PENDING','COMPUTING','PUBLISHING'].includes(j.state);
    return `<tr>
      <td><b>${j.report_name}</b><div class="mono">${j.job_id}</div>
        <div class="mono">规则版本 v${j.rule_version || 1} · ${new Date(j.created_at).toLocaleTimeString()}</div></td>
      <td><span class="badge ${j.state}">${j.state}</span>
        ${j.error ? '<div class="bad mono">' + j.error.stage + ': ' + j.error.message + '</div>' : ''}
        ${j.cancel_reason ? '<div class="mono">' + j.cancel_reason + '</div>' : ''}</td>
      <td><div class="kv">
        <span class="k">输入快照</span><span class="mono">${j.snapshot_id.slice(0,14)}…</span>
        <span class="k">输入哈希</span><span class="mono">${j.input_version_hash.slice(0,16)}…</span>
        <span class="k">规则哈希</span><span class="mono">${j.rule_hash.slice(0,16)}…</span>
        <span class="k">权限哈希</span><span class="mono">${j.permission_hash.slice(0,16)}…</span>
        <span class="k">清洗产物</span><span class="mono">${j.clean_artifact_id}</span>
        <span class="k">分区产物</span><span class="mono">${j.partition_artifact_id}</span>
        ${j.output ? '<span class="k">输出</span><a href="/api/output/'+j.job_id+'">下载 report.csv</a>' : ''}
      </div></td>
      <td>${active ? `<button class="danger" onclick="cancelJob('${j.job_id}')">取消</button>` : '—'}</td>
    </tr>`;
  }).join('') || '<tr><td colspan="4" class="mono">暂无作业</td></tr>';
}
async function refresh() {
  try {
    const s = await api('/api/state');
    reports = s.reports;
    renderReports(); renderArtifacts(s.artifacts, s.active_artifact_refs); renderJobs(s.jobs);
  } catch (e) { /* 服务重启中，下个轮询恢复 */ }
}
$('report').addEventListener('change', fillRuleForm);
refresh();
setInterval(refresh, 1200);
</script>
</body>
</html>
"""
