import re

with open("frontend/index.html", "r", encoding="utf-8") as f:
    html = f.read()

# 1. Update CSS
css_updates = {
    "body{": "body{background: linear-gradient(135deg, #0a0c10 0%, #1a1f2c 100%);",
    ".sidebar{": ".sidebar{background: rgba(17,19,24,0.65); backdrop-filter: blur(20px); border-right: 1px solid rgba(255,255,255,0.05);",
    ".topbar{": ".topbar{background: rgba(17,19,24,0.65); backdrop-filter: blur(20px); border-bottom: 1px solid rgba(255,255,255,0.05);",
    ".card{": ".card{background: rgba(17,19,24,0.65); backdrop-filter: blur(20px); border: 1px solid rgba(255,255,255,0.05); box-shadow: 0 8px 32px rgba(0,0,0,0.2);",
    ".stat-card{": ".stat-card{background: rgba(17,19,24,0.65); backdrop-filter: blur(20px); border: 1px solid rgba(255,255,255,0.05); box-shadow: 0 4px 16px rgba(0,0,0,0.1);",
    ".modal{": ".modal{background: rgba(17,19,24,0.85); backdrop-filter: blur(24px); border: 1px solid rgba(255,255,255,0.1); box-shadow: 0 24px 64px rgba(0,0,0,0.5);",
    ".btn-primary{": ".btn-primary{background: linear-gradient(135deg, var(--accent), #00ffb3); color:#000; box-shadow: 0 4px 12px rgba(0,229,160,0.3); border:1px solid rgba(255,255,255,0.2);",
    ".btn-primary:hover{": ".btn-primary:hover{transform: translateY(-2px); box-shadow: 0 6px 16px rgba(0,229,160,0.4);",
    ".form-group input,.form-group select,.form-group textarea{": ".form-group input,.form-group select,.form-group textarea{background: rgba(255,255,255,0.03); border: 1px solid rgba(255,255,255,0.1);",
}

for k, v in css_updates.items():
    html = html.replace(k, v)


# 2. Update Run Backup Modal HTML
backup_modal_old = '''    <div class="form-group">
      <label>Backup Type</label>
      <select id="bk-type" onchange="onBackupTypeChange()">
        <option value="full">Full Database</option>
        <option value="collection">Specific Collection / Table</option>
      </select>
    </div>'''

backup_modal_new = '''    <div class="form-group">
      <label>Backup Scope</label>
      <select id="bk-type" onchange="onBackupTypeChange()">
        <option value="full">Full Database</option>
        <option value="collection">Specific Collection / Table</option>
      </select>
    </div>
    <div class="form-group">
      <label>Backup Method</label>
      <select id="bk-method" onchange="onBackupMethodChange('bk')">
        <option value="full">Full Backup</option>
        <option value="incremental">Incremental Backup (MongoDB Only)</option>
      </select>
    </div>
    <div class="form-group" id="bk-incremental-group" style="display:none">
      <label>Incremental Field Name</label>
      <input type="text" id="bk-incremental-field" placeholder="e.g. updated_at"/>
      <div style="font-size:11px;color:var(--muted);margin-top:4px">Enter the timestamp/date field used to filter new records.</div>
    </div>'''
html = html.replace(backup_modal_old, backup_modal_new)


# 3. Update Add Schedule Modal HTML
schedule_modal_old = '''    <div class="form-group">
      <label>Storage Destination</label>
      <select id="sc-storage"></select>
    </div>'''

schedule_modal_new = '''    <div class="form-group">
      <label>Storage Destination</label>
      <select id="sc-storage"></select>
    </div>
    <div class="form-group">
      <label>Backup Method</label>
      <select id="sc-method" onchange="onBackupMethodChange('sc')">
        <option value="full">Full Backup</option>
        <option value="incremental">Incremental Backup (MongoDB Only)</option>
      </select>
    </div>
    <div class="form-group" id="sc-incremental-group" style="display:none">
      <label>Incremental Field Name</label>
      <input type="text" id="sc-incremental-field" placeholder="e.g. updated_at"/>
    </div>'''
html = html.replace(schedule_modal_old, schedule_modal_new)


# 4. Add JS function onBackupMethodChange
js_code = '''
function onBackupMethodChange(prefix) {
  const method = document.getElementById(`${prefix}-method`).value;
  const incGroup = document.getElementById(`${prefix}-incremental-group`);
  if (incGroup) {
    incGroup.style.display = method === 'incremental' ? 'block' : 'none';
  }
}
'''
html = html.replace("const API = '/api';", "const API = '/api';\n" + js_code)


# 5. Update runBackup JS payload
run_backup_old = '''  const payload = {
    database_id: dbId,
    storage_id: stId,
    label: v('bk-label'),
    collection: isColl ? v('bk-collection-select') : 'full'
  };'''

run_backup_new = '''  const payload = {
    database_id: dbId,
    storage_id: stId,
    label: v('bk-label'),
    collection: isColl ? v('bk-collection-select') : 'full',
    backup_method: v('bk-method'),
    incremental_field: v('bk-method') === 'incremental' ? v('bk-incremental-field') : null
  };'''
html = html.replace(run_backup_old, run_backup_new)


# 6. Update addSchedule JS payload
add_sched_old = '''  const payload = {
    name: v('sc-name'),
    database_id: v('sc-db'),
    storage_id: v('sc-storage'),
    frequency: v('sc-freq'),
    time: v('sc-time'),
    enabled: true
  };'''

add_sched_new = '''  const payload = {
    name: v('sc-name'),
    database_id: v('sc-db'),
    storage_id: v('sc-storage'),
    frequency: v('sc-freq'),
    time: v('sc-time'),
    enabled: true,
    backup_method: v('sc-method'),
    incremental_field: v('sc-method') === 'incremental' ? v('sc-incremental-field') : null
  };'''
html = html.replace(add_sched_old, add_sched_new)


with open("frontend/index.html", "w", encoding="utf-8") as f:
    f.write(html)
