'use strict';
const $ = id => document.getElementById(id);
let groups = [], tasks = [], template = '', taskTemplate = '', generated = null;
const referenceRevision = '5dda3191f4a5daaa2cbf227e9a16e1ae1b3c1688';
const task = () => tasks.find(item => item.id === $('task').value);
const accessLabels = {read: 'Read', write: 'Read + write/edit', delete: 'Read + write/edit + delete'};
const pythonCommand = /Win/i.test(navigator.userAgentData?.platform || navigator.platform) ? 'py -3' : 'python3';
const selected = new Set();
const baseline = 'BASE_USER_SESSION_PERMISSION';
const permissions = () => task() ? task().permissions : [...new Set([baseline, ...groups.filter(g => selected.has(g.id)).flatMap(g => g.permissions)])];
const activeGroups = () => groups.filter(g => selected.has(g.id));
const slug = value => value.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-|-$/g, '') || 'kaltura-app';
function render(filter = document.querySelector('[data-filter].active').dataset.filter) {
  $('groups').replaceChildren();
  for (const group of groups.filter(g => filter === 'all' || g.mode === filter)) {
    const label = document.createElement('label'); label.className = 'group';
    const checkbox = document.createElement('input'); checkbox.type = 'checkbox'; checkbox.checked = selected.has(group.id);
    checkbox.addEventListener('change', () => {checkbox.checked ? selected.add(group.id) : selected.delete(group.id); $('ack').checked = false; update();});
    const content = document.createElement('div');
    const name = document.createElement('span'); name.textContent = group.name;
    const description = document.createElement('p'); description.textContent = group.description;
    const codes = document.createElement('code'); codes.textContent = group.permissions.join(' · ');
    const operations = document.createElement('code'); operations.textContent = group.operations;
    content.append(name, description, codes, operations);
    const badge = document.createElement('span'); badge.className = `badge ${group.mode}`; badge.textContent = group.mode.toUpperCase();
    label.append(checkbox, content, badge); $('groups').append(label);
  }
}
function update() {
  generated = null; $('output').hidden = true; $('task-output').hidden = true; $('code').textContent = ''; $('runner-code').textContent = '';
  const values = permissions(), currentTask = task();
  const capability = currentTask ? currentTask.capability : activeGroups().some(g => g.mode === 'delete') ? 'delete' : activeGroups().some(g => g.mode === 'write') ? 'write' : 'read';
  const writes = capability !== 'read';
  $('count').textContent = values.length; $('permissions').replaceChildren(...values.map(p => {const node = document.createElement('div'); node.textContent = p; return node;}));
  $('risk').textContent = capability === 'delete' ? 'Delete-capable' : accessLabels[capability];
  $('ack-label').hidden = !writes; $('write-note').hidden = !writes;
  $('ttl').textContent = `${Number($('duration').value) / 60} minutes`;
  $('scope').textContent = $('list-all').checked ? 'All owners · list:*' : 'Existing entitlements';
  const ready = (currentTask || selected.size > 0) && (!writes || $('ack').checked) && template.length > 0 && (!currentTask || taskTemplate.length > 0);
  $('generate').disabled = !ready;
  $('status').textContent = !currentTask && !selected.size ? 'Choose at least one permission group.' : writes && !$('ack').checked ? 'Review and acknowledge write access.' : 'Generates files only. No API calls.';
}
function download(name, content, type) {
  const url = URL.createObjectURL(new Blob([content], {type}));
  const a = document.createElement('a'); a.href = url; a.download = name; a.click();
  setTimeout(() => URL.revokeObjectURL(url), 1000);
}
function encodeConfig(config) {
  return btoa(Array.from(new TextEncoder().encode(JSON.stringify(config)), b => String.fromCharCode(b)).join(''));
}
// ZIP stored entries: UTF-8 filenames, CRC32, and no external library or server upload.
function createZip(files) {
  const encoder = new TextEncoder(), chunks = [], directory = [];
  let offset = 0, directorySize = 0;
  const crc32 = data => {
    let crc = 0xffffffff;
    for (const byte of data) {
      crc ^= byte;
      for (let bit = 0; bit < 8; bit++) crc = (crc >>> 1) ^ ((crc & 1) ? 0xedb88320 : 0);
    }
    return (crc ^ 0xffffffff) >>> 0;
  };
  for (const [path, content] of Object.entries(files)) {
    const name = encoder.encode(path), data = encoder.encode(content), crc = crc32(data);
    const local = new Uint8Array(30 + name.length), view = new DataView(local.buffer);
    view.setUint32(0, 0x04034b50, true); view.setUint16(4, 20, true); view.setUint16(6, 0x800, true);
    view.setUint16(12, 33, true); // Valid DOS date: January 1, 1980.
    view.setUint32(14, crc, true); view.setUint32(18, data.length, true); view.setUint32(22, data.length, true);
    view.setUint16(26, name.length, true); local.set(name, 30);
    const central = new Uint8Array(46 + name.length), index = new DataView(central.buffer);
    index.setUint32(0, 0x02014b50, true); index.setUint16(4, 20, true); index.setUint16(6, 20, true);
    index.setUint16(8, 0x800, true); index.setUint16(14, 33, true); index.setUint32(16, crc, true);
    index.setUint32(20, data.length, true); index.setUint32(24, data.length, true);
    index.setUint16(28, name.length, true); index.setUint32(42, offset, true); central.set(name, 46);
    chunks.push(local, data); directory.push(central); offset += local.length + data.length; directorySize += central.length;
  }
  const end = new Uint8Array(22), view = new DataView(end.buffer), count = directory.length;
  view.setUint32(0, 0x06054b50, true); view.setUint16(8, count, true); view.setUint16(10, count, true);
  view.setUint32(12, directorySize, true); view.setUint32(16, offset, true);
  return new Blob([...chunks, ...directory, end], {type: 'application/zip'});
}
function generate(event) {
  event.preventDefault();
  if (!$('form').reportValidity() || $('generate').disabled) return;
  if (!/^[0-9]+$/.test($('pid').value) || !Number.isSafeInteger(Number($('pid').value)) || Number($('pid').value) < 1) {
    $('pid').setCustomValidity('Enter a positive numeric partner ID.'); $('pid').reportValidity(); return;
  }
  const role = $('role').value.trim(), purpose = $('purpose').value.trim();
  if (!role || !purpose) { $('status').textContent = 'Role name and purpose must contain text.'; return; }
  const config = {partner_id: $('pid').value, role_name: role, permissions: permissions().join(','), permissions_file: null,
    description: purpose, user_id: $('user').value.trim() || `app-token-${slug(role)}`, session_type: $('type').value,
    session_duration: Number($('duration').value), extra_privilege: $('list-all').checked ? ['list:*'] : [],
    service_url: 'https://www.kaltura.com/api_v3', output: null, yes: false};
  const script = template.replace('__CONFIG_BASE64__', encodeConfig(config));
  const env = `# Local provisioning credentials. Never commit or share this file.\nKALTURA_PARTNER_ID=${JSON.stringify(config.partner_id)}\nKALTURA_ADMIN_SECRET=${JSON.stringify($('secret').value)}\n`;
  const currentTask = task();
  const taskConfig = currentTask ? {...currentTask, partner_id: config.partner_id, credential_slug: slug(role), session_type: config.session_type === 'admin' ? 2 : 0} : null;
  const runner = currentTask ? taskTemplate.replace('TASK_CONFIG = None  # __TASK_CONFIG__', `TASK_CONFIG = json.loads(base64.b64decode("${encodeConfig(taskConfig)}"))`) : '';
  const csv = currentTask && currentTask.input_columns.length ? [currentTask.input_columns, currentTask.sample_row].map(row => row.map(value => '"' + String(value).replaceAll('"', '""') + '"').join(',')).join('\n') + '\n' : '';
  const inputArgument = csv ? ' --input input.csv' : '';
  const taskCommand = `${pythonCommand} run_task.py${inputArgument}`;
  const operationGuide = currentTask ? currentTask.api_actions.map(([service, action]) => `- ${service}.${action}`).join('\n') : activeGroups().map(g => `- ${g.name} (${g.mode}): ${g.operations}`).join('\n');
  const taskGuide = currentTask ? `## Scoped task: ${currentTask.name}\n\nScript operation category: ${accessLabels[currentTask.mode]}\nRole capability category: ${accessLabels[currentTask.capability]}\n\n${currentTask.notes}\n\nThe ZIP includes run_task.py${csv ? ' and input.csv' : ''} beside the provisioning script. ${csv ? 'Replace every sample target in input.csv with reviewed targets; expected_name and expected_label must match exactly.' : 'Optional report filters: --tag, --owner, --category, --start YYYY-MM-DD, --end YYYY-MM-DD (UTC).'}\n\nOffline plan (no network): ${taskCommand} --dry-run\nLive read-only preview: ${taskCommand} --preview\nExecution: ${taskCommand} --execute\n\nThe task reads credentials/${slug(role)}.env, using session.startWidgetSession followed by SHA256(widget KS + token value) and appToken.startSession. It never uses an admin secret or asks for one. These authentication actions are additional to the content actions listed below. It never requests privilege escalation or retries writes. Its default mode is offline. Write/delete execution requires typed terminal confirmation and rechecks expected values before each mutation. The runner's action allowlist is a local guard, not a server-side restriction on the permission's other actions. The local role-permissions record is a consistency check, not proof of the remote role's grants.\n\nSource reference (read-only): https://github.com/Kaltura-EDU/api-scripts/blob/${referenceRevision}/${currentTask.source}\nThese focused versions are independently authored and do not implement every optional feature of their references.\n\n` : '';
  const guide = `# ${role}: scoped AppToken\n\nPurpose: ${purpose}\nPartner ID: ${config.partner_id}\nSession identity: ${config.user_id}\nSession type: ${config.session_type} (role scoped)\nSession lifetime: ${config.session_duration} seconds\nHash: SHA256\nEndpoint: ${config.service_url}\nPrivileges: setrole:<new role ID>${config.extra_privilege.length ? ', list:*' : ''}\n\n## Required role permissions\n\n${permissions().map(p => '- ' + p).join('\n')}\n\n${taskGuide}## Content operations\n\n${operationGuide}\n\n## Provision locally\n\n1. Download the ZIP to a folder and unzip it. Open the extracted folder in your terminal. The ZIP preserves .env and includes all scripts, README.md, PERMISSIONS.md, and .gitignore.\n2. If the secret was left blank, edit KALTURA_ADMIN_SECRET in .env. JSON double-quoted values are supported.\n3. Run ${pythonCommand} provision_app_token.py --dry-run to review without network calls.\n4. Run ${pythonCommand} provision_app_token.py and confirm the displayed plan. This creates a scoped role and AppToken. It does not run content tasks.\n5. Token credentials are saved to credentials/${slug(role)}.env.\n6. Remove the provisioning .env containing the admin secret when finished.\n\nProvisioning performs session.start (admin type 2, 300 seconds), userRole.add, and appToken.add using the account admin secret. The new role receives only the permissions listed above, without role or token administration grants. No external Python packages are required (Python 3.10+). Existing token credentials are never overwritten. For timeouts or partial creation, inspect Kaltura before rerunning to avoid duplicates.\n\n## Verification status\n\nPermission names and selected actions reviewed offline on 2026-10-12. No live Kaltura calls were made by the agent. These are least-privilege candidates for the documented operations; successful access and minimum grants must be confirmed by the operator on the target account. On denial, the runner stops without admin fallback or automatic permission expansion. Content entitlements, access controls, and enabled plugins may require account-specific configuration.\n\n## References\n\n- https://kaltura.md/raw/KALTURA_APPTOKENS_API.md\n- https://github.com/Kaltura-EDU/api-scripts/blob/${referenceRevision}/list_of_kaltura_permissions.txt\n`;
  const folder = `kaltura-${slug(role)}`;
  const readme = `# ${role}: local Kaltura scripts\n\nDownload this ZIP to a folder and unzip it. Open the extracted ${folder} folder in your terminal; do not run scripts from inside the ZIP or from the Kaltura repository root.\n\nRequires Python 3.10+. No packages need installing. Windows: use py -3 (or your installed python command). Mac: use python3.\n\n1. If you left the secret blank, fill in KALTURA_ADMIN_SECRET in .env. The ZIP preserves its exact filename; Mac Finder may hide dotfiles.\n2. Review PERMISSIONS.md and preview provisioning without network calls:\n   ${pythonCommand} provision_app_token.py --dry-run\n3. Provision the role and token yourself, confirming the terminal plan:\n   ${pythonCommand} provision_app_token.py\n4. Remove the provisioning .env containing the admin secret when finished. Token credentials are saved to credentials/${slug(role)}.env.\n${currentTask ? `5. ${csv ? 'Replace sample targets in input.csv with reviewed values.' : 'Use optional report filters as described in PERMISSIONS.md.'}\n6. Preview the task offline: ${taskCommand} --dry-run\n7. Live read-only preview: ${taskCommand} --preview\n8. Execute: ${taskCommand} --execute\n` : ''}\nThe dashboard makes no Kaltura API calls. These scripts only make calls when you run them with the relevant execution command. Write/delete tasks require typed confirmation. Keep this ZIP private: it contains your provisioning secret if entered in the form. Never commit the ZIP or .env.\n`;
  const files = {'provision_app_token.py': script, '.env': env, 'PERMISSIONS.md': guide, 'README.md': readme,
    '.gitignore': '.env\n.env.*\ncredentials/\nreports/\n*.zip\n__pycache__/\n'};
  if (currentTask) files['run_task.py'] = runner;
  if (csv) files['input.csv'] = csv;
  const zip = createZip(Object.fromEntries(Object.entries(files).map(([name, content]) => [`${folder}/${name}`, content])));
  generated = {script, env, guide, runner, csv, zip, zipName: `${folder}.zip`}; $('code').textContent = script; $('output').hidden = false;
  $('provision-dry-command').textContent = `${pythonCommand} provision_app_token.py --dry-run`;
  $('provision-live-command').textContent = `${pythonCommand} provision_app_token.py`;
  $('output-note').textContent = $('secret').value ? 'Download this to a folder and unzip. Keep the ZIP private; it contains your .env secret.' : 'Download this to a folder and unzip, then fill in your admin secret in .env.';
  $('task-output').hidden = !currentTask;
  if (currentTask) {
    $('runner-code').textContent = runner;
    $('input-note').textContent = csv ? `Replace the sample row in input.csv. Required columns: ${currentTask.input_columns.join(', ')}.` : 'Optional filters: --tag, --owner, --category, --start YYYY-MM-DD, --end YYYY-MM-DD. Date ranges use UTC.';
    $('task-dry-command').textContent = `${taskCommand} --dry-run`;
    $('task-live-command').textContent = `${taskCommand} --preview\n${taskCommand} --execute`;
  }
  $('secret').value = ''; $('secret').type = 'password'; $('reveal').textContent = 'Show';
  $('output').scrollIntoView({behavior:'smooth', block:'start'});
}
$('form').addEventListener('submit', generate);
$('form').addEventListener('input', event => { if (event.target.id === 'pid') $('pid').setCustomValidity(''); update(); });
document.querySelectorAll('[data-filter]').forEach(button => button.addEventListener('click', () => {
  document.querySelectorAll('[data-filter]').forEach(b => {b.classList.remove('active'); b.setAttribute('aria-pressed', 'false');});
  button.classList.add('active'); button.setAttribute('aria-pressed', 'true'); render(button.dataset.filter);
}));
$('clear').addEventListener('click', () => {selected.clear(); $('ack').checked = false; render(); update();});
$('reveal').addEventListener('click', () => {const show = $('secret').type === 'password'; $('secret').type = show ? 'text' : 'password'; $('reveal').textContent = show ? 'Hide' : 'Show'; $('reveal').setAttribute('aria-label', show ? 'Hide admin secret' : 'Show admin secret');});
$('download-zip').addEventListener('click', () => generated && download(generated.zipName, generated.zip, 'application/zip'));
$('task').addEventListener('change', () => {
  const currentTask = task();
  selected.clear(); $('ack').checked = false;
  $('task-detail').hidden = !currentTask; $('custom-groups').hidden = !!currentTask;
  $('type').disabled = !!currentTask; $('list-all').disabled = !!currentTask;
  if (currentTask) {
    $('type').value = 'admin'; $('list-all').checked = false;
    $('role').value = currentTask.name; $('purpose').value = currentTask.description;
    $('task-name').textContent = currentTask.name;
    $('task-description').textContent = currentTask.description;
    $('task-actions').textContent = currentTask.api_actions.map(([service, action]) => `${service}.${action}`).join(', ');
    $('task-note').textContent = currentTask.notes;
    $('task-source').href = `https://github.com/Kaltura-EDU/api-scripts/blob/${referenceRevision}/${currentTask.source}`;
  }
  render(); update();
});
Promise.all([fetch('/groups.json').then(r => {if (!r.ok) throw Error('catalog'); return r.json();}),
  fetch('/template.py').then(r => {if (!r.ok) throw Error('template'); return r.text();}),
  fetch('/tasks.json').then(r => {if (!r.ok) throw Error('tasks'); return r.json();}),
  fetch('/task-template.py').then(r => {if (!r.ok) throw Error('runtime'); return r.text();})])
  .then(([catalog, source, taskCatalog, runtime]) => {
    groups = catalog; template = source; tasks = taskCatalog; taskTemplate = runtime;
    for (const capability of ['read', 'write', 'delete']) {
      const optionGroup = document.createElement('optgroup'); optionGroup.label = accessLabels[capability];
      for (const item of tasks.filter(t => t.capability === capability)) {
        const option = document.createElement('option'); option.value = item.id; option.textContent = item.name; optionGroup.append(option);
      }
      $('task').append(optionGroup);
    }
    render(); update();
  })
  .catch(() => {$('status').textContent = 'Could not load generator files. Restart the local dashboard.';});
