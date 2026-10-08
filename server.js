import express from 'express';
import { spawn } from 'node:child_process';
import { promises as fs, createReadStream, createWriteStream } from 'node:fs';
import { pipeline } from 'node:stream/promises';
import { Readable } from 'node:stream';
import os from 'node:os';
import path from 'node:path';
import crypto from 'node:crypto';

const PORT = process.env.PORT || 8080;
const RENDER_TOKEN = process.env.RENDER_TOKEN || '';
const PUBLIC_URL = (process.env.PUBLIC_URL ||
  (process.env.RAILWAY_PUBLIC_DOMAIN ? 'https://' + process.env.RAILWAY_PUBLIC_DOMAIN : '')).replace(/\/$/, '');
const FILE_TTL_MS = Number(process.env.FILE_TTL_MS || 2 * 60 * 60 * 1000);
const jobs = new Map();

async function downloadUrl(url, destPath) {
  const resp = await fetch(url, { redirect: 'follow' });
  if (!resp.ok || !resp.body) throw new Error(`download failed (${resp.status}) for ${url}`);
  const ctype = resp.headers.get('content-type') || '';
  if (ctype.includes('text/html')) {
    const html = await resp.text();
    const formAction = html.match(/action="(https:\/\/drive\.usercontent\.google\.com\/download[^"]*)"/);
    if (formAction) {
      const params = {}; const re = /name="([^"]+)"\s+value="([^"]*)"/g; let mm;
      while ((mm = re.exec(html)) !== null) params[mm[1]] = mm[2];
      const retryUrl = formAction[1].replace(/&amp;/g, '&') + (formAction[1].includes('?') ? '&' : '?') + new URLSearchParams(params).toString();
      const r2 = await fetch(retryUrl, { redirect: 'follow' });
      if (!r2.ok || !r2.body) throw new Error(`download retry failed (${r2.status}) for ${url}`);
      await pipeline(Readable.fromWeb(r2.body), createWriteStream(destPath)); return;
    }
    const m = html.match(/href="(\/uc\?export=download[^"]+)"/);
    if (m) {
      const r2 = await fetch('https://drive.google.com' + m[1].replace(/&amp;/g, '&'), { redirect: 'follow' });
      if (!r2.ok || !r2.body) throw new Error(`download retry failed (${r2.status}) for ${url}`);
      await pipeline(Readable.fromWeb(r2.body), createWriteStream(destPath)); return;
    }
    throw new Error(`got HTML instead of file for ${url} (is it shared "anyone with link"?)`);
  }
  await pipeline(Readable.fromWeb(resp.body), createWriteStream(destPath));
}

function runScript(scriptPath, cwd) {
  return new Promise((resolve, reject) => {
    const child = spawn('bash', [scriptPath], { cwd, env: process.env });
    let stderr = '';
    child.stdout.on('data', (d) => process.stdout.write(d));
    child.stderr.on('data', (d) => { stderr += d.toString(); if (stderr.length > 20000) stderr = stderr.slice(-8000); process.stderr.write(d); });
    child.on('error', reject);
    child.on('close', (code) => code === 0 ? resolve() : reject(new Error('render script exited with code ' + code + '\n' + stderr.slice(-4000))));
  });
}

async function doRender(work, { files, script, script_name, outName }) {
  for (const f of files) {
    if (!f || !f.name || !f.url) continue;
    await downloadUrl(f.url, path.join(work, f.name));
    console.log(`[render] downloaded ${f.name}`);
  }
  let scriptToRun;
  if (typeof script === 'string' && script.trim()) { scriptToRun = 'run.sh'; await fs.writeFile(path.join(work, scriptToRun), script, 'utf8'); }
  else if (script_name) scriptToRun = script_name;
  else { const sh = (await fs.readdir(work)).filter((n) => /\.sh$/i.test(n)).sort(); scriptToRun = sh[sh.length - 1]; }
  if (!scriptToRun) throw new Error('no script to run (provide script, script_name, or a .sh file)');
  await runScript(path.join(work, scriptToRun), work);
  const finalPath = path.join(work, outName);
  await fs.access(finalPath).catch(() => { throw new Error(`render finished but ${outName} was not produced`); });
  return finalPath;
}

async function postCallback(url, payload) {
  for (let a = 1; a <= 5; a++) {
    try {
      const r = await fetch(url, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(payload) });
      console.log(`[callback] ${payload.job_id} -> ${r.status}`);
      if (r.ok) return;
    } catch (e) { console.error(`[callback] attempt ${a} failed:`, e.message); }
    await new Promise((r) => setTimeout(r, a * 5000));
  }
}

function scheduleCleanup(id) {
  setTimeout(() => { const j = jobs.get(id); if (j) fs.rm(j.work, { recursive: true, force: true }).catch(() => {}); jobs.delete(id); }, FILE_TTL_MS).unref();
}

const app = express();
app.use(express.json({ limit: '10mb' }));
function checkAuth(req, res) {
  if (!RENDER_TOKEN) return true;
  if ((req.get('authorization') || '') === `Bearer ${RENDER_TOKEN}`) return true;
  res.status(401).json({ error: 'unauthorized' }); return false;
}

app.get('/', (_q, res) => res.json({ ok: true, service: 'railway-github-ffmpeg', async: true }));
app.get('/health', (_q, res) => res.json({ ok: true }));

app.get('/jobs/:id', (req, res) => {
  if (!checkAuth(req, res)) return;
  const j = jobs.get(req.params.id);
  if (!j) return res.status(404).json({ error: 'unknown job' });
  res.json({ job_id: req.params.id, status: j.status, out: j.out, error: j.error || null, startedAt: j.startedAt, finishedAt: j.finishedAt || null });
});

app.get('/jobs/:id/file', async (req, res) => {
  if (!checkAuth(req, res)) return;
  const j = jobs.get(req.params.id);
  if (!j || j.status !== 'done') return res.status(404).json({ error: 'file not ready' });
  const p = path.join(j.work, j.out);
  const st = await fs.stat(p).catch(() => null);
  if (!st) return res.status(410).json({ error: 'file expired' });
  res.setHeader('Content-Type', 'video/mp4');
  res.setHeader('Content-Length', st.size);
  res.setHeader('Content-Disposition', `attachment; filename="${j.out}"`);
  await pipeline(createReadStream(p), res).catch(() => {});
});

app.post('/render', async (req, res) => {
  if (!checkAuth(req, res)) return;
  const { topic_title, files, script, script_name, out, job_id, callback_url, meta } = req.body || {};
  if (!Array.isArray(files) || files.length === 0) return res.status(400).json({ error: 'files[] is required (array of { name, url })' });
  const outName = (out && /^[\w.\-]+$/.test(out)) ? out : 'final.mp4';

  if (callback_url) {
    const id = (job_id && /^[\w.\-]{1,120}$/.test(job_id)) ? job_id : crypto.randomUUID();
    const existing = jobs.get(id);
    if (existing && existing.status === 'running') return res.status(202).json({ accepted: false, job_id: id, status: 'already_running' });
    if (existing) fs.rm(existing.work, { recursive: true, force: true }).catch(() => {});
    const work = await fs.mkdtemp(path.join(os.tmpdir(), 'render-'));
    const job = { status: 'running', out: outName, work, startedAt: new Date().toISOString() };
    jobs.set(id, job);
    console.log(`[render:async] job=${id} topic="${topic_title || ''}" files=${files.length} out=${outName}`);
    res.status(202).json({ accepted: true, job_id: id, status: 'running', out: outName });
    (async () => {
      const base = PUBLIC_URL || `https://${req.get('host')}`;
      try {
        await doRender(work, { files, script, script_name, outName });
        job.status = 'done'; job.finishedAt = new Date().toISOString();
        await postCallback(callback_url, { job_id: id, status: 'done', out: outName, download_url: `${base}/jobs/${encodeURIComponent(id)}/file`, error: null, topic_title: topic_title || '', meta: meta || {} });
      } catch (err) {
        job.status = 'error'; job.error = err.message; job.finishedAt = new Date().toISOString();
        console.error(`[render:async] job=${id} error:`, err);
        await postCallback(callback_url, { job_id: id, status: 'error', out: outName, download_url: null, error: String(err.message).slice(-2000), topic_title: topic_title || '', meta: meta || {} });
      } finally { scheduleCleanup(id); }
    })();
    return;
  }

  const work = await fs.mkdtemp(path.join(os.tmpdir(), 'render-'));
  try {
    const finalPath = await doRender(work, { files, script, script_name, outName });
    res.setHeader('Content-Type', 'video/mp4');
    res.setHeader('Content-Disposition', `attachment; filename="${outName}"`);
    await pipeline(createReadStream(finalPath), res);
  } catch (err) {
    console.error('[render] error:', err);
    if (!res.headersSent) res.status(500).json({ error: err.message }); else res.end();
  } finally { fs.rm(work, { recursive: true, force: true }).catch(() => {}); }
});

app.listen(PORT, () => console.log(`railway-github-ffmpeg listening on :${PORT}`));
