// Luminous Orbis online server (Cloudflare Worker + Durable Object).
// One public world split into channels of up to CAP players each,
// plus a single "Social" object for friends and friend-only messages.
import { DurableObject } from "cloudflare:workers";

const CAP = 60;              // players per channel
const MAX_CH = 20;
const JOBS = ["warrior", "thief", "archer", "mage", "priest"];
const NG = ["死ね", "しね", "殺す", "ころす", "きもい", "キモい", "うざい", "ウザい", "fuck", "shit", "bitch"];

function cleanText(s, max) {
  s = Array.from(String(s == null ? "" : s)).filter((ch) => { const c = ch.codePointAt(0); return !(c < 32 || c === 127 || (c >= 0x200b && c <= 0x200f) || (c >= 0x2028 && c <= 0x202e)); }).join("").trim();
  for (const w of NG) {
    if (s.toLowerCase().includes(w.toLowerCase())) {
      const re = new RegExp(w.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"), "gi");
      s = s.replace(re, "＊".repeat(w.length));
    }
  }
  s = Array.from(s);
  return (s.length > max ? s.slice(0, max) : s).join("");
}
const PID = /^[0-9a-f]{16}$/;
const PKEY = /^[0-9a-f]{32}$/;
const MAX_FRIENDS = 50, MAX_REQS = 20, MAIL_KEEP = 20;
async function sha(s) { const b = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(s)); return [...new Uint8Array(b)].map((x) => x.toString(16).padStart(2, "0")).join(""); }
const num = (v, lo, hi) => { v = Number(v); if (!Number.isFinite(v)) return 0; return Math.max(lo, Math.min(hi, Math.round(v * 100) / 100)); };

export default {
  async fetch(req, env) {
    const url = new URL(req.url);
    if (url.pathname === "/" || url.pathname === "/health") {
      return new Response("Luminous Orbis server OK", { headers: { "content-type": "text/plain; charset=utf-8", "access-control-allow-origin": "*" } });
    }
    if (url.pathname === "/social") {
      if (req.headers.get("Upgrade") !== "websocket") return new Response("expected websocket", { status: 426 });
      if (!PID.test(url.searchParams.get("p") || "")) return new Response("bad id", { status: 400 });
      return env.SOCIAL.get(env.SOCIAL.idFromName("social")).fetch(req);
    }
    if (url.pathname === "/ws") {
      if (req.headers.get("Upgrade") !== "websocket") return new Response("expected websocket", { status: 426 });
      let ch = parseInt(url.searchParams.get("ch") || "1", 10);
      if (!(ch >= 1 && ch <= MAX_CH)) ch = 1;
      const stub = env.WORLD.get(env.WORLD.idFromName("world-ch" + ch));
      return stub.fetch(req);
    }
    return new Response("not found", { status: 404 });
  },
};

export class World extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    // keep-alive pings are answered without waking the object
    this.ctx.setWebSocketAutoResponse(new WebSocketRequestResponsePair("ping", "pong"));
  }

  players() {
    const out = [];
    for (const ws of this.ctx.getWebSockets()) { const a = ws.deserializeAttachment(); if (a && a.joined) out.push([ws, a]); }
    return out;
  }
  send(ws, obj) { try { ws.send(JSON.stringify(obj)); } catch (e) {} }
  broadcast(obj, except) {
    const s = JSON.stringify(obj);
    for (const [ws] of this.players()) if (ws !== except) { try { ws.send(s); } catch (e) {} }
  }

  async fetch(req) {
    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);
    if (this.players().length >= CAP) {
      this.ctx.acceptWebSocket(server);
      server.serializeAttachment({ joined: false });
      this.send(server, { t: "full" });
      server.close(4001, "full");
      return new Response(null, { status: 101, webSocket: client });
    }
    this.ctx.acceptWebSocket(server);
    server.serializeAttachment({ joined: false, id: crypto.randomUUID().slice(0, 8) });
    return new Response(null, { status: 101, webSocket: client });
  }

  async webSocketMessage(ws, raw) {
    if (typeof raw !== "string" || raw.length > 2048) return;
    let m; try { m = JSON.parse(raw); } catch (e) { return; }
    const a = ws.deserializeAttachment() || {};
    const now = Date.now();
    if (m.t === "hello") {
      if (a.joined) return;
      a.joined = true;
      a.name = cleanText(m.name, 10) || "旅人";
      a.job = JOBS.includes(m.job) ? m.job : "warrior";
      a.app = /^[0-9a-z.\-]{0,400}$/.test(String(m.app || "")) ? String(m.app) : "";
      a.p = PID.test(String(m.p || "")) ? String(m.p) : "";
      a.s = [0, 0, 8, Math.PI, 0];
      a.chatT = 0; a.chatN = 0;
      ws.serializeAttachment(a);
      const others = this.players().filter(([w]) => w !== ws).map(([, p]) => ({ id: p.id, p: p.p, n: p.name, j: p.job, a: p.app, s: p.s }));
      this.send(ws, { t: "welcome", id: a.id, cap: CAP, players: others });
      this.broadcast({ t: "join", p: { id: a.id, p: a.p, n: a.name, j: a.job, a: a.app, s: a.s } }, ws);
      return;
    }
    if (!a.joined) return;
    if (m.t === "s") {
      a.s = [num(m.x, -500, 500), num(m.y, -100, 300), num(m.z, -500, 500), num(m.r, -100, 100), Math.max(0, Math.min(15, m.a | 0))];
      ws.serializeAttachment(a);
      this.broadcast({ t: "s", id: a.id, s: a.s }, ws);
      return;
    }
    if (m.t === "look") {
      a.job = JOBS.includes(m.job) ? m.job : a.job;
      if (/^[0-9a-z.\-]{0,400}$/.test(String(m.app || ""))) a.app = String(m.app);
      a.name = cleanText(m.name, 10) || a.name;
      ws.serializeAttachment(a);
      this.broadcast({ t: "look", id: a.id, n: a.name, j: a.job, a: a.app }, ws);
      return;
    }
    // public chat is off: players talk only with friends (see Social)
  }

  async webSocketClose(ws, code, reason) { this.leave(ws); try { ws.close(code, "bye"); } catch (e) {} }
  async webSocketError(ws) { this.leave(ws); }
  leave(ws) {
    const a = ws.deserializeAttachment();
    if (a && a.joined && !a.left) { a.left = true; a.joined = false; try { ws.serializeAttachment(a); } catch (e) {} this.broadcast({ t: "l", id: a.id }, ws); }
  }
}

// Friends, friend requests and friend-only messages for every player.
// A player is a random 16-hex id plus a secret key kept on their device.
export class Social extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    this.ctx.setWebSocketAutoResponse(new WebSocketRequestResponsePair("ping", "pong"));
  }
  send(ws, obj) { try { ws.send(JSON.stringify(obj)); } catch (e) {} }
  socks(pid) { return this.ctx.getWebSockets(pid).filter((w) => { const a = w.deserializeAttachment(); return a && a.auth; }); }
  online(pid) { return this.socks(pid).length > 0; }
  tell(pid, obj) { for (const w of this.socks(pid)) this.send(w, obj); }
  async acct(pid) { return (await this.ctx.storage.get("a:" + pid)) || null; }
  async list(key) { return (await this.ctx.storage.get(key)) || []; }

  async fetch(req) {
    const p = new URL(req.url).searchParams.get("p");
    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);
    this.ctx.acceptWebSocket(server, [p]);
    server.serializeAttachment({ p, auth: false, n: "", rq: [], dm: [] });
    return new Response(null, { status: 101, webSocket: client });
  }

  async webSocketMessage(ws, raw) {
    if (typeof raw !== "string" || raw.length > 1024) return;
    let m; try { m = JSON.parse(raw); } catch (e) { return; }
    const a = ws.deserializeAttachment() || {};
    const me = a.p, st = this.ctx.storage, now = Date.now();
    if (m.t === "hello") {
      if (a.auth || !PKEY.test(String(m.k || ""))) return;
      const h = await sha(me + ":" + m.k);
      const acc = await this.acct(me);
      if (acc && acc.h !== h) { this.send(ws, { t: "err", m: "auth" }); ws.close(4003, "auth"); return; }
      const name = cleanText(m.n, 10) || (acc && acc.n) || "旅人";
      await st.put("a:" + me, { h, n: name });
      const wasOn = this.online(me);
      a.auth = true; a.n = name; ws.serializeAttachment(a);
      const fr = await this.list("f:" + me), friends = [];
      for (const f of fr) { const fa = await this.acct(f); friends.push({ p: f, n: fa ? fa.n : "旅人", on: this.online(f) }); }
      const reqs = await this.list("r:" + me);
      const mail = await this.list("m:" + me);
      if (mail.length) await st.delete("m:" + me);
      this.send(ws, { t: "welcome", friends, reqs, mail });
      if (!wasOn) for (const f of fr) this.tell(f, { t: "on", p: me, n: name, on: true });
      return;
    }
    if (!a.auth) return;
    const ok = (arr, n, win) => { const r = (arr || []).filter((t) => now - t < win); if (r.length >= n) return null; r.push(now); return r; };
    if (m.t === "req") {
      const to = String(m.to || "");
      if (!PID.test(to) || to === me || !(await this.acct(to))) { this.send(ws, { t: "sys", m: "このプレイヤーには申請できません" }); return; }
      const rq = ok(a.rq, 10, 60000); if (!rq) { this.send(ws, { t: "sys", m: "申請が多すぎます。少し待ってください" }); return; }
      a.rq = rq; ws.serializeAttachment(a);
      const myF = await this.list("f:" + me);
      if (myF.includes(to)) { this.send(ws, { t: "sys", m: "すでにフレンドです" }); return; }
      const mine = await this.list("r:" + me);
      if (mine.some((r) => r.p === to)) { await this.befriend(me, to); return; }   // they asked first: accept
      const theirs = (await this.list("r:" + to)).filter((r) => r.p !== me);
      theirs.push({ p: me, n: a.n, ts: now }); while (theirs.length > MAX_REQS) theirs.shift();
      await st.put("r:" + to, theirs);
      this.tell(to, { t: "req", p: me, n: a.n });
      this.send(ws, { t: "sent", p: to });
      return;
    }
    if (m.t === "acc" || m.t === "dec") {
      const from = String(m.p || ""); const mine = await this.list("r:" + me);
      if (!mine.some((r) => r.p === from)) return;
      await st.put("r:" + me, mine.filter((r) => r.p !== from));
      if (m.t === "acc") await this.befriend(me, from);
      else this.send(ws, { t: "reqgone", p: from });
      return;
    }
    if (m.t === "rm") {
      const o = String(m.p || "");
      await st.put("f:" + me, (await this.list("f:" + me)).filter((x) => x !== o));
      if (PID.test(o)) await st.put("f:" + o, (await this.list("f:" + o)).filter((x) => x !== me));
      this.tell(me, { t: "frrm", p: o }); this.tell(o, { t: "frrm", p: me });
      return;
    }
    if (m.t === "dm") {
      const to = String(m.to || "");
      if (!(await this.list("f:" + me)).includes(to)) { this.send(ws, { t: "sys", m: "フレンドにだけメッセージを送れます" }); return; }
      const dm = ok(a.dm, 3, 6000); if (!dm) { this.send(ws, { t: "sys", m: "送信が早すぎます。少し待ってください" }); return; }
      a.dm = dm; ws.serializeAttachment(a);
      const text = cleanText(m.m, 80); if (!text) return;
      const line = { f: me, n: a.n, m: text, ts: now };
      if (this.online(to)) this.tell(to, { t: "dm", ...line });
      else { const box = await this.list("m:" + to); box.push(line); while (box.length > MAIL_KEEP) box.shift(); await st.put("m:" + to, box); }
      this.tell(me, { t: "dmok", to, m: text, ts: now });
      return;
    }
  }

  async befriend(x, y) {
    const st = this.ctx.storage;
    const fx = await this.list("f:" + x), fy = await this.list("f:" + y);
    if (fx.length >= MAX_FRIENDS || fy.length >= MAX_FRIENDS) { this.tell(x, { t: "sys", m: "フレンドが上限（" + MAX_FRIENDS + "人）です" }); return; }
    if (!fx.includes(y)) fx.push(y); if (!fy.includes(x)) fy.push(x);
    await st.put("f:" + x, fx); await st.put("f:" + y, fy);
    await st.put("r:" + x, (await this.list("r:" + x)).filter((r) => r.p !== y));
    await st.put("r:" + y, (await this.list("r:" + y)).filter((r) => r.p !== x));
    const ax = await this.acct(x), ay = await this.acct(y);
    this.tell(x, { t: "fr", p: y, n: ay ? ay.n : "旅人", on: this.online(y) });
    this.tell(y, { t: "fr", p: x, n: ax ? ax.n : "旅人", on: this.online(x) });
  }

  async webSocketClose(ws, code) { await this.gone(ws); try { ws.close(code, "bye"); } catch (e) {} }
  async webSocketError(ws) { await this.gone(ws); }
  async gone(ws) {
    const a = ws.deserializeAttachment(); if (!a || !a.auth) return;
    a.auth = false; try { ws.serializeAttachment(a); } catch (e) {}
    if (this.online(a.p)) return;
    for (const f of await this.list("f:" + a.p)) this.tell(f, { t: "on", p: a.p, on: false });
  }
}
