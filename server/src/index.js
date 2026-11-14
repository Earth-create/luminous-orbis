// Luminous Orbis online server (Cloudflare Worker + Durable Object).
// One public world split into channels of up to CAP players each.
import { DurableObject } from "cloudflare:workers";

const CAP = 60;              // players per channel
const MAX_CH = 20;
const CHAT_KEEP = 30;        // recent chat lines kept for newcomers
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
const num = (v, lo, hi) => { v = Number(v); if (!Number.isFinite(v)) return 0; return Math.max(lo, Math.min(hi, Math.round(v * 100) / 100)); };

export default {
  async fetch(req, env) {
    const url = new URL(req.url);
    if (url.pathname === "/" || url.pathname === "/health") {
      return new Response("Luminous Orbis server OK", { headers: { "content-type": "text/plain; charset=utf-8", "access-control-allow-origin": "*" } });
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
    this.chat = null;
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
  async recentChat() {
    if (!this.chat) this.chat = (await this.ctx.storage.get("chat")) || [];
    return this.chat;
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
      a.s = [0, 0, 8, Math.PI, 0];
      a.chatT = 0; a.chatN = 0;
      ws.serializeAttachment(a);
      const others = this.players().filter(([w]) => w !== ws).map(([, p]) => ({ id: p.id, n: p.name, j: p.job, a: p.app, s: p.s }));
      this.send(ws, { t: "welcome", id: a.id, cap: CAP, players: others, chat: await this.recentChat() });
      this.broadcast({ t: "join", p: { id: a.id, n: a.name, j: a.job, a: a.app, s: a.s } }, ws);
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
    if (m.t === "c") {
      // at most 3 lines per 6 seconds
      if (now - (a.chatT || 0) > 6000) { a.chatT = now; a.chatN = 0; }
      if (a.chatN >= 3) { this.send(ws, { t: "sys", m: "チャットの送信が早すぎます。少し待ってください。" }); return; }
      const text = cleanText(m.m, 80);
      if (!text) return;
      a.chatN++; ws.serializeAttachment(a);
      const line = { id: a.id, n: a.name, m: text, ts: now };
      const chat = await this.recentChat();
      chat.push(line); while (chat.length > CHAT_KEEP) chat.shift();
      await this.ctx.storage.put("chat", chat);
      this.broadcast({ t: "c", ...line });
      return;
    }
  }

  async webSocketClose(ws, code, reason) { this.leave(ws); try { ws.close(code, "bye"); } catch (e) {} }
  async webSocketError(ws) { this.leave(ws); }
  leave(ws) {
    const a = ws.deserializeAttachment();
    if (a && a.joined && !a.left) { a.left = true; a.joined = false; try { ws.serializeAttachment(a); } catch (e) {} this.broadcast({ t: "l", id: a.id }, ws); }
  }
}
