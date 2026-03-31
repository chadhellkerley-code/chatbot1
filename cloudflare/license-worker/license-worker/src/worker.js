var __defProp = Object.defineProperty;
var __name = (target, value) => __defProp(target, "name", { value, configurable: true });

// src/worker.ts
var JSON_HEADERS = {
  "content-type": "application/json; charset=utf-8"
};
function jsonResponse(body, status = 200) {
  return new Response(JSON.stringify(body), { status, headers: JSON_HEADERS });
}
__name(jsonResponse, "jsonResponse");
function nowIso() {
  return (/* @__PURE__ */ new Date()).toISOString();
}
__name(nowIso, "nowIso");
function asString(value) {
  if (value === null || value === void 0) return "";
  return String(value).trim();
}
__name(asString, "asString");
function parseDays(value) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return 0;
  return Math.floor(parsed);
}
__name(parseDays, "parseDays");
function base64UrlEncode(data) {
  const bytes = new Uint8Array(data);
  let raw = "";
  for (const byte of bytes) {
    raw += String.fromCharCode(byte);
  }
  return btoa(raw).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/g, "");
}
__name(base64UrlEncode, "base64UrlEncode");
async function hmacSign(secret, message) {
  const enc = new TextEncoder();
  const key = await crypto.subtle.importKey(
    "raw",
    enc.encode(secret),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"]
  );
  const signature = await crypto.subtle.sign("HMAC", key, enc.encode(message));
  return base64UrlEncode(signature);
}
__name(hmacSign, "hmacSign");
async function signToken(licenseId, secret) {
  const signature = await hmacSign(secret, licenseId);
  return `${licenseId}.${signature}`;
}
__name(signToken, "signToken");
async function verifyToken(token, secret) {
  const trimmed = token.trim();
  if (!trimmed || !trimmed.includes(".")) return null;
  const [licenseId, signature] = trimmed.split(".", 2);
  if (!licenseId || !signature) return null;
  const expected = await hmacSign(secret, licenseId);
  if (expected !== signature) return null;
  return licenseId;
}
__name(verifyToken, "verifyToken");
async function readJson(req) {
  try {
    return await req.json();
  } catch {
    return {};
  }
}
__name(readJson, "readJson");
function isExpired(expiresAt) {
  if (!expiresAt) return false;
  const ms = Date.parse(expiresAt);
  if (Number.isNaN(ms)) return false;
  return ms <= Date.now();
}
__name(isExpired, "isExpired");
async function resolveLicenseId(body, env) {
  const rawToken = asString(body.token || body.license_token || body.license_key);
  const rawId = asString(body.license_id || body.license_key);
  if (rawToken && env.LICENSE_HASH_SECRET) {
    const viaToken = await verifyToken(rawToken, env.LICENSE_HASH_SECRET);
    if (viaToken) {
      return { licenseId: viaToken, tokenUsed: true };
    }
  }
  if (rawId) {
    return { licenseId: rawId, tokenUsed: false };
  }
  if (rawToken && !env.LICENSE_HASH_SECRET) {
    return { licenseId: rawToken, tokenUsed: false };
  }
  return { licenseId: null, tokenUsed: false };
}
__name(resolveLicenseId, "resolveLicenseId");
async function fetchLicense(env, licenseId) {
  return env.DB.prepare("SELECT * FROM licenses WHERE id = ?").bind(licenseId).first();
}
__name(fetchLicense, "fetchLicense");
async function handleHealth() {
  return jsonResponse({ ok: true });
}
__name(handleHealth, "handleHealth");
async function handleAdminCreate(req, env) {
  const adminToken = req.headers.get("x-admin-token") || "";
  if (!env.ADMIN_TOKEN || adminToken !== env.ADMIN_TOKEN) {
    return jsonResponse({ ok: false, error: "unauthorized" }, 401);
  }
  const body = await readJson(req);
  const name = asString(body.name);
  const days = parseDays(body.days);
  const email = asString(body.email || "");
  if (!name || days <= 0) {
    return jsonResponse({ ok: false, error: "invalid_payload" }, 400);
  }
  const licenseId = crypto.randomUUID();
  const createdAt = nowIso();
  const expiresAt = new Date(Date.now() + days * 86400 * 1e3).toISOString();
  await env.DB.prepare(
    "INSERT INTO licenses (id, name, email, status, expires_at, created_at, bound_fingerprint, last_seen_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
  ).bind(
    licenseId,
    name,
    email || null,
    "active",
    expiresAt,
    createdAt,
    null,
    null
  ).run();
  const response = {
    ok: true,
    license_id: licenseId,
    license_key: licenseId,
    expires_at: expiresAt
  };
  if (env.LICENSE_HASH_SECRET) {
    const token = await signToken(licenseId, env.LICENSE_HASH_SECRET);
    response.token = token;
    response.license_key = token;
  }
  return jsonResponse(response, 201);
}
__name(handleAdminCreate, "handleAdminCreate");
async function handleActivate(req, env) {
  const body = await readJson(req);
  const { licenseId } = await resolveLicenseId(body, env);
  if (!licenseId) {
    return jsonResponse({ ok: false, error: "missing_license" }, 400);
  }
  const machineId = asString(body.machine_id || body.machine || body.client_fingerprint || body.fingerprint);
  const fingerprint = asString(body.client_fingerprint || body.fingerprint || body.machine || body.machine_id);
  if (!machineId || !fingerprint) {
    return jsonResponse({ ok: false, error: "missing_machine" }, 400);
  }
  const license = await fetchLicense(env, licenseId);
  if (!license) {
    return jsonResponse({ ok: false, error: "license_not_found" }, 404);
  }
  const status = asString(license.status || "active").toLowerCase();
  const expiresAt = asString(license.expires_at || "");
  if (isExpired(expiresAt)) {
    await env.DB.prepare("UPDATE licenses SET status = ? WHERE id = ?").bind("expired", licenseId).run();
    return jsonResponse(
      { ok: false, error: "license_expired", status: "expired", expires_at: expiresAt },
      403
    );
  }
  if (status !== "active") {
    return jsonResponse(
      { ok: false, error: "license_inactive", status, expires_at: expiresAt },
      403
    );
  }
  const bound = asString(license.bound_fingerprint || "");
  if (bound && bound !== fingerprint) {
    return jsonResponse(
      { ok: false, error: "license_bound", status, expires_at: expiresAt },
      403
    );
  }
  if (!bound) {
    await env.DB.prepare("UPDATE licenses SET bound_fingerprint = ? WHERE id = ?").bind(fingerprint, licenseId).run();
  }
  const now = nowIso();
  await env.DB.prepare("UPDATE licenses SET last_seen_at = ? WHERE id = ?").bind(now, licenseId).run();
  const activation = await env.DB.prepare(
    "SELECT id FROM activations WHERE license_id = ? AND machine_id = ?"
  ).bind(licenseId, machineId).first();
  if (activation?.id) {
    await env.DB.prepare("UPDATE activations SET last_seen_at = ? WHERE id = ?").bind(now, activation.id).run();
  } else {
    await env.DB.prepare(
      "INSERT INTO activations (id, license_id, machine_id, created_at, last_seen_at) VALUES (?, ?, ?, ?, ?)"
    ).bind(crypto.randomUUID(), licenseId, machineId, now, now).run();
  }
  return jsonResponse({ ok: true, status, expires_at: expiresAt });
}
__name(handleActivate, "handleActivate");
async function handleValidate(req, env) {
  const body = await readJson(req);
  const { licenseId } = await resolveLicenseId(body, env);
  if (!licenseId) {
    return jsonResponse({ ok: false, error: "missing_license" }, 400);
  }
  const machineId = asString(body.machine_id || body.machine || body.client_fingerprint || body.fingerprint);
  const fingerprint = asString(body.client_fingerprint || body.fingerprint || body.machine || body.machine_id);
  if (!machineId || !fingerprint) {
    return jsonResponse({ ok: false, error: "missing_machine" }, 400);
  }
  const license = await fetchLicense(env, licenseId);
  if (!license) {
    return jsonResponse({ ok: false, error: "license_not_found" }, 404);
  }
  const status = asString(license.status || "active").toLowerCase();
  const expiresAt = asString(license.expires_at || "");
  if (isExpired(expiresAt)) {
    await env.DB.prepare("UPDATE licenses SET status = ? WHERE id = ?").bind("expired", licenseId).run();
    return jsonResponse(
      { ok: false, error: "license_expired", status: "expired", expires_at: expiresAt },
      403
    );
  }
  if (status !== "active") {
    return jsonResponse(
      { ok: false, error: "license_inactive", status, expires_at: expiresAt },
      403
    );
  }
  const bound = asString(license.bound_fingerprint || "");
  if (bound && bound !== fingerprint) {
    return jsonResponse(
      { ok: false, error: "license_bound", status, expires_at: expiresAt },
      403
    );
  }
  const now = nowIso();
  await env.DB.prepare("UPDATE licenses SET last_seen_at = ? WHERE id = ?").bind(now, licenseId).run();
  const activation = await env.DB.prepare(
    "SELECT id FROM activations WHERE license_id = ? AND machine_id = ?"
  ).bind(licenseId, machineId).first();
  if (activation?.id) {
    await env.DB.prepare("UPDATE activations SET last_seen_at = ? WHERE id = ?").bind(now, activation.id).run();
  }
  return jsonResponse({ ok: true, status, expires_at: expiresAt });
}
__name(handleValidate, "handleValidate");
async function handleUpdateManifest(env) {
  const key = env.UPDATE_MANIFEST_KEY || "manifest.json";
  const obj = await env.UPDATES_BUCKET.get(key);
  if (!obj) {
    return jsonResponse({ ok: false, error: "manifest_not_found" }, 404);
  }
  const body = await obj.text();
  return new Response(body, { headers: JSON_HEADERS });
}
__name(handleUpdateManifest, "handleUpdateManifest");
async function handleUpdateDownload(env, key) {
  const cleaned = key.replace(/^\/+/, "");
  if (!cleaned) {
    return jsonResponse({ ok: false, error: "missing_key" }, 400);
  }
  const obj = await env.UPDATES_BUCKET.get(cleaned);
  if (!obj) {
    return jsonResponse({ ok: false, error: "file_not_found" }, 404);
  }
  const headers = new Headers();
  const contentType = obj.httpMetadata?.contentType || "application/octet-stream";
  headers.set("content-type", contentType);
  headers.set("etag", obj.etag);
  return new Response(obj.body, { headers });
}
__name(handleUpdateDownload, "handleUpdateDownload");
var worker_default = {
  async fetch(req, env) {
    const url = new URL(req.url);
    const path = url.pathname.replace(/\/+$/, "") || "/";
    if (req.method === "GET" && path === "/health") {
      return handleHealth();
    }
    if (req.method === "POST" && path === "/admin/licenses") {
      return handleAdminCreate(req, env);
    }
    if (req.method === "POST" && path === "/activate") {
      return handleActivate(req, env);
    }
    if (req.method === "POST" && path === "/validate") {
      return handleValidate(req, env);
    }
    if (req.method === "GET" && path === "/update/manifest") {
      return handleUpdateManifest(env);
    }
    if (req.method === "GET" && path.startsWith("/updates/")) {
      return handleUpdateDownload(env, path.slice("/updates/".length));
    }
    return jsonResponse({ ok: false, error: "not_found" }, 404);
  }
};
export {
  worker_default as default
};
//# sourceMappingURL=worker.js.map
