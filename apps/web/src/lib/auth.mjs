import { createHmac, timingSafeEqual, randomBytes } from "node:crypto";
export function equal(a, b) {
  const x = Buffer.from(a || ""),
    y = Buffer.from(b || "");
  return x.length === y.length && timingSafeEqual(x, y);
}
export function issueSession(secret, now = Date.now()) {
  const payload = `${now}:${randomBytes(16).toString("hex")}`;
  return `${payload}.${createHmac("sha256", secret).update(payload).digest("hex")}`;
}
export function verifySession(value, secret, now = Date.now()) {
  if (!value || !secret) return false;
  const [payload, signature, ...extra] = value.split(".");
  if (extra.length || !payload || !signature) return false;
  const time = Number(payload.split(":")[0]);
  if (!Number.isFinite(time) || time > now || now - time > 3600000)
    return false;
  return equal(
    signature,
    createHmac("sha256", secret).update(payload).digest("hex"),
  );
}
