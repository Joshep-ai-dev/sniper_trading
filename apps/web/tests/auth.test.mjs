import { test } from "node:test";
import assert from "node:assert/strict";
import { issueSession, verifySession, equal } from "../src/lib/auth.mjs";
test("session signature and expiration enforced", () => {
  const token = issueSession("private-secret", 1000);
  assert.equal(verifySession(token, "private-secret", 2000), true);
  assert.equal(verifySession(token, "wrong", 2000), false);
  assert.equal(verifySession(token + "a", "private-secret", 2000), false);
  assert.equal(verifySession(token, "private-secret", 3601001), false);
  assert.equal(verifySession(token, "private-secret", 999), false);
});
test("credentials compared without truncation", () => {
  assert.equal(equal("a", "aa"), false);
  assert.equal(equal("long-password", "long-password"), true);
  assert.equal(equal("long-password", "wrong-password"), false);
});
