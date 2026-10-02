import { NextRequest, NextResponse } from "next/server";
import { equal, issueSession } from "@/lib/auth.mjs";
const failures = new Map<string, { count: number; time: number }>();
export async function POST(request: NextRequest) {
  const origin = request.headers.get("origin");
  if (origin !== request.nextUrl.origin)
    return NextResponse.json({ error: "Origin rejected" }, { status: 403 });
  const password = process.env.SNIPER_UI_PASSWORD,
    secret = process.env.SNIPER_API_TOKEN;
  if (!password || password.length < 16 || !secret)
    return NextResponse.json(
      {
        error:
          "Operator authentication is not configured. Set SNIPER_UI_PASSWORD and SNIPER_API_TOKEN on the Next.js server.",
      },
      { status: 503 },
    );
  const key = request.headers.get("x-forwarded-for")?.split(",")[0] || "local";
  const now = Date.now();
  const failure = failures.get(key);
  if (failure && now - failure.time < 60000 && failure.count >= 5)
    return NextResponse.json(
      { error: "Try again in one minute" },
      { status: 429 },
    );
  const body = await request.json();
  if (!equal(body.password, password)) {
    failures.set(key, { count: (failure?.count || 0) + 1, time: now });
    if (failures.size > 1000) failures.clear();
    return NextResponse.json(
      { error: "Incorrect operator password" },
      { status: 401 },
    );
  }
  failures.delete(key);
  const response = NextResponse.json({ ok: true });
  response.cookies.set("sniper_operator", issueSession(secret), {
    httpOnly: true,
    secure: request.nextUrl.protocol === "https:",
    sameSite: "strict",
    path: "/",
    maxAge: 3600,
  });
  return response;
}
