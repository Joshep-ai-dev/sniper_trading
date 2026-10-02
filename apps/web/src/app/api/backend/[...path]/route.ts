import { NextRequest, NextResponse } from "next/server";
import { verifySession } from "@/lib/auth.mjs";
export const runtime = "nodejs";
async function proxy(
  request: NextRequest,
  context: { params: Promise<{ path: string[] }> },
) {
  const token = process.env.SNIPER_API_TOKEN;
  if (
    !token ||
    !verifySession(request.cookies.get("sniper_operator")?.value, token)
  )
    return NextResponse.json(
      { error: "Operator login required" },
      { status: 401 },
    );
  if (
    request.headers.get("origin") &&
    request.headers.get("origin") !== request.nextUrl.origin
  )
    return NextResponse.json({ error: "Origin rejected" }, { status: 403 });
  const { path } = await context.params;
  if (
    ![
      "state",
      "action",
      "sell",
      "history",
      "analytics",
      "database",
      "credentials",
      "test",
      "wallet",
      "session",
    ].includes(path[0]) ||
    path.some((p) => !/^[a-zA-Z0-9_-]+$/.test(p))
  )
    return NextResponse.json({ error: "Unknown operation" }, { status: 404 });
  if (Number(request.headers.get("content-length") || 0) > 65536)
    return NextResponse.json({ error: "Request too large" }, { status: 413 });
  const body = request.method === "GET" ? undefined : await request.text();
  if (body && body.length > 65536)
    return NextResponse.json({ error: "Request too large" }, { status: 413 });
  try {
    const response = await fetch(
      `${process.env.SNIPER_BACKEND_URL || "http://127.0.0.1:8787"}/api/${path.join("/")}${request.nextUrl.search}`,
      {
        method: request.method,
        body,
        headers: {
          authorization: `Bearer ${token}`,
          "content-type": "application/json",
          "x-sniper-https": String(
            request.nextUrl.protocol === "https:" ||
              request.headers.get("x-forwarded-proto") === "https",
          ),
        },
        cache: "no-store",
        signal: AbortSignal.timeout(15000),
      },
    );
    const outgoing = new NextResponse(await response.text(), {
      status: response.status,
      headers: {
        "content-type": "application/json",
        "cache-control": "no-store",
      },
    });
    const cookie = response.headers.get("set-cookie");
    if (cookie) outgoing.headers.set("set-cookie", cookie);
    return outgoing;
  } catch {
    return NextResponse.json(
      { error: "Rust trading service is unreachable" },
      { status: 503 },
    );
  }
}
export const GET = proxy;
export const POST = proxy;
