export class ApiError extends Error {
  constructor(
    public status: number,
    message: string,
  ) {
    super(message);
  }
}
export async function api<T>(path: string, body?: unknown): Promise<T> {
  const response = await fetch(`/api/backend/${path}`, {
    method: body === undefined ? "GET" : "POST",
    headers: { "content-type": "application/json" },
    body: body === undefined ? undefined : JSON.stringify(body),
    cache: "no-store",
  });
  const json = await response.json();
  if (!response.ok)
    throw new ApiError(response.status, json.error || "Request failed");
  return json;
}
export const sol = (lamports: number | undefined) =>
  lamports === undefined
    ? "—"
    : (lamports / 1e9).toLocaleString(undefined, {
        minimumFractionDigits: 3,
        maximumFractionDigits: 6,
      });
export const short = (key: string) => `${key.slice(0, 5)}…${key.slice(-5)}`;
export const date = (ms: number) => (ms ? new Date(ms).toLocaleString() : "—");
