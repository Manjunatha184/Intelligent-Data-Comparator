export const API_BASE = "/api/v1";

export function formatApiError(detail) {
  if (!detail) return "";
  if (typeof detail === "string") return detail;

  if (Array.isArray(detail)) {
    return detail
      .map((item) => {
        const location = Array.isArray(item?.loc)
          ? item.loc.join(".")
          : item?.loc;
        return [location, item?.msg]
          .filter(Boolean)
          .join(": ");
      })
      .filter(Boolean)
      .join("; ");
  }

  return detail?.msg || "";
}

export async function apiRequest(path, options = {}) {
  const method = String(options.method || "GET").toUpperCase();
  const response = await fetch(`${API_BASE}${path}`, {
    ...options,
    method,
    cache: method === "GET" ? "no-store" : options.cache,
    headers: {
      "Content-Type": "application/json",
      ...(options.headers || {}),
    },
  });

  const text = await response.text();
  let data = null;

  try {
    data = text ? JSON.parse(text) : null;
  } catch {
    data = text;
  }

  if (!response.ok) {
    throw new Error(
      formatApiError(data?.detail) ||
      data?.message ||
      `Request failed with status ${response.status}`
    );
  }

  return data;
}
