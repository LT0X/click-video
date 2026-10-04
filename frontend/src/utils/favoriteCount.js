export function favoriteCountAfterAction(headers, currentCount, wasFavorite) {
  const rawCount = headers?.["x-favorite-count"] ?? headers?.["X-Favorite-Count"];
  if (typeof rawCount === "string" && /^\d+$/.test(rawCount)) {
    const count = Number(rawCount);
    if (Number.isSafeInteger(count)) return count;
  }

  const current = Number(currentCount);
  const base = Number.isFinite(current) ? current : 0;
  return Math.max(0, base + (wasFavorite ? -1 : 1));
}
