export function buildChatSocketURL(token) {
  const baseURL = `${process.env.REACT_APP_API_URL}:${process.env.REACT_APP_API_PORT}${process.env.REACT_APP_API_PATH}`;
  const url = new URL(`${baseURL.replace(/\/$/, "")}/message/ws`);
  url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
  url.searchParams.set("token", token);
  return url.toString();
}

function sameMessage(left, right) {
  if (left?.event_id && right?.event_id && left.event_id === right.event_id) return true;
  if (Number(left?.id) > 0 && Number(right?.id) > 0) return Number(left.id) === Number(right.id);
  const sameContent =
    Number(left?.from_user_id) === Number(right?.from_user_id) &&
    Number(left?.to_user_id) === Number(right?.to_user_id) &&
    left?.content === right?.content;
  if (!sameContent) return false;
  if (Number(left?.create_time) === Number(right?.create_time)) return true;

  const leftPending = String(left?.event_id || "").startsWith("fallback-");
  const rightPending = String(right?.event_id || "").startsWith("fallback-");
  const pendingPair = (leftPending && Number(right?.id) > 0) || (rightPending && Number(left?.id) > 0);
  return pendingPair && Math.abs(Number(left?.create_time) - Number(right?.create_time)) <= 5000;
}

export function mergeChatMessages(existing = [], incoming = []) {
  const merged = [...existing];
  incoming.forEach((message) => {
    if (!merged.some((item) => sameMessage(item, message))) merged.push(message);
  });
  return merged.sort((left, right) => Number(left?.create_time || 0) - Number(right?.create_time || 0));
}
