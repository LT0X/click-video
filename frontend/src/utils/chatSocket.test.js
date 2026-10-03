import { buildChatSocketURL, mergeChatMessages } from "./chatSocket";

describe("chat WebSocket helpers", () => {
  test("builds a WebSocket URL from the configured API base", () => {
    const original = {
      url: process.env.REACT_APP_API_URL,
      port: process.env.REACT_APP_API_PORT,
      path: process.env.REACT_APP_API_PATH,
    };
    try {
      process.env.REACT_APP_API_URL = "https://api.example.test";
      process.env.REACT_APP_API_PORT = "8443";
      process.env.REACT_APP_API_PATH = "/douyin";

      expect(buildChatSocketURL("jwt token")).toBe(
        "wss://api.example.test:8443/douyin/message/ws?token=jwt+token"
      );
    } finally {
      ["REACT_APP_API_URL", "REACT_APP_API_PORT", "REACT_APP_API_PATH"].forEach((key, index) => {
        if (original[["url", "port", "path"][index]] === undefined) delete process.env[key];
        else process.env[key] = original[["url", "port", "path"][index]];
      });
    }
  });

  test("merges real-time and history messages without duplication", () => {
    const pushed = {
      event_id: "event-1",
      id: 0,
      from_user_id: 2,
      to_user_id: 1,
      create_time: 100,
      content: "hello",
    };
    const persisted = { ...pushed, event_id: undefined, id: 19 };
    const older = { ...pushed, event_id: "event-0", create_time: 90, content: "first" };

    expect(mergeChatMessages([pushed], [persisted, older])).toEqual([older, pushed]);
  });

  test("deduplicates HTTP fallback messages after they appear in history", () => {
    const fallback = {
      event_id: "fallback-1000",
      id: 0,
      from_user_id: 1,
      to_user_id: 2,
      create_time: 1000,
      content: "sent while reconnecting",
    };
    const persisted = { ...fallback, event_id: undefined, id: 28, create_time: 1200 };

    expect(mergeChatMessages([fallback], [persisted])).toEqual([fallback]);
  });
});
