import { uploadFileInChunks } from "./chunkUpload";

jest.mock("axios", () => ({ __esModule: true, default: { post: jest.fn() } }));

const makeStorage = () => {
  const values = new Map();
  return {
    getItem: (key) => values.get(key) || null,
    setItem: (key, value) => values.set(key, value),
    removeItem: (key) => values.delete(key),
  };
};

const makeFile = (size = 11) => ({
  name: "clip.mp4",
  size,
  type: "video/mp4",
  slice: (start, end) => ({ start, end, size: end - start }),
});

const success = (data) => ({ data: { status_code: 0, ...data } });

function makeClient({ uploadedParts = [], onChunk } = {}) {
  const post = jest.fn(async (url, body, options) => {
    if (url.endsWith("/upload/init")) {
      return success({ upload_id: "session-1", uploaded_parts: uploadedParts });
    }
    if (url.includes("/upload/chunk?")) {
      return onChunk ? onChunk(url, body, options) : success({});
    }
    if (url.endsWith("/upload/merge")) return success({ video_id: 42 });
    throw new Error(`unexpected URL: ${url}`);
  });
  return { post };
}

const metadata = { title: "title", topic: "music" };

test("只上传 init 返回中未完成的分片，然后合并", async () => {
  const client = makeClient({ uploadedParts: [2] });
  const result = await uploadFileInChunks(makeFile(), metadata, "token", undefined, {
    httpClient: client,
    computeMD5: async () => "0123456789abcdef0123456789abcdef",
    storage: makeStorage(),
    chunkSize: 4,
    sleep: async () => {},
  });

  const chunkCalls = client.post.mock.calls.filter(([url]) => url.includes("/upload/chunk?"));
  expect(chunkCalls.map(([url]) => Number(new URLSearchParams(url.split("?")[1]).get("part_number")))).toEqual([1, 3]);
  expect(chunkCalls[0][1]).toMatchObject({ start: 0, end: 4 });
  expect(chunkCalls[0][2].headers).toMatchObject({ token: "token", "Content-Type": "application/octet-stream" });
  expect(result.video_id).toBe(42);
});

test("失败分片按指数间隔重试并最终成功", async () => {
  let attempts = 0;
  const delays = [];
  const client = makeClient({
    onChunk: async () => {
      attempts += 1;
      if (attempts < 3) throw new Error("temporary failure");
      return success({});
    },
  });

  await uploadFileInChunks(makeFile(3), metadata, "token", undefined, {
    httpClient: client,
    computeMD5: async () => "0123456789abcdef0123456789abcdef",
    storage: makeStorage(),
    chunkSize: 4,
    sleep: async (delay) => delays.push(delay),
  });

  expect(attempts).toBe(3);
  expect(delays).toEqual([250, 500]);
});

test("标题或分类变更时不复用不匹配的续传任务", async () => {
  const storage = makeStorage();
  storage.setItem("click-video:upload:0123456789abcdef0123456789abcdef:11:clip.mp4", "stale-session");
  const client = makeClient();

  await uploadFileInChunks(makeFile(), { title: "changed title", topic: "music" }, "token", undefined, {
    httpClient: client,
    computeMD5: async () => "0123456789abcdef0123456789abcdef",
    storage,
    chunkSize: 4,
    sleep: async () => {},
  });

  expect(client.post.mock.calls[0][1].upload_id).toBeUndefined();
});

test("浏览器禁用 localStorage 时仍可完成本次上传", async () => {
  const client = makeClient();
  const storage = {
    getItem: () => null,
    setItem: () => { throw new Error("storage disabled"); },
    removeItem: () => { throw new Error("storage disabled"); },
  };

  await expect(uploadFileInChunks(makeFile(3), metadata, "token", undefined, {
    httpClient: client,
    computeMD5: async () => "0123456789abcdef0123456789abcdef",
    storage,
    chunkSize: 4,
    sleep: async () => {},
  })).resolves.toMatchObject({ video_id: 42 });
});

test("续传初始化遇到网络错误时保留 upload ID 并向上返回错误", async () => {
  const md5 = "0123456789abcdef0123456789abcdef";
  const storage = makeStorage();
  storage.setItem(`click-video:upload:${md5}:11:clip.mp4:title:music`, "session-1");
  const client = { post: jest.fn().mockRejectedValue(new Error("network unreachable")) };

  await expect(uploadFileInChunks(makeFile(), metadata, "token", undefined, {
    httpClient: client,
    computeMD5: async () => md5,
    storage,
    chunkSize: 4,
    sleep: async () => {},
  })).rejects.toThrow("network unreachable");

  expect(client.post).toHaveBeenCalledTimes(1);
  expect(storage.getItem(`click-video:upload:${md5}:11:clip.mp4:title:music`)).toBe("session-1");
});

test("同时上传的分片不超过三路", async () => {
  let active = 0;
  let peak = 0;
  const client = makeClient({
    onChunk: async () => {
      active += 1;
      peak = Math.max(peak, active);
      await new Promise((resolve) => setTimeout(resolve, 5));
      active -= 1;
      return success({});
    },
  });

  await uploadFileInChunks(makeFile(29), metadata, "token", undefined, {
    httpClient: client,
    computeMD5: async () => "0123456789abcdef0123456789abcdef",
    storage: makeStorage(),
    chunkSize: 4,
    sleep: async () => {},
  });

  expect(peak).toBe(3);
});
