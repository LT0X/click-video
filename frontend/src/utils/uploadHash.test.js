import SparkMD5 from "spark-md5";
import { calculateFileMD5 } from "./uploadHash";

test("MD5 通过 2MiB File.slice 分块计算，不读取整个文件", async () => {
  const bytes = new Uint8Array(2 * 1024 * 1024 + 3);
  const slices = [];
  const file = {
    size: bytes.length,
    slice: (start, end) => {
      slices.push([start, end]);
      return { arrayBuffer: async () => bytes.slice(start, end).buffer };
    },
  };

  const digest = await calculateFileMD5(file, SparkMD5);

  expect(slices).toEqual([[0, 2 * 1024 * 1024], [2 * 1024 * 1024, bytes.length]]);
  expect(digest).toBe(new SparkMD5.ArrayBuffer().append(bytes.buffer).end());
});
