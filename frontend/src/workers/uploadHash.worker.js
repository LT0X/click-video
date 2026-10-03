import SparkMD5 from "spark-md5";
import { calculateFileMD5 } from "../utils/uploadHash";

/* global globalThis */
const workerScope = globalThis;

workerScope.onmessage = async (event) => {
  try {
    const md5 = await calculateFileMD5(event.data.file, SparkMD5, (percent) => {
      workerScope.postMessage({ type: "progress", percent });
    });
    workerScope.postMessage({ type: "complete", md5 });
  } catch (error) {
    workerScope.postMessage({ type: "error", message: error?.message || "文件 MD5 计算失败" });
  }
};
