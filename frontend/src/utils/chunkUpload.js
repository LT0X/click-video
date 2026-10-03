import axios from "axios";

export const UPLOAD_CHUNK_SIZE = 5 * 1024 * 1024;
const MAX_PARALLEL_UPLOADS = 3;
const MAX_CHUNK_RETRIES = 3;
const RETRY_BASE_DELAY_MS = 250;

const apiBaseURL = `${process.env.REACT_APP_API_URL}:${process.env.REACT_APP_API_PORT}`.replace(/\/+$/, "");

export async function calculateMD5InWorker(file, onProgress, workerFactory) {
  const createWorker = workerFactory || (await import("./uploadHashWorkerFactory")).default;
  return new Promise((resolve, reject) => {
    let worker;
    try {
      worker = createWorker();
    } catch (error) {
      reject(error);
      return;
    }
    worker.onmessage = (event) => {
      const data = event.data || {};
      if (data.type === "progress" && onProgress) onProgress(data.percent);
      if (data.type === "complete") {
        worker.terminate();
        resolve(data.md5);
      }
      if (data.type === "error") {
        worker.terminate();
        reject(new Error(data.message || "文件 MD5 计算失败"));
      }
    };
    worker.onerror = (error) => {
      worker.terminate();
      reject(error instanceof Error ? error : new Error("文件 MD5 Worker 运行失败"));
    };
    worker.postMessage({ file });
  });
}

function getLocalStorage() {
  try {
    return typeof window === "undefined" ? null : window.localStorage;
  } catch (_) {
    return null;
  }
}

function uploadStorageKey(file, md5, metadata) {
  return `click-video:upload:${md5}:${file.size}:${encodeURIComponent(file.name)}:${encodeURIComponent(metadata.title || "")}:${encodeURIComponent(metadata.topic || "")}`;
}

function readUploadID(storage, key) {
  try {
    return storage?.getItem(key) || "";
  } catch (_) {
    return "";
  }
}

function saveUploadID(storage, key, uploadID) {
  try {
    storage?.setItem(key, uploadID);
  } catch (_) {
    // 浏览器禁用本地存储时仍可上传，只是刷新页面后无法自动恢复任务。
  }
}

function clearUploadID(storage, key) {
  try {
    storage?.removeItem(key);
  } catch (_) {
    // 清理本地续传记录失败不影响服务端上传结果。
  }
}

function requireSuccess(response) {
  const data = response?.data ?? response;
  if (!data || data.status_code !== 0) {
    throw new Error(data?.status_msg || "上传请求失败");
  }
  return data;
}

function sleep(milliseconds) {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}

async function post(httpClient, path, body, token) {
  const endpoint = path.split("?")[0];
  const response = await httpClient.post(`${apiBaseURL}/api/upload/${path}`, body, {
    headers: {
      token,
      ...(endpoint === "chunk" ? { "Content-Type": "application/octet-stream" } : {}),
    },
    timeout: endpoint === "chunk" || endpoint === "merge" ? 0 : 30000,
  });
  return requireSuccess(response);
}

async function uploadChunkWithRetry({ httpClient, uploadID, partNumber, blob, token, sleepFn }) {
  const path = `chunk?upload_id=${encodeURIComponent(uploadID)}&part_number=${partNumber}&size=${blob.size}`;
  for (let attempt = 0; ; attempt += 1) {
    try {
      return await post(httpClient, path, blob, token);
    } catch (error) {
      if (attempt >= MAX_CHUNK_RETRIES) throw error;
      await sleepFn(RETRY_BASE_DELAY_MS * 2 ** attempt);
    }
  }
}

export async function uploadFileInChunks(file, metadata, token, onProgress, dependencies = {}) {
  if (!file || !Number.isSafeInteger(file.size) || file.size <= 0) {
    throw new Error("请选择有效的视频文件");
  }

  const httpClient = dependencies.httpClient || axios;
  const storage = dependencies.storage === undefined ? getLocalStorage() : dependencies.storage;
  const chunkSize = dependencies.chunkSize || UPLOAD_CHUNK_SIZE;
  const sleepFn = dependencies.sleep || sleep;
  const computeMD5 = dependencies.computeMD5 || ((source, progress) =>
    calculateMD5InWorker(source, progress, dependencies.workerFactory));
  const md5 = await computeMD5(file, (percent) => onProgress?.({ phase: "hashing", percent }));
  const totalParts = Math.ceil(file.size / chunkSize);
  const key = uploadStorageKey(file, md5, metadata);
  let uploadID = readUploadID(storage, key);
  const initRequest = {
    upload_id: uploadID || undefined,
    file_name: file.name,
    file_size: file.size,
    file_md5: md5,
    total_parts: totalParts,
    title: metadata.title,
    topic: metadata.topic,
  };

  let init;
  try {
    init = await post(httpClient, "init", initRequest, token);
  } catch (error) {
    if (!uploadID || error.message !== "上传任务不存在或已过期") throw error;
    clearUploadID(storage, key);
    uploadID = "";
    initRequest.upload_id = undefined;
    init = await post(httpClient, "init", initRequest, token);
  }

  if (init.already_uploaded) {
    clearUploadID(storage, key);
    onProgress?.({ phase: "complete", percent: 100, alreadyUploaded: true });
    return init;
  }
  if (!init.upload_id) throw new Error("上传初始化未返回上传 ID");
  uploadID = init.upload_id;
  saveUploadID(storage, key, uploadID);

  const uploaded = new Set((init.uploaded_parts || []).map(Number).filter((part) => part >= 1 && part <= totalParts));
  const pending = [];
  for (let partNumber = 1; partNumber <= totalParts; partNumber += 1) {
    if (!uploaded.has(partNumber)) pending.push(partNumber);
  }

  let completed = uploaded.size;
  onProgress?.({ phase: "uploading", percent: Math.round((completed / totalParts) * 100), uploadedParts: completed, totalParts });
  let nextIndex = 0;
  const uploadWorker = async () => {
    while (nextIndex < pending.length) {
      const partNumber = pending[nextIndex];
      nextIndex += 1;
      const start = (partNumber - 1) * chunkSize;
      const blob = file.slice(start, Math.min(start + chunkSize, file.size));
      await uploadChunkWithRetry({ httpClient, uploadID, partNumber, blob, token, sleepFn });
      completed += 1;
      onProgress?.({ phase: "uploading", percent: Math.round((completed / totalParts) * 100), uploadedParts: completed, totalParts });
    }
  };

  await Promise.all(Array.from({ length: Math.min(MAX_PARALLEL_UPLOADS, pending.length) }, uploadWorker));
  onProgress?.({ phase: "merging", percent: 100, uploadedParts: completed, totalParts });
  const result = await post(httpClient, "merge", { upload_id: uploadID, file_md5: md5 }, token);
  clearUploadID(storage, key);
  onProgress?.({ phase: "complete", percent: 100, videoID: result.video_id });
  return result;
}
