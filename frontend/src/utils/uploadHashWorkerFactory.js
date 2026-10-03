export default function createUploadHashWorker() {
  return new Worker(new URL("../workers/uploadHash.worker.js", import.meta.url), { type: "module" });
}
