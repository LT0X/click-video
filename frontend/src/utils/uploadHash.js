export const HASH_READ_SIZE = 2 * 1024 * 1024;

export async function calculateFileMD5(file, SparkMD5, onProgress) {
  const hasher = new SparkMD5.ArrayBuffer();
  const totalChunks = Math.ceil(file.size / HASH_READ_SIZE);

  for (let offset = 0; offset < file.size; offset += HASH_READ_SIZE) {
    const end = Math.min(offset + HASH_READ_SIZE, file.size);
    const buffer = await file.slice(offset, end).arrayBuffer();
    hasher.append(buffer);
    if (onProgress) {
      onProgress(Math.round(((offset / HASH_READ_SIZE + 1) / totalChunks) * 100));
    }
  }

  return hasher.end();
}
