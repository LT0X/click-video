/**
 * @file 上传弹出框组件
 * @module UploadPopover
 */
import styles from "../assets/styles/UploadPopover.module.scss";
import { message, Button, Progress, Input } from "antd";
import { useSelector } from "react-redux";
import { UploadOutlined } from "@ant-design/icons";
import React, { useRef, useState } from "react";
import { AiFillCloseCircle } from "react-icons/ai";
import { hideUpload } from "../redux/actions/popoverAction";
import { useDispatch } from "react-redux";
import { uploadFileInChunks } from "../utils/chunkUpload";

function UploadPopover() {
  const dispatch = useDispatch();
  const { TextArea } = Input;
  const [title, setTitle] = useState("默认标题");
  const [topic, setTopic] = useState("其它");
  const [uploadProgress, setUploadProgress] = useState(null);
  const [uploading, setUploading] = useState(false);
  const fileInput = useRef(null);
  const token = useSelector((state) => state?.loginRegister?.token);

  const handleFile = async (file) => {
    if (!file) return;
    const isMp4 = file.type === "video/mp4" || /\.mp4$/i.test(file.name || "");
    if (!isMp4) {
      message.error("您只能上传 MP4 格式的视频文件!");
      return;
    }
    if (uploading) return;

    setUploading(true);
    setUploadProgress({ phase: "hashing", percent: 0 });
    message.loading({ content: "正在校验并上传视频...", key: "upload", duration: 0 });
    try {
      await uploadFileInChunks(file, { title, topic }, token, setUploadProgress);
      message.destroy("upload");
      message.success("视频上传成功");
      setUploadProgress({ phase: "complete", percent: 100 });
      dispatch(hideUpload());
    } catch (error) {
      message.destroy("upload");
      message.error(error?.message || "视频上传失败，可重新选择文件后续传");
      setUploadProgress((progress) => ({ ...progress, phase: "error" }));
    } finally {
      setUploading(false);
      if (fileInput.current) fileInput.current.value = "";
    }
  };

  const visibleProgress = uploadProgress && uploadProgress.phase !== "complete";

  return (
    <div className={styles.uploadContainer}>
      <div className={styles.title}>
        <TextArea
          showCount
          maxLength={30}
          style={{ height: 120, resize: "none" }}
          value={title}
          onChange={(event) => setTitle(event.target.value)}
          placeholder="请输入视频标题，不超过30字，只能上传MP4格式的文件"
          disabled={uploading}
        />
      </div>
      <div className={styles.classify}>
        <div className={styles.classifyTitle}>
          <span>选择分类：</span>
        </div>
        <div className={styles.classifyContent}>
          <input type="radio" name="classify" value="体育" onChange={(event) => setTopic(event.target.value)} disabled={uploading} />
          体育
          <input type="radio" name="classify" value="游戏" onChange={(event) => setTopic(event.target.value)} disabled={uploading} />
          游戏
          <input type="radio" name="classify" value="音乐" onChange={(event) => setTopic(event.target.value)} disabled={uploading} />
          音乐
          <input type="radio" name="classify" value="音乐" onChange={(event) => setTopic(event.target.value)} disabled={uploading} />
          其它
        </div>
      </div>
      {visibleProgress && (
        <Progress
          percent={uploadProgress.percent || 0}
          status={uploadProgress.phase === "error" ? "exception" : "active"}
          format={() => uploadProgress.phase === "hashing" ? `MD5 ${uploadProgress.percent || 0}%` : `上传 ${uploadProgress.uploadedParts || 0}/${uploadProgress.totalParts || 0}`}
        />
      )}
      <div className={styles.uploadButton}>
        <input
          ref={fileInput}
          type="file"
          accept="video/mp4,.mp4"
          hidden
          onChange={(event) => handleFile(event.target.files?.[0])}
        />
        <Button
          icon={<UploadOutlined />}
          loading={uploading}
          onClick={() => fileInput.current?.click()}
        >
          {uploading ? "分片上传中..." : "选择视频并上传"}
        </Button>
      </div>
      <AiFillCloseCircle
        className={styles.close}
        onClick={() => !uploading && dispatch(hideUpload())}
      />
    </div>
  );
}

export default UploadPopover;
