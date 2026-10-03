/**
 * @file 消息弹窗组件
 * @module MessagePopover
 */
import styles from "../assets/styles/MessagePopover.module.scss";
import { useEffect, useState, useRef } from "react";
import { useSelector } from "react-redux";
import { getFriendList, getMessages, sendMessage } from "../utils/getMessage";
import { buildChatSocketURL, mergeChatMessages } from "../utils/chatSocket";
import { BsSendFill } from "react-icons/bs";
import { AiFillCloseCircle } from "react-icons/ai";
import transfromTime from "../utils/transformTime";
import { useDispatch } from "react-redux";
import {
  appendMessage,
  changeMessages,
  changeFriendList,
  changeChattingFriendId,
} from "../redux/actions/personalAction";
import { message } from "antd";
function MessagePopover({ handleMessage }) {
  const dispatch = useDispatch();
  const token = useSelector((state) => state?.loginRegister?.token); //获取登录状态
  const user_id = useSelector((state) => state?.loginRegister?.user_id); //获取用户id
  const messages = useSelector((state) => state?.personal?.messages);
  const friendListArr = useSelector((state) => Object.values(state?.personal?.friendList));
  const friendList = useSelector((state) => state?.personal?.friendList);
  const info = useSelector((state) => state?.personal?.info);
  const chattingFriendId = useSelector((state) => state?.personal?.chattingFriendId);
  const [inputValue, setInputValue] = useState("");
  const scrollRef = useRef(null);
  const messageEndRef = useRef(null);
  const socketRef = useRef(null);
  const messagesRef = useRef(messages);
  const chattingFriendIdRef = useRef(chattingFriendId);
  messagesRef.current = messages;
  chattingFriendIdRef.current = chattingFriendId;

  function handleSendMessage() {
    const content = inputValue.trim();
    if (!content || !chattingFriendId) return;
    const socket = socketRef.current;
    if (socket?.readyState === WebSocket.OPEN) {
      socket.send(JSON.stringify({ type: "message", to_user_id: chattingFriendId, content }));
      setInputValue("");
      return;
    }
    sendMessage(token, chattingFriendId, content)
      .then(() => {
        dispatch(appendMessage(chattingFriendId, {
          event_id: `fallback-${Date.now()}`,
          id: 0,
          from_user_id: user_id,
          to_user_id: chattingFriendId,
          create_time: Date.now(),
          content,
        }));
        setInputValue("");
      })
  }

  useEffect(() => {
    if (messageEndRef.current) {
      messageEndRef.current.scrollIntoView({ behavior: "smooth" });
    }
  }, [chattingFriendId]);

  useEffect(() => {
    if (scrollRef.current && messageEndRef.current) {
      const isNearBottom =
        scrollRef.current.scrollTop + scrollRef.current.clientHeight >=
        scrollRef.current.scrollHeight - 50;
      if (isNearBottom) messageEndRef.current.scrollIntoView({ behavior: "smooth" });
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [messages[chattingFriendId]]);

  useEffect(() => {
    if (!token || !user_id) return;
    const refreshFriendList = () => {
      getFriendList(user_id, token)
        .then((res) => {
          const friendMap = (res.user_list || []).reduce((result, user) => {
            result[user.id] = user;
            return result;
          }, {});
          dispatch(changeFriendList(friendMap));
          if (!chattingFriendId && res.user_list?.length) {
            dispatch(changeChattingFriendId(res.user_list[0].id));
          }
        });
    };
    refreshFriendList();
    const intervalId = setInterval(refreshFriendList, 10000);
    return () => clearInterval(intervalId);
  }, [chattingFriendId, dispatch, token, user_id]);

  useEffect(() => {
    if (!token || !chattingFriendId) return;
    getMessages(token, chattingFriendId).then((res) => {
      if (res?.message_list) {
        const current = messagesRef.current[chattingFriendId] || [];
        dispatch(changeMessages(chattingFriendId, mergeChatMessages(res.message_list, current)));
      }
    });
  }, [chattingFriendId, dispatch, token]);

  useEffect(() => {
    if (!token || !user_id) return;
    let disposed = false;
    let socket;
    let heartbeat;
    let reconnectTimer;
    let reconnectAttempt = 0;

    const connect = () => {
      if (disposed) return;
      socket = new WebSocket(buildChatSocketURL(token));
      socketRef.current = socket;
      socket.onopen = () => {
        reconnectAttempt = 0;
        const activeFriendID = chattingFriendIdRef.current;
        if (activeFriendID) {
          getMessages(token, activeFriendID).then((res) => {
            if (res?.message_list) {
              dispatch(changeMessages(activeFriendID, mergeChatMessages(res.message_list, messagesRef.current[activeFriendID] || [])));
            }
          });
        }
        heartbeat = setInterval(() => {
          if (socket.readyState === WebSocket.OPEN) socket.send("ping");
        }, 10000);
      };
      socket.onmessage = (event) => {
        let incoming;
        try {
          incoming = JSON.parse(event.data);
        } catch (error) {
          return;
        }
        if (incoming.type === "message") {
          dispatch(appendMessage(incoming.from_user_id, incoming));
        } else if (incoming.type === "accepted") {
          dispatch(appendMessage(incoming.to_user_id, incoming));
        } else if (incoming.type === "error") {
          message.error(incoming.message || "消息发送失败");
        }
      };
      socket.onclose = () => {
        clearInterval(heartbeat);
        if (socketRef.current === socket) socketRef.current = null;
        if (disposed) return;
        const delay = Math.min(1000 * (2 ** reconnectAttempt), 10000);
        reconnectAttempt += 1;
        reconnectTimer = setTimeout(connect, delay);
      };
      socket.onerror = () => socket.close();
    };

    connect();
    return () => {
      disposed = true;
      clearInterval(heartbeat);
      clearTimeout(reconnectTimer);
      if (socketRef.current === socket) socketRef.current = null;
      socket?.close();
    };
  }, [dispatch, token, user_id]);

  return (
    <div className={styles.messageContainer} onWheel={(e) => e.stopPropagation()}>
      <div className={styles.left}>
        {friendListArr.map((item) => {
          return (
            <div
              key={item.id}
              className={`${styles.person} ${item.id === chattingFriendId && styles.selected}`}
              onClick={() => dispatch(changeChattingFriendId(item.id))}>
              <div>
                <div
                  className={styles.avatar}
                  style={{
                    backgroundImage: `url(${item?.avatar})`,
                    backgroundSize: "cover",
                  }}></div>
              </div>
              <div className={styles.info}>
                <div className={styles.name}>{item?.name}</div>
                <div className={styles.lastMessage}>
                  {item?.message?.length > 7 ? item?.message?.slice(0, 7) + "..." : item?.message}
                </div>
              </div>
            </div>
          );
        })}
      </div>
      <div className={styles.rightContainer}>
        <div className={styles.right} ref={scrollRef}>
          <div className={styles.messageArea}>
            {messages[chattingFriendId] &&
              messages[chattingFriendId]?.map((item, index) => {
                return (
                  <div key={index} className={styles.message}>
                    <div className={styles.time}>{transfromTime(item?.create_time)}</div>
                    {user_id !== item?.from_user_id ? (
                      <div className={styles.single}>
                        <div
                          className={styles.avatarSmall}
                          style={{
                            backgroundImage: `url(${friendList[chattingFriendId]?.avatar})`,
                            backgroundSize: "cover",
                          }}></div>
                        <div className={styles.content}>{item?.content}</div>
                      </div>
                    ) : (
                      <div className={styles.singleReverse}>
                        <div className={styles.content}>{item?.content}</div>
                        <div
                          className={styles.avatarSmall}
                          style={{
                            backgroundImage: `url(${info?.avatar})`,
                            backgroundSize: "cover",
                          }}></div>
                      </div>
                    )}
                  </div>
                );
              })}
            <div ref={messageEndRef} style={{ width: "100%", height: "50px" }}></div>
          </div>
        </div>
        <input
          className={styles.input}
          placeholder="输入消息和好友聊天"
          value={inputValue}
          onChange={(e) => setInputValue(e.target.value)}
          onKeyDown={(e) => e.key === "Enter" && handleSendMessage()}></input>
        <BsSendFill className={styles.icon} onClick={handleSendMessage}></BsSendFill>
        <AiFillCloseCircle className={styles.close} onClick={handleMessage}></AiFillCloseCircle>
      </div>
    </div>
  );
}
export default MessagePopover;
