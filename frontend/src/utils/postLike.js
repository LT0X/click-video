import api from "./request";

async function postLike(video_id, token = "") {
    return api.post(`/favorite/action/?video_id=${video_id}&action_type=1`, null, { returnFullResponse: true });
}

async function postCancelLike(video_id, token = "") {
    return api.post(`/favorite/action/?video_id=${video_id}&action_type=2`, null, { returnFullResponse: true });
}
export { postLike, postCancelLike };
