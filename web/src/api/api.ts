import axios from 'axios';

const api = axios.create({
  baseURL: '/api',
});

export const getMerges = async () => {
  const { data } = await api.get('/merges');
  return data;
};

export const getMergeDetails = async (id: number) => {
  const { data } = await api.get(`/merges/${id}`);
  return data;
};

export const getMergeUsers = async (id: number, params: any) => {
  const { data } = await api.get(`/merges/${id}/users`, { params });
  return data;
};

export const getUserInteractions = async (email: string, mergeId?: number) => {
  const { data } = await api.get(`/users/${email}/interactions`, { params: { merge_id: mergeId } });
  return data;
};

export const getUserRecommendations = async (email: string, mergeId?: number) => {
  const { data } = await api.get(`/users/${email}/recommendations`, { params: { merge_id: mergeId } });
  return data;
};
