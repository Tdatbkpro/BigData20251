import axios from 'axios';

const API_BASE_URL = process.env.REACT_APP_API_URL || '/api/v1';

const api = axios.create({
  baseURL: API_BASE_URL,
  timeout: 30000,
  headers: {
    'Content-Type': 'application/json',
  },
});

// Request interceptor
api.interceptors.request.use(
  (config) => {
    // Add auth token if available
    const token = localStorage.getItem('token');
    if (token) {
      config.headers.Authorization = `Bearer ${token}`;
    }
    return config;
  },
  (error) => {
    return Promise.reject(error);
  }
);

// Response interceptor
api.interceptors.response.use(
  (response) => response,
  (error) => {
    if (error.response?.status === 401) {
      // Handle unauthorized
      localStorage.removeItem('token');
      window.location.href = '/login';
    }
    return Promise.reject(error);
  }
);

// Stock APIs
export const stockAPI = {
  // Get all stocks
  getStocks: () => api.get('/stocks'),
  
  // Get stock by symbol
  getStock: (symbol) => api.get(`/stocks/${symbol}`),
  
  // Get stock prices
  getStockPrices: (symbol, params) => 
    api.get(`/stocks/${symbol}/prices`, { params }),
  
  // Get analytics summary
  getAnalyticsSummary: () => api.get('/analytics/summary'),
  
  // Get top gainers
  getTopGainers: (days = 7) => 
    api.get('/analytics/top-gainers', { params: { days } }),
  
  // Get high volume stocks
  getHighVolumeStocks: (days = 7) => 
    api.get('/analytics/high-volume', { params: { days } }),
};

// HDFS APIs
export const hdfsAPI = {
  // List HDFS files
  listFiles: (path = '/stock_data') => 
    api.get('/hdfs/files', { params: { path } }),
  
  // Read HDFS file
  readFile: (filePath, limit = 100) => 
    api.get('/hdfs/read', { params: { file_path: filePath, limit } }),
};

// Spark APIs
export const sparkAPI = {
  // Run batch job
  runBatchJob: () => api.get('/spark/jobs/run-batch'),
};

// System APIs
export const systemAPI = {
  // Health check
  healthCheck: () => api.get('/health'),
  
  // Get system metrics
  getMetrics: () => api.get('/metrics'),
};

export default api;