import React, { useState } from 'react';
import {
  Box,
  Paper,
  Typography,
  Grid,
  Card,
  CardContent,
  Chip,
  Tab,
  Tabs,
  LinearProgress,
  Alert,
  IconButton,
  Button,
} from '@mui/material';
import {
  TrendingUp,
  TrendingDown,
  Timeline,
  BarChart,
  Info,
  Download,
  Refresh,
} from '@mui/icons-material';
import { useQuery, useQueryClient } from 'react-query';

import { useParams } from 'react-router-dom';
import axios from 'axios';
import {
  LineChart,
  Line,
  BarChart as RechartsBarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
  AreaChart,
  Area,
} from 'recharts';
import toast from 'react-hot-toast';
import { stockAPI } from '../services/api';

const API_BASE_URL = process.env.REACT_APP_API_URL || '/api/v1';

function StockDetail() {
  const { symbol } = useParams();
  const [tabValue, setTabValue] = useState(0);
  const [timeRange, setTimeRange] = useState('1m');
  const queryClient = useQueryClient();
  // Fetch stock details
  const { data: stock, isLoading: stockLoading, error: stockError } = useQuery(
    ['stock', symbol],
    () => stockAPI.getStock(symbol).then(res => res.data),
    { refetchInterval: 30000 }
  );

  // Fetch stock prices
  const { data: prices, isLoading: pricesLoading } = useQuery(
    ['prices', symbol, timeRange],
    async () => {
      const endDate = new Date();
      const startDate = new Date();
      
      switch (timeRange) {
        case '1w':
          startDate.setDate(endDate.getDate() - 7);
          break;
        case '1m':
          startDate.setMonth(endDate.getMonth() - 1);
          break;
        case '3m':
          startDate.setMonth(endDate.getMonth() - 3);
          break;
        case '6m':
          startDate.setMonth(endDate.getMonth() - 6);
          break;
        case '1y':
          startDate.setFullYear(endDate.getFullYear() - 1);
          break;
        default:
          startDate.setMonth(endDate.getMonth() - 1);
      }

      const response = await stockAPI.getStockPrices(symbol, {
        start_date: startDate.toISOString().split('T')[0],
        end_date: endDate.toISOString().split('T')[0],
        limit: 100
      });
      return response.data;
    }
  );

  const handleTabChange = (event, newValue) => {
    setTabValue(newValue);
  };

  const handleRefresh = () => {
    toast.success('Refreshing stock data...');
    // Refetch queries
    queryClient.invalidateQueries(['stock', symbol]);
    queryClient.invalidateQueries(['prices', symbol, timeRange]);
  };

  if (stockLoading) {
    return (
      <Box sx={{ width: '100%' }}>
        <LinearProgress />
      </Box>
    );
  }

  if (stockError) {
    return (
      <Alert severity="error" sx={{ mt: 2 }}>
        Failed to load stock data: {stockError.message}
      </Alert>
    );
  }

  // Prepare chart data
  const priceData = prices?.map(item => ({
    date: new Date(item.date).toLocaleDateString(),
    price: item.close,
    open: item.open,
    high: item.high,
    low: item.low,
    volume: item.volume / 1000000, // Convert to millions
  })) || [];

  // Calculate statistics
  const latestPrice = priceData[priceData.length - 1]?.price || 0;
  const firstPrice = priceData[0]?.price || 0;
  const priceChange = latestPrice - firstPrice;
  const priceChangePercent = firstPrice ? (priceChange / firstPrice) * 100 : 0;

  const avgVolume = priceData.reduce((sum, item) => sum + (item.volume || 0), 0) / priceData.length;
  const maxPrice = Math.max(...priceData.map(item => item.price));
  const minPrice = Math.min(...priceData.map(item => item.price));

  return (
    <Box>
      {/* Header */}
      <Paper sx={{ p: 3, mb: 3 }}>
        <Box display="flex" justifyContent="space-between" alignItems="flex-start" flexWrap="wrap" gap={2}>
          <Box>
            <Box display="flex" alignItems="center" gap={2} mb={1}>
              <Typography variant="h4">
                {symbol}
              </Typography>
              <Chip
                label={stock?.exchange || 'N/A'}
                color="primary"
                variant="outlined"
              />
              <Chip
                icon={priceChange >= 0 ? <TrendingUp /> : <TrendingDown />}
                label={`${priceChange >= 0 ? '+' : ''}${priceChangePercent.toFixed(2)}%`}
                color={priceChange >= 0 ? 'success' : 'error'}
                variant="filled"
              />
            </Box>
            <Typography variant="h6" color="textSecondary" gutterBottom>
              {stock?.name || 'Loading...'}
            </Typography>
            <Box display="flex" gap={2} flexWrap="wrap">
              <Chip label={`Sector: ${stock?.sector || 'N/A'}`} size="small" />
              <Chip label={`Industry: ${stock?.industry || 'N/A'}`} size="small" />
            </Box>
          </Box>
          <Box display="flex" gap={1}>
            <IconButton onClick={handleRefresh} color="primary">
              <Refresh />
            </IconButton>
            <Button
              variant="outlined"
              startIcon={<Download />}
              onClick={() => toast.success('Export feature coming soon!')}
            >
              Export Data
            </Button>
          </Box>
        </Box>
      </Paper>

      {/* Price Summary */}
      <Grid container spacing={3} sx={{ mb: 3 }}>
        <Grid item xs={12} sm={6} md={3}>
          <Card>
            <CardContent>
              <Typography color="textSecondary" gutterBottom variant="overline">
                Current Price
              </Typography>
              <Typography variant="h4">
                ${latestPrice.toFixed(2)}
              </Typography>
              <Typography
                variant="body2"
                color={priceChange >= 0 ? 'success.main' : 'error.main'}
              >
                {priceChange >= 0 ? '+' : ''}{priceChange.toFixed(2)} ({priceChangePercent.toFixed(2)}%)
              </Typography>
            </CardContent>
          </Card>
        </Grid>
        <Grid item xs={12} sm={6} md={3}>
          <Card>
            <CardContent>
              <Typography color="textSecondary" gutterBottom variant="overline">
                52-Week High
              </Typography>
              <Typography variant="h4">
                ${maxPrice.toFixed(2)}
              </Typography>
              <Typography variant="body2" color="textSecondary">
                {((latestPrice / maxPrice) * 100).toFixed(1)}% of high
              </Typography>
            </CardContent>
          </Card>
        </Grid>
        <Grid item xs={12} sm={6} md={3}>
          <Card>
            <CardContent>
              <Typography color="textSecondary" gutterBottom variant="overline">
                52-Week Low
              </Typography>
              <Typography variant="h4">
                ${minPrice.toFixed(2)}
              </Typography>
              <Typography variant="body2" color="textSecondary">
                {((latestPrice / minPrice) * 100).toFixed(1)}% of low
              </Typography>
            </CardContent>
          </Card>
        </Grid>
        <Grid item xs={12} sm={6} md={3}>
          <Card>
            <CardContent>
              <Typography color="textSecondary" gutterBottom variant="overline">
                Avg Volume (M)
              </Typography>
              <Typography variant="h4">
                {avgVolume.toFixed(2)}
              </Typography>
              <Typography variant="body2" color="textSecondary">
                {priceData.length} trading days
              </Typography>
            </CardContent>
          </Card>
        </Grid>
      </Grid>

      {/* Time Range Selector */}
      <Paper sx={{ p: 2, mb: 3 }}>
        <Box display="flex" justifyContent="space-between" alignItems="center">
          <Typography variant="h6">Price History</Typography>
          <Box>
            {['1w', '1m', '3m', '6m', '1y'].map((range) => (
              <Button
                key={range}
                variant={timeRange === range ? 'contained' : 'outlined'}
                size="small"
                onClick={() => setTimeRange(range)}
                sx={{ ml: 1 }}
              >
                {range}
              </Button>
            ))}
          </Box>
        </Box>
      </Paper>

      {/* Tabs */}
      <Paper sx={{ mb: 3 }}>
        <Tabs value={tabValue} onChange={handleTabChange}>
          <Tab icon={<Timeline />} label="Price Chart" />
          <Tab icon={<BarChart />} label="Volume" />
          <Tab icon={<Info />} label="Details" />
        </Tabs>
      </Paper>

      {/* Tab Content */}
      {tabValue === 0 && (
        <Paper sx={{ p: 2, height: 500 }}>
          {pricesLoading ? (
            <Box display="flex" justifyContent="center" alignItems="center" height="100%">
              <LinearProgress sx={{ width: '50%' }} />
            </Box>
          ) : (
            <ResponsiveContainer width="100%" height="100%">
              <AreaChart data={priceData}>
                <CartesianGrid strokeDasharray="3 3" stroke="#444" />
                <XAxis dataKey="date" stroke="#fff" />
                <YAxis stroke="#fff" domain={['dataMin', 'dataMax']} />
                <Tooltip
                  contentStyle={{ backgroundColor: '#132f4c', borderColor: '#2196f3' }}
                  formatter={(value) => [`$${value.toFixed(2)}`, 'Price']}
                />
                <Legend />
                <Area
                  type="monotone"
                  dataKey="price"
                  stroke="#2196f3"
                  fill="url(#colorPrice)"
                  fillOpacity={0.3}
                  strokeWidth={2}
                />
                <defs>
                  <linearGradient id="colorPrice" x1="0" y1="0" x2="0" y2="1">
                    <stop offset="5%" stopColor="#2196f3" stopOpacity={0.8} />
                    <stop offset="95%" stopColor="#2196f3" stopOpacity={0} />
                  </linearGradient>
                </defs>
              </AreaChart>
            </ResponsiveContainer>
          )}
        </Paper>
      )}

      {tabValue === 1 && (
        <Paper sx={{ p: 2, height: 500 }}>
          {pricesLoading ? (
            <Box display="flex" justifyContent="center" alignItems="center" height="100%">
              <LinearProgress sx={{ width: '50%' }} />
            </Box>
          ) : (
            <ResponsiveContainer width="100%" height="100%">
              <RechartsBarChart data={priceData}>
                <CartesianGrid strokeDasharray="3 3" stroke="#444" />
                <XAxis dataKey="date" stroke="#fff" />
                <YAxis stroke="#fff" label={{ value: 'Volume (M)', angle: -90, position: 'insideLeft' }} />
                <Tooltip
                  contentStyle={{ backgroundColor: '#132f4c', borderColor: '#2196f3' }}
                  formatter={(value) => [`${value}M`, 'Volume']}
                />
                <Legend />
                <Bar dataKey="volume" fill="#4caf50" radius={[4, 4, 0, 0]} />
              </RechartsBarChart>
            </ResponsiveContainer>
          )}
        </Paper>
      )}

      {tabValue === 2 && (
        <Grid container spacing={3}>
          <Grid item xs={12} md={6}>
            <Paper sx={{ p: 3 }}>
              <Typography variant="h6" gutterBottom>
                Company Information
              </Typography>
              <Box sx={{ mt: 2 }}>
                <Grid container spacing={2}>
                  <Grid item xs={6}>
                    <Typography variant="body2" color="textSecondary">
                      Symbol
                    </Typography>
                    <Typography variant="body1">
                      {symbol}
                    </Typography>
                  </Grid>
                  <Grid item xs={6}>
                    <Typography variant="body2" color="textSecondary">
                      Exchange
                    </Typography>
                    <Typography variant="body1">
                      {stock?.exchange || 'N/A'}
                    </Typography>
                  </Grid>
                  <Grid item xs={6}>
                    <Typography variant="body2" color="textSecondary">
                      Sector
                    </Typography>
                    <Typography variant="body1">
                      {stock?.sector || 'N/A'}
                    </Typography>
                  </Grid>
                  <Grid item xs={6}>
                    <Typography variant="body2" color="textSecondary">
                      Industry
                    </Typography>
                    <Typography variant="body1">
                      {stock?.industry || 'N/A'}
                    </Typography>
                  </Grid>
                </Grid>
              </Box>
            </Paper>
          </Grid>
          <Grid item xs={12} md={6}>
            <Paper sx={{ p: 3 }}>
              <Typography variant="h6" gutterBottom>
                Trading Statistics
              </Typography>
              <Box sx={{ mt: 2 }}>
                <Grid container spacing={2}>
                  <Grid item xs={6}>
                    <Typography variant="body2" color="textSecondary">
                      Current Price
                    </Typography>
                    <Typography variant="body1">
                      ${latestPrice.toFixed(2)}
                    </Typography>
                  </Grid>
                  <Grid item xs={6}>
                    <Typography variant="body2" color="textSecondary">
                      Daily Change
                    </Typography>
                    <Typography variant="body1" color={priceChange >= 0 ? 'success.main' : 'error.main'}>
                      {priceChange >= 0 ? '+' : ''}{priceChange.toFixed(2)}
                    </Typography>
                  </Grid>
                  <Grid item xs={6}>
                    <Typography variant="body2" color="textSecondary">
                      High (Period)
                    </Typography>
                    <Typography variant="body1">
                      ${maxPrice.toFixed(2)}
                    </Typography>
                  </Grid>
                  <Grid item xs={6}>
                    <Typography variant="body2" color="textSecondary">
                      Low (Period)
                    </Typography>
                    <Typography variant="body1">
                      ${minPrice.toFixed(2)}
                    </Typography>
                  </Grid>
                  <Grid item xs={6}>
                    <Typography variant="body2" color="textSecondary">
                      Avg Volume
                    </Typography>
                    <Typography variant="body1">
                      {avgVolume.toFixed(2)}M
                    </Typography>
                  </Grid>
                  <Grid item xs={6}>
                    <Typography variant="body2" color="textSecondary">
                      Data Points
                    </Typography>
                    <Typography variant="body1">
                      {priceData.length}
                    </Typography>
                  </Grid>
                </Grid>
              </Box>
            </Paper>
          </Grid>
        </Grid>
      )}

      {/* Recent Prices Table */}
      <Paper sx={{ p: 3, mt: 3 }}>
        <Typography variant="h6" gutterBottom>
          Recent Price History
        </Typography>
        {pricesLoading ? (
          <LinearProgress />
        ) : (
          <Box sx={{ overflowX: 'auto' }}>
            <table style={{ width: '100%', borderCollapse: 'collapse' }}>
              <thead>
                <tr style={{ borderBottom: '1px solid #444' }}>
                  <th style={{ textAlign: 'left', padding: '8px' }}>Date</th>
                  <th style={{ textAlign: 'right', padding: '8px' }}>Open</th>
                  <th style={{ textAlign: 'right', padding: '8px' }}>High</th>
                  <th style={{ textAlign: 'right', padding: '8px' }}>Low</th>
                  <th style={{ textAlign: 'right', padding: '8px' }}>Close</th>
                  <th style={{ textAlign: 'right', padding: '8px' }}>Volume</th>
                  <th style={{ textAlign: 'right', padding: '8px' }}>Change</th>
                </tr>
              </thead>
              <tbody>
                {priceData.slice(-10).reverse().map((item, index) => {
                  const prevPrice = priceData[priceData.length - index - 2]?.price;
                  const change = prevPrice ? item.price - prevPrice : 0;
                  const changePercent = prevPrice ? (change / prevPrice) * 100 : 0;

                  return (
                    <tr key={index} style={{ borderBottom: '1px solid #333' }}>
                      <td style={{ padding: '8px' }}>{item.date}</td>
                      <td style={{ textAlign: 'right', padding: '8px' }}>
                        ${item.open?.toFixed(2) || 'N/A'}
                      </td>
                      <td style={{ textAlign: 'right', padding: '8px' }}>
                        ${item.high?.toFixed(2) || 'N/A'}
                      </td>
                      <td style={{ textAlign: 'right', padding: '8px' }}>
                        ${item.low?.toFixed(2) || 'N/A'}
                      </td>
                      <td style={{ textAlign: 'right', padding: '8px' }}>
                        ${item.price?.toFixed(2) || 'N/A'}
                      </td>
                      <td style={{ textAlign: 'right', padding: '8px' }}>
                        {item.volume?.toFixed(2)}M
                      </td>
                      <td style={{ 
                        textAlign: 'right', 
                        padding: '8px',
                        color: change >= 0 ? '#4caf50' : '#f44336'
                      }}>
                        {change >= 0 ? '+' : ''}{changePercent.toFixed(2)}%
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </Box>
        )}
      </Paper>
    </Box>
  );
}

export default StockDetail;