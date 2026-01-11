import React, { useState } from 'react';
import {
  Box,
  Paper,
  Typography,
  Grid,
  Card,
  CardContent,
  Tab,
  Tabs,
  LinearProgress,
  Alert,
  FormControl,
  InputLabel,
  Select,
  MenuItem,
  Button,
  Chip,
} from '@mui/material';
import {
  TrendingUp,
  TrendingDown,
  Equalizer,
  Timeline,
  BarChart,
  PieChart,
  TableChart,
  FilterList,
} from '@mui/icons-material';
import { useQuery } from 'react-query';
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
  PieChart as RechartsPieChart,
  Pie,
  Cell,
  ScatterChart,
  Scatter,
  ZAxis,
} from 'recharts';
import toast from 'react-hot-toast';
import { stockAPI } from '../services/api';

const API_BASE_URL = '/api/v1';

function Analytics() {
  const [tabValue, setTabValue] = useState(0);
  const [timeRange, setTimeRange] = useState('7');
  const [metric, setMetric] = useState('change');

  // Fetch analytics data
  const { data: summary, isLoading: summaryLoading, error: summaryError } = useQuery(
    'analyticsSummary',
    () => stockAPI.getAnalyticsSummary().then(res => res.data),
    { refetchInterval: 60000 }
  );

  const { data: gainers, isLoading: gainersLoading } = useQuery(
    ['topGainers', timeRange],
    () => stockAPI.getTopGainers(parseInt(timeRange)).then(res => res.data)
  );

  const { data: volumeStocks, isLoading: volumeLoading } = useQuery(
    ['highVolume', timeRange],
    () => stockAPI.getHighVolumeStocks(parseInt(timeRange)).then(res => res.data)
  );

  const handleTabChange = (event, newValue) => {
    setTabValue(newValue);
  };

  // Prepare chart data
  const gainersData = (gainers || []).slice(0, 10).map(stock => ({
    name: stock.symbol,
    value: Math.abs(stock.change),
    change: stock.change,
    volume: stock.volume / 1000000, // Convert to millions
    type: stock.change >= 0 ? 'Gainer' : 'Loser',
  }));

  const volumeData = (volumeStocks || []).slice(0, 10).map(stock => ({
    name: stock.symbol,
    volume: stock.avg_volume / 1000000, // Convert to millions
    company: stock.company_name,
    exchange: stock.exchange,
  }));

  const exchangeData = Object.entries(summary?.exchanges || {}).map(([name, count]) => ({
    name,
    value: count,
  }));

  // Scatter plot data (volume vs change)
  const scatterData = (gainers || []).slice(0, 20).map(stock => ({
    x: stock.change,
    y: stock.volume / 1000000,
    z: Math.abs(stock.change) * 10,
    name: stock.symbol,
  }));

  const COLORS = ['#0088FE', '#00C49F', '#FFBB28', '#FF8042', '#8884D8', '#82ca9d'];

  if (summaryLoading) {
    return (
      <Box sx={{ width: '100%' }}>
        <LinearProgress />
      </Box>
    );
  }

  if (summaryError) {
    return (
      <Alert severity="error" sx={{ mt: 2 }}>
        Failed to load analytics data: {summaryError.message}
      </Alert>
    );
  }

  return (
    <Box>
      {/* Header */}
      <Paper sx={{ p: 3, mb: 3 }}>
        <Box display="flex" justifyContent="space-between" alignItems="flex-start" flexWrap="wrap" gap={2}>
          <Box>
            <Typography variant="h4" gutterBottom>
              Market Analytics
            </Typography>
            <Typography variant="body1" color="textSecondary">
              Real-time analysis of stock market performance and trends
            </Typography>
          </Box>
          <Box display="flex" gap={2}>
            <FormControl size="small" sx={{ minWidth: 120 }}>
              <InputLabel>Time Range</InputLabel>
              <Select
                value={timeRange}
                label="Time Range"
                onChange={(e) => setTimeRange(e.target.value)}
              >
                <MenuItem value="1">1 Day</MenuItem>
                <MenuItem value="7">7 Days</MenuItem>
                <MenuItem value="30">30 Days</MenuItem>
                <MenuItem value="90">90 Days</MenuItem>
              </Select>
            </FormControl>
            <FormControl size="small" sx={{ minWidth: 120 }}>
              <InputLabel>Metric</InputLabel>
              <Select
                value={metric}
                label="Metric"
                onChange={(e) => setMetric(e.target.value)}
              >
                <MenuItem value="change">Price Change</MenuItem>
                <MenuItem value="volume">Volume</MenuItem>
                <MenuItem value="volatility">Volatility</MenuItem>
              </Select>
            </FormControl>
            <Button
              variant="outlined"
              startIcon={<FilterList />}
              onClick={() => toast.success('Advanced filters coming soon!')}
            >
              Filters
            </Button>
          </Box>
        </Box>
      </Paper>

      {/* Summary Cards */}
      <Grid container spacing={3} sx={{ mb: 3 }}>
        <Grid item xs={12} sm={6} md={3}>
          <Card>
            <CardContent>
              <Box display="flex" justifyContent="space-between" alignItems="center">
                <Box>
                  <Typography color="textSecondary" gutterBottom variant="overline">
                    Total Stocks
                  </Typography>
                  <Typography variant="h4">
                    {summary?.total_stocks || 0}
                  </Typography>
                  <Typography variant="body2" color="textSecondary">
                    Across {Object.keys(summary?.exchanges || {}).length} exchanges
                  </Typography>
                </Box>
                <Box
                  sx={{
                    backgroundColor: '#2196f320',
                    borderRadius: '12px',
                    p: 1.5,
                  }}
                >
                  <Equalizer sx={{ color: '#2196f3', fontSize: 28 }} />
                </Box>
              </Box>
            </CardContent>
          </Card>
        </Grid>
        <Grid item xs={12} sm={6} md={3}>
          <Card>
            <CardContent>
              <Box display="flex" justifyContent="space-between" alignItems="center">
                <Box>
                  <Typography color="textSecondary" gutterBottom variant="overline">
                    Total Records
                  </Typography>
                  <Typography variant="h4">
                    {(summary?.total_records || 0).toLocaleString()}
                  </Typography>
                  <Typography variant="body2" color="textSecondary">
                    Latest: {new Date(summary?.latest_update).toLocaleDateString()}
                  </Typography>
                </Box>
                <Box
                  sx={{
                    backgroundColor: '#4caf5020',
                    borderRadius: '12px',
                    p: 1.5,
                  }}
                >
                  <TableChart sx={{ color: '#4caf50', fontSize: 28 }} />
                </Box>
              </Box>
            </CardContent>
          </Card>
        </Grid>
        <Grid item xs={12} sm={6} md={3}>
          <Card>
            <CardContent>
              <Box display="flex" justifyContent="space-between" alignItems="center">
                <Box>
                  <Typography color="textSecondary" gutterBottom variant="overline">
                    Top Gainer
                  </Typography>
                  <Typography variant="h4">
                    {gainers?.[0]?.symbol || 'N/A'}
                  </Typography>
                  <Typography variant="body2" color="success.main">
                    +{(gainers?.[0]?.change || 0).toFixed(2)}%
                  </Typography>
                </Box>
                <Box
                  sx={{
                    backgroundColor: '#4caf5020',
                    borderRadius: '12px',
                    p: 1.5,
                  }}
                >
                  <TrendingUp sx={{ color: '#4caf50', fontSize: 28 }} />
                </Box>
              </Box>
            </CardContent>
          </Card>
        </Grid>
        <Grid item xs={12} sm={6} md={3}>
          <Card>
            <CardContent>
              <Box display="flex" justifyContent="space-between" alignItems="center">
                <Box>
                  <Typography color="textSecondary" gutterBottom variant="overline">
                    High Volume
                  </Typography>
                  <Typography variant="h4">
                    {volumeStocks?.[0]?.symbol || 'N/A'}
                  </Typography>
                  <Typography variant="body2" color="textSecondary">
                    {(volumeStocks?.[0]?.avg_volume / 1000000).toFixed(1)}M avg
                  </Typography>
                </Box>
                <Box
                  sx={{
                    backgroundColor: '#ff980020',
                    borderRadius: '12px',
                    p: 1.5,
                  }}
                >
                  <BarChart sx={{ color: '#ff9800', fontSize: 28 }} />
                </Box>
              </Box>
            </CardContent>
          </Card>
        </Grid>
      </Grid>

      {/* Tabs */}
      <Paper sx={{ mb: 3 }}>
        <Tabs value={tabValue} onChange={handleTabChange}>
          <Tab icon={<Timeline />} label="Performance" />
          <Tab icon={<BarChart />} label="Volume Analysis" />
          <Tab icon={<PieChart />} label="Market Distribution" />
          <Tab icon={<Equalizer />} label="Advanced Analytics" />
        </Tabs>
      </Paper>

      {/* Tab Content */}
      {tabValue === 0 && (
        <Grid container spacing={3}>
          <Grid item xs={12} md={8}>
            <Paper sx={{ p: 2, height: 400 }}>
              <Typography variant="h6" gutterBottom>
                Top Performers (Last {timeRange} Days)
              </Typography>
              {gainersLoading ? (
                <Box display="flex" justifyContent="center" alignItems="center" height="100%">
                  <LinearProgress sx={{ width: '50%' }} />
                </Box>
              ) : (
                <ResponsiveContainer width="100%" height="90%">
                  <RechartsBarChart data={gainersData}>
                    <CartesianGrid strokeDasharray="3 3" stroke="#444" />
                    <XAxis dataKey="name" stroke="#fff" />
                    <YAxis stroke="#fff" />
                    <Tooltip
                      contentStyle={{ backgroundColor: '#132f4c', borderColor: '#2196f3' }}
                      formatter={(value, name) => {
                        if (name === 'value') return [`${value}%`, 'Change'];
                        if (name === 'volume') return [`${value}M`, 'Volume'];
                        return [value, name];
                      }}
                    />
                    <Legend />
                    <Bar dataKey="value" fill="#8884d8" radius={[4, 4, 0, 0]}>
                      {gainersData.map((entry, index) => (
                        <Cell key={`cell-${index}`} fill={entry.change >= 0 ? '#4caf50' : '#f44336'} />
                      ))}
                    </Bar>
                  </RechartsBarChart>
                </ResponsiveContainer>
              )}
            </Paper>
          </Grid>
          <Grid item xs={12} md={4}>
            <Paper sx={{ p: 2, height: 400 }}>
              <Typography variant="h6" gutterBottom>
                Top 5 Gainers
              </Typography>
              {gainersLoading ? (
                <LinearProgress />
              ) : (
                <Box sx={{ mt: 2 }}>
                  {(gainers || []).slice(0, 5).map((stock, index) => (
                    <Box
                      key={stock.symbol}
                      sx={{
                        p: 2,
                        mb: 1,
                        borderRadius: 1,
                        backgroundColor: index % 2 === 0 ? 'rgba(33, 150, 243, 0.1)' : 'transparent',
                      }}
                    >
                      <Box display="flex" justifyContent="space-between" alignItems="center">
                        <Box>
                          <Typography variant="subtitle1">{stock.symbol}</Typography>
                          <Typography variant="caption" color="textSecondary">
                            {(stock.volume / 1000000).toFixed(1)}M volume
                          </Typography>
                        </Box>
                        <Chip
                          label={`+${stock.change.toFixed(2)}%`}
                          color="success"
                          size="small"
                          icon={<TrendingUp />}
                        />
                      </Box>
                    </Box>
                  ))}
                </Box>
              )}
            </Paper>
          </Grid>
        </Grid>
      )}

      {tabValue === 1 && (
        <Grid container spacing={3}>
          <Grid item xs={12} md={8}>
            <Paper sx={{ p: 2, height: 400 }}>
              <Typography variant="h6" gutterBottom>
                High Volume Stocks (Last {timeRange} Days)
              </Typography>
              {volumeLoading ? (
                <Box display="flex" justifyContent="center" alignItems="center" height="100%">
                  <LinearProgress sx={{ width: '50%' }} />
                </Box>
              ) : (
                <ResponsiveContainer width="100%" height="90%">
                  <RechartsBarChart data={volumeData}>
                    <CartesianGrid strokeDasharray="3 3" stroke="#444" />
                    <XAxis dataKey="name" stroke="#fff" />
                    <YAxis stroke="#fff" label={{ value: 'Volume (M)', angle: -90, position: 'insideLeft' }} />
                    <Tooltip
                      contentStyle={{ backgroundColor: '#132f4c', borderColor: '#2196f3' }}
                      formatter={(value) => [`${value}M`, 'Average Volume']}
                    />
                    <Legend />
                    <Bar dataKey="volume" fill="#2196f3" radius={[4, 4, 0, 0]} />
                  </RechartsBarChart>
                </ResponsiveContainer>
              )}
            </Paper>
          </Grid>
          <Grid item xs={12} md={4}>
            <Paper sx={{ p: 2, height: 400 }}>
              <Typography variant="h6" gutterBottom>
                Volume Leaders
              </Typography>
              {volumeLoading ? (
                <LinearProgress />
              ) : (
                <Box sx={{ mt: 2 }}>
                  {(volumeStocks || []).slice(0, 5).map((stock, index) => (
                    <Box
                      key={stock.symbol}
                      sx={{
                        p: 2,
                        mb: 1,
                        borderRadius: 1,
                        backgroundColor: index % 2 === 0 ? 'rgba(33, 150, 243, 0.1)' : 'transparent',
                      }}
                    >
                      <Box display="flex" justifyContent="space-between" alignItems="center">
                        <Box>
                          <Typography variant="subtitle1">{stock.symbol}</Typography>
                          <Typography variant="caption" color="textSecondary">
                            {stock.exchange} • {stock.company_name}
                          </Typography>
                        </Box>
                        <Typography variant="subtitle2">
                          {(stock.avg_volume / 1000000).toFixed(1)}M
                        </Typography>
                      </Box>
                    </Box>
                  ))}
                </Box>
              )}
            </Paper>
          </Grid>
        </Grid>
      )}

      {tabValue === 2 && (
        <Grid container spacing={3}>
          <Grid item xs={12} md={6}>
            <Paper sx={{ p: 2, height: 400 }}>
              <Typography variant="h6" gutterBottom>
                Stocks by Exchange
              </Typography>
              <ResponsiveContainer width="100%" height="90%">
                <RechartsPieChart>
                  <Pie
                    data={exchangeData}
                    cx="50%"
                    cy="50%"
                    labelLine={false}
                    label={({ name, percent }) => `${name}: ${(percent * 100).toFixed(0)}%`}
                    outerRadius={100}
                    fill="#8884d8"
                    dataKey="value"
                  >
                    {exchangeData.map((entry, index) => (
                      <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
                    ))}
                  </Pie>
                  <Tooltip formatter={(value) => [`${value} stocks`, 'Count']} />
                  <Legend />
                </RechartsPieChart>
              </ResponsiveContainer>
            </Paper>
          </Grid>
          <Grid item xs={12} md={6}>
            <Paper sx={{ p: 2, height: 400 }}>
              <Typography variant="h6" gutterBottom>
                Exchange Statistics
              </Typography>
              <Box sx={{ mt: 2 }}>
                {Object.entries(summary?.exchanges || {}).map(([exchange, count], index) => (
                  <Box
                    key={exchange}
                    sx={{
                      p: 2,
                      mb: 1,
                      borderRadius: 1,
                      backgroundColor: index % 2 === 0 ? 'rgba(33, 150, 243, 0.1)' : 'transparent',
                    }}
                  >
                    <Box display="flex" justifyContent="space-between" alignItems="center">
                      <Box>
                        <Typography variant="subtitle1">{exchange}</Typography>
                        <Typography variant="caption" color="textSecondary">
                          {((count / summary.total_stocks) * 100).toFixed(1)}% of total
                        </Typography>
                      </Box>
                      <Chip label={count} color="primary" variant="outlined" />
                    </Box>
                  </Box>
                ))}
              </Box>
            </Paper>
          </Grid>
        </Grid>
      )}

      {tabValue === 3 && (
        <Grid container spacing={3}>
          <Grid item xs={12}>
            <Paper sx={{ p: 2, height: 400 }}>
              <Typography variant="h6" gutterBottom>
                Volume vs Price Change Correlation
              </Typography>
              {gainersLoading ? (
                <Box display="flex" justifyContent="center" alignItems="center" height="100%">
                  <LinearProgress sx={{ width: '50%' }} />
                </Box>
              ) : (
                <ResponsiveContainer width="100%" height="90%">
                  <ScatterChart>
                    <CartesianGrid strokeDasharray="3 3" stroke="#444" />
                    <XAxis 
                      type="number" 
                      dataKey="x" 
                      name="Price Change %" 
                      stroke="#fff"
                      label={{ value: 'Price Change %', position: 'insideBottom', offset: -5 }}
                    />
                    <YAxis 
                      type="number" 
                      dataKey="y" 
                      name="Volume (M)" 
                      stroke="#fff"
                      label={{ value: 'Volume (M)', angle: -90, position: 'insideLeft' }}
                    />
                    <ZAxis type="number" dataKey="z" range={[50, 300]} name="size" />
                    <Tooltip 
                      contentStyle={{ backgroundColor: '#132f4c', borderColor: '#2196f3' }}
                      formatter={(value, name) => {
                        if (name === 'x') return [`${value}%`, 'Price Change'];
                        if (name === 'y') return [`${value}M`, 'Volume'];
                        return [value, name];
                      }}
                    />
                    <Legend />
                    <Scatter name="Stocks" data={scatterData} fill="#8884d8" />
                  </ScatterChart>
                </ResponsiveContainer>
              )}
            </Paper>
          </Grid>
        </Grid>
      )}

      {/* Analytics Summary */}
      <Paper sx={{ p: 3, mt: 3 }}>
        <Typography variant="h6" gutterBottom>
          Analytics Summary
        </Typography>
        <Grid container spacing={2}>
          <Grid item xs={12} md={6}>
            <Box>
              <Typography variant="subtitle2" color="primary" gutterBottom>
                Data Coverage
              </Typography>
              <Typography variant="body2">
                Total stocks tracked: {summary?.total_stocks || 0}
              </Typography>
              <Typography variant="body2">
                Historical records: {(summary?.total_records || 0).toLocaleString()}
              </Typography>
              <Typography variant="body2">
                Date range: {summary?.date_range?.min_date} to {summary?.date_range?.max_date}
              </Typography>
            </Box>
          </Grid>
          <Grid item xs={12} md={6}>
            <Box>
              <Typography variant="subtitle2" color="primary" gutterBottom>
                Market Distribution
              </Typography>
              <Box display="flex" gap={1} flexWrap="wrap">
                {Object.entries(summary?.exchanges || {}).map(([exchange, count]) => (
                  <Chip
                    key={exchange}
                    label={`${exchange}: ${count}`}
                    size="small"
                    variant="outlined"
                  />
                ))}
              </Box>
            </Box>
          </Grid>
        </Grid>
      </Paper>
    </Box>
  );
}

export default Analytics;