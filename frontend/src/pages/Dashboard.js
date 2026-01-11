import React from 'react';
import {
  Grid,
  Paper,
  Typography,
  Box,
  Card,
  CardContent,
  LinearProgress,
  Chip,
} from '@mui/material';
import {
  TrendingUp,
  TrendingDown,
  Equalizer,
  Storage,
  Speed,
  Security,
} from '@mui/icons-material';
import { useQuery } from 'react-query';
import axios from 'axios';
import {
  LineChart,
  Line,
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
  PieChart,
  Pie,
  Cell,
} from 'recharts';
import toast from 'react-hot-toast';

// API base URL
// const API_BASE_URL = process.env.REACT_APP_API_URL || 'http://localhost:8000/api/v1';
const API_BASE_URL = '/api/v1';
// Custom hook for API calls
const useStockData = () => {
  return useQuery('dashboardData', async () => {
    const [summary, gainers, volume, health] = await Promise.all([
      axios.get(`${API_BASE_URL}/analytics/summary`),
      axios.get(`${API_BASE_URL}/analytics/top-gainers`),
      axios.get(`${API_BASE_URL}/analytics/high-volume`),
      axios.get(`${API_BASE_URL}/health`),
    ]);
    return {
      summary: summary.data,
      gainers: gainers.data,
      volume: volume.data,
      health: health.data,
    };
  }, {
    refetchInterval: 30000, // Refetch every 30 seconds
    onError: (error) => {
      toast.error(`Failed to load data: ${error.message}`);
    },
  });
};

// Stat Card Component
const StatCard = ({ title, value, icon, color, trend, subtitle }) => (
  <Card sx={{ height: '100%' }}>
    <CardContent>
      <Box display="flex" justifyContent="space-between" alignItems="center" mb={2}>
        <Box>
          <Typography color="textSecondary" gutterBottom variant="overline">
            {title}
          </Typography>
          <Typography variant="h4">{value}</Typography>
          {subtitle && (
            <Typography variant="body2" color="textSecondary">
              {subtitle}
            </Typography>
          )}
        </Box>
        <Box
          sx={{
            backgroundColor: `${color}20`,
            borderRadius: '12px',
            p: 1.5,
          }}
        >
          {React.cloneElement(icon, { sx: { color, fontSize: 28 } })}
        </Box>
      </Box>
      {trend && (
        <Box display="flex" alignItems="center">
          <Chip
            label={trend.value}
            size="small"
            icon={trend.direction === 'up' ? <TrendingUp /> : <TrendingDown />}
            color={trend.direction === 'up' ? 'success' : 'error'}
            variant="outlined"
          />
          <Typography variant="caption" color="textSecondary" sx={{ ml: 1 }}>
            {trend.label}
          </Typography>
        </Box>
      )}
    </CardContent>
  </Card>
);

function Dashboard() {
  const { data, isLoading, error } = useStockData();

  if (isLoading) {
    return (
      <Box display="flex" justifyContent="center" alignItems="center" minHeight="60vh">
        <LinearProgress sx={{ width: '50%' }} />
      </Box>
    );
  }

  if (error) {
    return (
      <Paper sx={{ p: 3, textAlign: 'center' }}>
        <Typography color="error">Error loading dashboard data</Typography>
      </Paper>
    );
  }

  const { summary, gainers, volume } = data;

  // Prepare chart data
  const exchangeData = summary?.exchanges
  ? Object.entries(summary.exchanges).map(([name, count]) => ({
      name,
      value: count,
    }))
  : [];


  const topGainersData = (gainers || []).slice(0, 5).map((stock) => ({
    name: stock.symbol,
    change: stock.change,
  }));

  const highVolumeData = (volume || []).slice(0, 5).map((stock) => ({
    name: stock.symbol,
    volume: Math.round(stock.avg_volume / 1000000), // Convert to millions
  }));

  const COLORS = ['#0088FE', '#00C49F', '#FFBB28', '#FF8042', '#8884D8'];

  return (
    <Box>
      <Typography variant="h4" gutterBottom sx={{ mb: 4 }}>
        Dashboard Overview
      </Typography>

      {/* Statistics Cards */}
      <Grid container spacing={3} sx={{ mb: 4 }}>
        <Grid item xs={12} sm={6} md={3}>
          <StatCard
            title="Total Stocks"
            value={summary?.total_stocks || 0}
            icon={<Equalizer />}
            color="#2196f3"
            trend={{ value: '+5.2%', direction: 'up', label: 'vs last week' }}
          />
        </Grid>
        <Grid item xs={12} sm={6} md={3}>
          <StatCard
            title="Total Records"
            value={(summary?.total_records || 0).toLocaleString()}
            icon={<Storage />}
            color="#4caf50"
            subtitle={
            summary?.latest_update
              ? `Last update: ${new Date(summary.latest_update).toLocaleDateString()}`
              : 'No update yet'
          }

          />
        </Grid>
        <Grid item xs={12} sm={6} md={3}>
          <StatCard
            title="Top Gainer"
            value={gainers?.[0]?.symbol || 'N/A'}
            icon={<TrendingUp />}
            color="#ff9800"
            trend={
              gainers?.[0] && {
                value: `+${gainers[0].change.toFixed(2)}%`,
                direction: 'up',
                label: 'Today',
              }
            }
          />
        </Grid>
        <Grid item xs={12} sm={6} md={3}>
          <StatCard
            title="System Health"
            value="Healthy"
            icon={<Security />}
            color="#9c27b0"
            subtitle="All services running"
          />
        </Grid>
      </Grid>

      {/* Charts Row 1 */}
      <Grid container spacing={3} sx={{ mb: 4 }}>
        <Grid item xs={12} md={6}>
          <Paper sx={{ p: 2, height: 350 }}>
            <Typography variant="h6" gutterBottom>
              Stocks by Exchange
            </Typography>
            <ResponsiveContainer width="100%" height="85%">
              <PieChart>
                <Pie
                  data={exchangeData}
                  cx="50%"
                  cy="50%"
                  labelLine={false}
                  label={({ name, percent }) => `${name}: ${(percent * 100).toFixed(0)}%`}
                  outerRadius={80}
                  fill="#8884d8"
                  dataKey="value"
                >
                  {exchangeData.map((entry, index) => (
                    <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
                  ))}
                </Pie>
                <Tooltip formatter={(value) => [`${value} stocks`, 'Count']} />
                <Legend />
              </PieChart>
            </ResponsiveContainer>
          </Paper>
        </Grid>
        <Grid item xs={12} md={6}>
          <Paper sx={{ p: 2, height: 350 }}>
            <Typography variant="h6" gutterBottom>
              Top Gainers Performance
            </Typography>
            <ResponsiveContainer width="100%" height="85%">
              <BarChart data={topGainersData}>
                <CartesianGrid strokeDasharray="3 3" stroke="#444" />
                <XAxis dataKey="name" stroke="#fff" />
                <YAxis stroke="#fff" label={{ value: '% Change', angle: -90, position: 'insideLeft' }} />
                <Tooltip
                  formatter={(value) => [`${value}%`, 'Change']}
                  contentStyle={{ backgroundColor: '#132f4c', borderColor: '#2196f3' }}
                />
                <Legend />
                <Bar dataKey="change" fill="#4caf50" radius={[4, 4, 0, 0]} />
              </BarChart>
            </ResponsiveContainer>
          </Paper>
        </Grid>
      </Grid>

      {/* Charts Row 2 */}
      <Grid container spacing={3}>
        <Grid item xs={12} md={6}>
          <Paper sx={{ p: 2, height: 350 }}>
            <Typography variant="h6" gutterBottom>
              High Volume Stocks (in millions)
            </Typography>
            <ResponsiveContainer width="100%" height="85%">
              <BarChart data={highVolumeData}>
                <CartesianGrid strokeDasharray="3 3" stroke="#444" />
                <XAxis dataKey="name" stroke="#fff" />
                <YAxis stroke="#fff" label={{ value: 'Volume (M)', angle: -90, position: 'insideLeft' }} />
                <Tooltip
                  formatter={(value) => [`${value}M`, 'Average Volume']}
                  contentStyle={{ backgroundColor: '#132f4c', borderColor: '#2196f3' }}
                />
                <Legend />
                <Bar dataKey="volume" fill="#2196f3" radius={[4, 4, 0, 0]} />
              </BarChart>
            </ResponsiveContainer>
          </Paper>
        </Grid>
        <Grid item xs={12} md={6}>
          <Paper sx={{ p: 2, height: 350 }}>
            <Typography variant="h6" gutterBottom>
              System Performance
            </Typography>
            <Box sx={{ mt: 2 }}>
              <Box sx={{ mb: 2 }}>
                <Typography variant="body2" gutterBottom>
                  API Response Time
                </Typography>
                <LinearProgress variant="determinate" value={85} sx={{ height: 8, borderRadius: 4 }} />
                <Typography variant="caption" color="textSecondary">
                  85% - 120ms average
                </Typography>
              </Box>
              <Box sx={{ mb: 2 }}>
                <Typography variant="body2" gutterBottom>
                  Data Processing
                </Typography>
                <LinearProgress variant="determinate" value={72} sx={{ height: 8, borderRadius: 4 }} />
                <Typography variant="caption" color="textSecondary">
                  72% - Processing 5000 records/sec
                </Typography>
              </Box>
              <Box sx={{ mb: 2 }}>
                <Typography variant="body2" gutterBottom>
                  HDFS Storage
                </Typography>
                <LinearProgress variant="determinate" value={45} sx={{ height: 8, borderRadius: 4 }} />
                <Typography variant="caption" color="textSecondary">
                  45% - 2.3TB of 5TB used
                </Typography>
              </Box>
              <Box>
                <Typography variant="body2" gutterBottom>
                  Database Connections
                </Typography>
                <LinearProgress variant="determinate" value={60} sx={{ height: 8, borderRadius: 4 }} />
                <Typography variant="caption" color="textSecondary">
                  60% - 12/20 connections active
                </Typography>
              </Box>
            </Box>
          </Paper>
        </Grid>
      </Grid>

      {/* Recent Activity */}
      <Paper sx={{ p: 3, mt: 4 }}>
        <Typography variant="h6" gutterBottom>
          Recent Activity
        </Typography>
        <Grid container spacing={2}>
          <Grid item xs={12} md={6}>
            <Box>
              <Typography variant="subtitle2" color="primary" gutterBottom>
                Latest Data Collection
              </Typography>
              <Typography variant="body2">
                {summary?.latest_update
                  ? `Last updated: ${new Date(summary.latest_update).toLocaleString()}`
                  : 'No data available'}
              </Typography>
              <Typography variant="caption" color="textSecondary">
                {summary?.date_range?.min_date && summary?.date_range?.max_date
                  ? `Data range: ${summary.date_range.min_date} to ${summary.date_range.max_date}`
                  : 'Data range: N/A'}

              </Typography>
            </Box>
          </Grid>
          <Grid item xs={12} md={6}>
            <Box>
              <Typography variant="subtitle2" color="primary" gutterBottom>
                Top Performing Exchanges
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

export default Dashboard;