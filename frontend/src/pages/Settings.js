import React, { useState } from 'react';
import {
  Box,
  Paper,
  Typography,
  Grid,
  Card,
  CardContent,
  TextField,
  Button,
  Switch,
  FormControlLabel,
  Divider,
  Alert,
  LinearProgress,
  Tab,
  Tabs,
  Select,
  MenuItem,
  InputLabel,
  FormControl,
  Slider,
  Chip,
  IconButton,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  List,
  ListItem,
  ListItemText,
  ListItemIcon,
  ListItemSecondaryAction,
} from '@mui/material';
import {
  Save as SaveIcon,
  RestartAlt as RestartIcon,
  Backup as BackupIcon,
  Security as SecurityIcon,
  Notifications as NotificationsIcon,
  Api as ApiIcon,
  Storage as StorageIcon,
  Speed as SpeedIcon,
  Delete as DeleteIcon,
  Add as AddIcon,
  Edit as EditIcon,
} from '@mui/icons-material';
import { useQuery, useMutation } from 'react-query';
import axios from 'axios';
import toast from 'react-hot-toast';

const API_BASE_URL = process.env.REACT_APP_API_URL || '/api/v1';

// Mock settings data
const mockSettings = {
  general: {
    appName: 'Stock Data Pipeline',
    timezone: 'UTC',
    refreshInterval: 30,
    theme: 'dark',
  },
  dataCollection: {
    enabled: true,
    schedule: '6 hours',
    symbolsPerExchange: 10,
    daysOfData: 30,
    apiKey: '••••••••••••••••',
  },
  notifications: {
    emailAlerts: true,
    emailAddress: 'admin@example.com',
    slackAlerts: false,
    slackWebhook: '',
    criticalAlerts: true,
    warningAlerts: true,
    infoAlerts: false,
  },
  performance: {
    sparkWorkers: 2,
    sparkMemory: '2G',
    databaseConnections: 20,
    cacheSize: '1GB',
  },
};

function Settings() {
  const [tabValue, setTabValue] = useState(0);
  const [settings, setSettings] = useState(mockSettings);
  const [backupDialog, setBackupDialog] = useState(false);
  const [resetDialog, setResetDialog] = useState(false);
  const [saving, setSaving] = useState(false);

  // Fetch actual settings from API
  const { data, isLoading, error } = useQuery(
    'settings',
    async () => {
      // In production, this would be an API call
      // For now, return mock data
      return mockSettings;
    }
  );

  const handleTabChange = (event, newValue) => {
    setTabValue(newValue);
  };

  const handleSettingChange = (category, key, value) => {
    setSettings(prev => ({
      ...prev,
      [category]: {
        ...prev[category],
        [key]: value,
      },
    }));
  };

  const handleSave = async () => {
    setSaving(true);
    try {
      // In production, this would be an API call
      await new Promise(resolve => setTimeout(resolve, 1000));
      toast.success('Settings saved successfully');
    } catch (error) {
      toast.error('Failed to save settings');
    } finally {
      setSaving(false);
    }
  };

  const handleBackup = () => {
    setBackupDialog(true);
  };

  const handleReset = () => {
    setResetDialog(true);
  };

  const confirmReset = () => {
    setSettings(mockSettings);
    setResetDialog(false);
    toast.success('Settings reset to defaults');
  };

  const confirmBackup = () => {
    setBackupDialog(false);
    toast.success('Backup created successfully');
  };

  if (isLoading) {
    return (
      <Box sx={{ width: '100%' }}>
        <LinearProgress />
      </Box>
    );
  }

  if (error) {
    return (
      <Alert severity="error" sx={{ mt: 2 }}>
        Failed to load settings: {error.message}
      </Alert>
    );
  }

  return (
    <Box>
      {/* Header */}
      <Paper sx={{ p: 3, mb: 3 }}>
        <Box display="flex" justifyContent="space-between" alignItems="center" flexWrap="wrap" gap={2}>
          <Box>
            <Typography variant="h4" gutterBottom>
              System Settings
            </Typography>
            <Typography variant="body1" color="textSecondary">
              Configure system behavior and preferences
            </Typography>
          </Box>
          <Box display="flex" gap={1}>
            <Button
              variant="outlined"
              startIcon={<BackupIcon />}
              onClick={handleBackup}
            >
              Backup
            </Button>
            <Button
              variant="outlined"
              startIcon={<RestartIcon />}
              onClick={handleReset}
              color="warning"
            >
              Reset
            </Button>
            <Button
              variant="contained"
              startIcon={<SaveIcon />}
              onClick={handleSave}
              disabled={saving}
            >
              {saving ? 'Saving...' : 'Save Changes'}
            </Button>
          </Box>
        </Box>
      </Paper>

      {/* Tabs */}
      <Paper sx={{ mb: 3 }}>
        <Tabs value={tabValue} onChange={handleTabChange}>
          <Tab icon={<SpeedIcon />} label="General" />
          <Tab icon={<ApiIcon />} label="Data Collection" />
          <Tab icon={<NotificationsIcon />} label="Notifications" />
          <Tab icon={<StorageIcon />} label="Performance" />
          <Tab icon={<SecurityIcon />} label="Security" />
        </Tabs>
      </Paper>

      {/* General Settings */}
      {tabValue === 0 && (
        <Grid container spacing={3}>
          <Grid item xs={12} md={6}>
            <Paper sx={{ p: 3 }}>
              <Typography variant="h6" gutterBottom>
                Application Settings
              </Typography>
              <Box sx={{ mt: 2 }}>
                <TextField
                  fullWidth
                  label="Application Name"
                  value={settings.general.appName}
                  onChange={(e) => handleSettingChange('general', 'appName', e.target.value)}
                  margin="normal"
                />
                <FormControl fullWidth margin="normal">
                  <InputLabel>Timezone</InputLabel>
                  <Select
                    value={settings.general.timezone}
                    label="Timezone"
                    onChange={(e) => handleSettingChange('general', 'timezone', e.target.value)}
                  >
                    <MenuItem value="UTC">UTC</MenuItem>
                    <MenuItem value="EST">Eastern Time</MenuItem>
                    <MenuItem value="PST">Pacific Time</MenuItem>
                    <MenuItem value="CET">Central European Time</MenuItem>
                  </Select>
                </FormControl>
                <Box sx={{ mt: 2 }}>
                  <Typography gutterBottom>Auto-refresh Interval (seconds)</Typography>
                  <Slider
                    value={settings.general.refreshInterval}
                    onChange={(e, value) => handleSettingChange('general', 'refreshInterval', value)}
                    min={5}
                    max={300}
                    step={5}
                    valueLabelDisplay="auto"
                    valueLabelFormat={(value) => `${value}s`}
                  />
                  <Typography variant="caption" color="textSecondary">
                    Current: {settings.general.refreshInterval} seconds
                  </Typography>
                </Box>
                <FormControl fullWidth margin="normal">
                  <InputLabel>Theme</InputLabel>
                  <Select
                    value={settings.general.theme}
                    label="Theme"
                    onChange={(e) => handleSettingChange('general', 'theme', e.target.value)}
                  >
                    <MenuItem value="dark">Dark</MenuItem>
                    <MenuItem value="light">Light</MenuItem>
                    <MenuItem value="auto">Auto</MenuItem>
                  </Select>
                </FormControl>
              </Box>
            </Paper>
          </Grid>
          <Grid item xs={12} md={6}>
            <Paper sx={{ p: 3 }}>
              <Typography variant="h6" gutterBottom>
                System Information
              </Typography>
              <List>
                <ListItem>
                  <ListItemText
                    primary="Version"
                    secondary="1.0.0"
                  />
                </ListItem>
                <Divider />
                <ListItem>
                  <ListItemText
                    primary="Last Updated"
                    secondary={new Date().toLocaleDateString()}
                  />
                </ListItem>
                <Divider />
                <ListItem>
                  <ListItemText
                    primary="Database Size"
                    secondary="45.2 GB"
                  />
                </ListItem>
                <Divider />
                <ListItem>
                  <ListItemText
                    primary="HDFS Usage"
                    secondary="2.3 TB / 5 TB"
                  />
                  <ListItemSecondaryAction>
                    <Chip label="46%" color="primary" size="small" />
                  </ListItemSecondaryAction>
                </ListItem>
                <Divider />
                <ListItem>
                  <ListItemText
                    primary="Active Services"
                    secondary="6 services running"
                  />
                  <ListItemSecondaryAction>
                    <Chip label="Healthy" color="success" size="small" />
                  </ListItemSecondaryAction>
                </ListItem>
              </List>
            </Paper>
          </Grid>
        </Grid>
      )}

      {/* Data Collection Settings */}
      {tabValue === 1 && (
        <Grid container spacing={3}>
          <Grid item xs={12}>
            <Paper sx={{ p: 3 }}>
              <Typography variant="h6" gutterBottom>
                Data Collection Configuration
              </Typography>
              <Box sx={{ mt: 2 }}>
                <FormControlLabel
                  control={
                    <Switch
                      checked={settings.dataCollection.enabled}
                      onChange={(e) => handleSettingChange('dataCollection', 'enabled', e.target.checked)}
                    />
                  }
                  label="Enable Automatic Data Collection"
                />
                <FormControl fullWidth margin="normal">
                  <InputLabel>Collection Schedule</InputLabel>
                  <Select
                    value={settings.dataCollection.schedule}
                    label="Collection Schedule"
                    onChange={(e) => handleSettingChange('dataCollection', 'schedule', e.target.value)}
                  >
                    <MenuItem value="1 hour">Every Hour</MenuItem>
                    <MenuItem value="6 hours">Every 6 Hours</MenuItem>
                    <MenuItem value="12 hours">Every 12 Hours</MenuItem>
                    <MenuItem value="24 hours">Daily</MenuItem>
                  </Select>
                </FormControl>
                <TextField
                  fullWidth
                  label="API Key"
                  type="password"
                  value={settings.dataCollection.apiKey}
                  onChange={(e) => handleSettingChange('dataCollection', 'apiKey', e.target.value)}
                  margin="normal"
                  helperText="EOD Historical Data API key"
                />
                <Grid container spacing={2} sx={{ mt: 1 }}>
                  <Grid item xs={12} md={6}>
                    <TextField
                      fullWidth
                      label="Symbols per Exchange"
                      type="number"
                      value={settings.dataCollection.symbolsPerExchange}
                      onChange={(e) => handleSettingChange('dataCollection', 'symbolsPerExchange', e.target.value)}
                      margin="normal"
                      helperText="Number of stocks to collect per exchange"
                    />
                  </Grid>
                  <Grid item xs={12} md={6}>
                    <TextField
                      fullWidth
                      label="Days of Historical Data"
                      type="number"
                      value={settings.dataCollection.daysOfData}
                      onChange={(e) => handleSettingChange('dataCollection', 'daysOfData', e.target.value)}
                      margin="normal"
                      helperText="Number of days of historical data to collect"
                    />
                  </Grid>
                </Grid>
              </Box>
            </Paper>
          </Grid>
          <Grid item xs={12}>
            <Paper sx={{ p: 3 }}>
              <Typography variant="h6" gutterBottom>
                Data Sources
              </Typography>
              <List>
                <ListItem>
                  <ListItemIcon>
                    <ApiIcon />
                  </ListItemIcon>
                  <ListItemText
                    primary="EOD Historical Data"
                    secondary="Primary data source for stock prices"
                  />
                  <ListItemSecondaryAction>
                    <Chip label="Active" color="success" size="small" />
                  </ListItemSecondaryAction>
                </ListItem>
                <Divider />
                <ListItem>
                  <ListItemIcon>
                    <StorageIcon />
                  </ListItemIcon>
                  <ListItemText
                    primary="HDFS Storage"
                    secondary="Distributed storage for raw data"
                  />
                  <ListItemSecondaryAction>
                    <Chip label="2.3TB" color="primary" size="small" />
                  </ListItemSecondaryAction>
                </ListItem>
                <Divider />
                <ListItem>
                  <ListItemIcon>
                    <SpeedIcon />
                  </ListItemIcon>
                  <ListItemText
                    primary="PostgreSQL Database"
                    secondary="Metadata and processed data storage"
                  />
                  <ListItemSecondaryAction>
                    <Chip label="45.2GB" color="primary" size="small" />
                  </ListItemSecondaryAction>
                </ListItem>
              </List>
            </Paper>
          </Grid>
        </Grid>
      )}

      {/* Notification Settings */}
      {tabValue === 2 && (
        <Grid container spacing={3}>
          <Grid item xs={12} md={6}>
            <Paper sx={{ p: 3 }}>
              <Typography variant="h6" gutterBottom>
                Alert Preferences
              </Typography>
              <Box sx={{ mt: 2 }}>
                <FormControlLabel
                  control={
                    <Switch
                      checked={settings.notifications.emailAlerts}
                      onChange={(e) => handleSettingChange('notifications', 'emailAlerts', e.target.checked)}
                    />
                  }
                  label="Email Alerts"
                />
                {settings.notifications.emailAlerts && (
                  <TextField
                    fullWidth
                    label="Email Address"
                    value={settings.notifications.emailAddress}
                    onChange={(e) => handleSettingChange('notifications', 'emailAddress', e.target.value)}
                    margin="normal"
                  />
                )}
                <FormControlLabel
                  control={
                    <Switch
                      checked={settings.notifications.slackAlerts}
                      onChange={(e) => handleSettingChange('notifications', 'slackAlerts', e.target.checked)}
                    />
                  }
                  label="Slack Notifications"
                />
                {settings.notifications.slackAlerts && (
                  <TextField
                    fullWidth
                    label="Slack Webhook URL"
                    value={settings.notifications.slackWebhook}
                    onChange={(e) => handleSettingChange('notifications', 'slackWebhook', e.target.value)}
                    margin="normal"
                  />
                )}
              </Box>
            </Paper>
          </Grid>
          <Grid item xs={12} md={6}>
            <Paper sx={{ p: 3 }}>
              <Typography variant="h6" gutterBottom>
                Alert Types
              </Typography>
              <Box sx={{ mt: 2 }}>
                <FormControlLabel
                  control={
                    <Switch
                      checked={settings.notifications.criticalAlerts}
                      onChange={(e) => handleSettingChange('notifications', 'criticalAlerts', e.target.checked)}
                    />
                  }
                  label="Critical Alerts"
                />
                <Typography variant="caption" color="textSecondary" display="block">
                  System failures, data corruption, service downtime
                </Typography>
                <FormControlLabel
                  control={
                    <Switch
                      checked={settings.notifications.warningAlerts}
                      onChange={(e) => handleSettingChange('notifications', 'warningAlerts', e.target.checked)}
                    />
                  }
                  label="Warning Alerts"
                />
                <Typography variant="caption" color="textSecondary" display="block">
                  Performance degradation, resource warnings
                </Typography>
                <FormControlLabel
                  control={
                    <Switch
                      checked={settings.notifications.infoAlerts}
                      onChange={(e) => handleSettingChange('notifications', 'infoAlerts', e.target.checked)}
                    />
                  }
                  label="Information Alerts"
                />
                <Typography variant="caption" color="textSecondary" display="block">
                  System updates, scheduled maintenance
                </Typography>
              </Box>
            </Paper>
          </Grid>
        </Grid>
      )}

      {/* Performance Settings */}
      {tabValue === 3 && (
        <Grid container spacing={3}>
          <Grid item xs={12} md={6}>
            <Paper sx={{ p: 3 }}>
              <Typography variant="h6" gutterBottom>
                Spark Configuration
              </Typography>
              <Box sx={{ mt: 2 }}>
                <TextField
                  fullWidth
                  label="Number of Spark Workers"
                  type="number"
                  value={settings.performance.sparkWorkers}
                  onChange={(e) => handleSettingChange('performance', 'sparkWorkers', e.target.value)}
                  margin="normal"
                  helperText="Recommended: 2-4 workers"
                />
                <FormControl fullWidth margin="normal">
                  <InputLabel>Worker Memory</InputLabel>
                  <Select
                    value={settings.performance.sparkMemory}
                    label="Worker Memory"
                    onChange={(e) => handleSettingChange('performance', 'sparkMemory', e.target.value)}
                  >
                    <MenuItem value="1G">1 GB</MenuItem>
                    <MenuItem value="2G">2 GB</MenuItem>
                    <MenuItem value="4G">4 GB</MenuItem>
                    <MenuItem value="8G">8 GB</MenuItem>
                  </Select>
                </FormControl>
              </Box>
            </Paper>
          </Grid>
          <Grid item xs={12} md={6}>
            <Paper sx={{ p: 3 }}>
              <Typography variant="h6" gutterBottom>
                System Resources
              </Typography>
              <Box sx={{ mt: 2 }}>
                <TextField
                  fullWidth
                  label="Database Connections"
                  type="number"
                  value={settings.performance.databaseConnections}
                  onChange={(e) => handleSettingChange('performance', 'databaseConnections', e.target.value)}
                  margin="normal"
                  helperText="Maximum database connection pool size"
                />
                <FormControl fullWidth margin="normal">
                  <InputLabel>Redis Cache Size</InputLabel>
                  <Select
                    value={settings.performance.cacheSize}
                    label="Redis Cache Size"
                    onChange={(e) => handleSettingChange('performance', 'cacheSize', e.target.value)}
                  >
                    <MenuItem value="512MB">512 MB</MenuItem>
                    <MenuItem value="1GB">1 GB</MenuItem>
                    <MenuItem value="2GB">2 GB</MenuItem>
                    <MenuItem value="4GB">4 GB</MenuItem>
                  </Select>
                </FormControl>
              </Box>
            </Paper>
          </Grid>
        </Grid>
      )}

      {/* Security Settings */}
      {tabValue === 4 && (
        <Grid container spacing={3}>
          <Grid item xs={12}>
            <Paper sx={{ p: 3 }}>
              <Typography variant="h6" gutterBottom>
                Security Configuration
              </Typography>
              <Alert severity="info" sx={{ mb: 3 }}>
                Security settings require system restart to take effect
              </Alert>
              <Box sx={{ mt: 2 }}>
                <FormControlLabel
                  control={<Switch defaultChecked />}
                  label="Enable HTTPS"
                />
                <FormControlLabel
                  control={<Switch defaultChecked />}
                  label="Require Authentication"
                />
                <FormControlLabel
                  control={<Switch />}
                  label="Two-Factor Authentication"
                />
                <TextField
                  fullWidth
                  label="Session Timeout (minutes)"
                  type="number"
                  defaultValue="30"
                  margin="normal"
                />
                <TextField
                  fullWidth
                  label="API Rate Limit (requests/minute)"
                  type="number"
                  defaultValue="60"
                  margin="normal"
                />
              </Box>
            </Paper>
          </Grid>
        </Grid>
      )}

      {/* Backup Dialog */}
      <Dialog open={backupDialog} onClose={() => setBackupDialog(false)}>
        <DialogTitle>Create Backup</DialogTitle>
        <DialogContent>
          <Typography variant="body2" color="textSecondary" gutterBottom>
            This will create a backup of all system settings and configurations.
          </Typography>
          <FormControl fullWidth margin="normal">
            <InputLabel>Backup Type</InputLabel>
            <Select defaultValue="full">
              <MenuItem value="full">Full Backup (Settings + Data)</MenuItem>
              <MenuItem value="settings">Settings Only</MenuItem>
              <MenuItem value="config">Configuration Only</MenuItem>
            </Select>
          </FormControl>
          <TextField
            fullWidth
            label="Backup Name"
            defaultValue={`backup_${new Date().toISOString().split('T')[0]}`}
            margin="normal"
          />
          <FormControlLabel
            control={<Switch defaultChecked />}
            label="Include historical data"
          />
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setBackupDialog(false)}>Cancel</Button>
          <Button onClick={confirmBackup} variant="contained">
            Create Backup
          </Button>
        </DialogActions>
      </Dialog>

      {/* Reset Dialog */}
      <Dialog open={resetDialog} onClose={() => setResetDialog(false)}>
        <DialogTitle>Reset Settings</DialogTitle>
        <DialogContent>
          <Alert severity="warning" sx={{ mb: 2 }}>
            This will reset all settings to their default values. This action cannot be undone.
          </Alert>
          <Typography variant="body2">
            Are you sure you want to reset all settings?
          </Typography>
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setResetDialog(false)}>Cancel</Button>
          <Button onClick={confirmReset} color="warning" variant="contained">
            Reset Settings
          </Button>
        </DialogActions>
      </Dialog>

      {/* Settings Summary */}
      <Paper sx={{ p: 3, mt: 3 }}>
        <Typography variant="h6" gutterBottom>
          Settings Summary
        </Typography>
        <Grid container spacing={2}>
          <Grid item xs={12} md={6}>
            <Box>
              <Typography variant="subtitle2" color="primary" gutterBottom>
                Current Configuration
              </Typography>
              <Typography variant="body2">
                Data Collection: {settings.dataCollection.enabled ? 'Enabled' : 'Disabled'}
              </Typography>
              <Typography variant="body2">
                Collection Schedule: Every {settings.dataCollection.schedule}
              </Typography>
              <Typography variant="body2">
                Theme: {settings.general.theme}
              </Typography>
              <Typography variant="body2">
                Auto-refresh: {settings.general.refreshInterval} seconds
              </Typography>
            </Box>
          </Grid>
          <Grid item xs={12} md={6}>
            <Box>
              <Typography variant="subtitle2" color="primary" gutterBottom>
                System Status
              </Typography>
              <Box display="flex" gap={1} flexWrap="wrap">
                <Chip label={`${settings.performance.sparkWorkers} Spark Workers`} size="small" />
                <Chip label={`${settings.performance.databaseConnections} DB Connections`} size="small" />
                <Chip label={settings.notifications.emailAlerts ? 'Email Alerts On' : 'Email Alerts Off'} size="small" />
              </Box>
            </Box>
          </Grid>
        </Grid>
      </Paper>
    </Box>
  );
}

export default Settings;