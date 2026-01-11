import React, { useState, useEffect } from 'react';
import {
  Box,
  Paper,
  Typography,
  Breadcrumbs,
  Link,
  List,
  ListItem,
  ListItemIcon,
  ListItemText,
  IconButton,
  LinearProgress,
  Alert,
  TextField,
  Button,
  Chip,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Tooltip,
  Grid
} from '@mui/material';
import {
  Folder as FolderIcon,
  InsertDriveFile as FileIcon,
  Refresh as RefreshIcon,
  Home as HomeIcon,
  ArrowUpward as UpIcon,
  Search as SearchIcon,
  Storage as StorageIcon,
  Description as DescriptionIcon,
  GetApp as DownloadIcon,
  Visibility as ViewIcon,
  FolderOpen as FolderOpenIcon,
} from '@mui/icons-material';
import { useQuery } from 'react-query';
import axios from 'axios';
import toast from 'react-hot-toast';

const API_BASE_URL = process.env.REACT_APP_API_URL || '/api/v1';

function HDFSBrowser() {
  const [currentPath, setCurrentPath] = useState('/stock_data');
  const [searchTerm, setSearchTerm] = useState('');
  const [viewFileDialog, setViewFileDialog] = useState(false);
  const [selectedFile, setSelectedFile] = useState(null);
  const [fileContent, setFileContent] = useState([]);

  // Fetch HDFS files
  const { data: files, isLoading, error, refetch } = useQuery(
    ['hdfsFiles', currentPath],
    async () => {
      const response = await axios.get(`${API_BASE_URL}/hdfs/files`, {
        params: { path: currentPath }
      });
      return response.data;
    },
    { refetchInterval: 30000 }
  );

  // Fetch file content
  const fetchFileContent = async (filePath) => {
    try {
      const response = await axios.get(`${API_BASE_URL}/hdfs/read`, {
        params: { file_path: filePath, limit: 50 }
      });
      setFileContent(response.data);
    } catch (error) {
      toast.error(`Failed to read file: ${error.message}`);
      setFileContent([]);
    }
  };

  const handleNavigate = (path) => {
    setCurrentPath(path);
  };

  const handleGoUp = () => {
    const parentPath = currentPath.substring(0, currentPath.lastIndexOf('/'));
    if (parentPath === '') {
      setCurrentPath('/');
    } else {
      setCurrentPath(parentPath);
    }
  };

  const handleRefresh = () => {
    refetch();
    toast.success('Refreshed HDFS directory');
  };

  const handleViewFile = (file) => {
    if (file.type === 'file') {
      setSelectedFile(file);
      fetchFileContent(file.path);
      setViewFileDialog(true);
    } else {
      handleNavigate(file.path);
    }
  };

  const handleDownload = async (file) => {
    toast.success(`Download started for ${file.name}`);
    // In production, this would trigger a real download
    // For now, just show a toast
  };

  const formatFileSize = (bytes) => {
    if (bytes === 0) return '0 Bytes';
    const k = 1024;
    const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
  };

  const formatDate = (timestamp) => {
    return new Date(timestamp).toLocaleString();
  };

  // Filter files based on search term
  const filteredFiles = files?.filter(file =>
    file.name.toLowerCase().includes(searchTerm.toLowerCase())
  ) || [];

  // Separate directories and files
  const directories = filteredFiles.filter(file => file.type === 'directory');
  const fileList = filteredFiles.filter(file => file.type === 'file');

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
        Failed to load HDFS files: {error.message}
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
              HDFS Browser
            </Typography>
            <Typography variant="body1" color="textSecondary">
              Browse and manage files in Hadoop Distributed File System
            </Typography>
          </Box>
          <Box display="flex" gap={1}>
            <IconButton onClick={handleRefresh} color="primary">
              <RefreshIcon />
            </IconButton>
            <Chip
              icon={<StorageIcon />}
              label={`${files?.length || 0} items`}
              variant="outlined"
            />
          </Box>
        </Box>
      </Paper>

      {/* Breadcrumbs */}
      <Paper sx={{ p: 2, mb: 3 }}>
        <Box display="flex" alignItems="center" gap={2}>
          <IconButton onClick={() => handleNavigate('/stock_data')} size="small">
            <HomeIcon />
          </IconButton>
          <Breadcrumbs aria-label="breadcrumb">
            {currentPath.split('/').filter(Boolean).map((segment, index, array) => {
              const path = '/' + array.slice(0, index + 1).join('/');
              return (
                <Link
                  key={path}
                  component="button"
                  underline="hover"
                  color="inherit"
                  onClick={() => handleNavigate(path)}
                  sx={{ cursor: 'pointer' }}
                >
                  {segment}
                </Link>
              );
            })}
          </Breadcrumbs>
          <Box sx={{ flexGrow: 1 }} />
          <IconButton onClick={handleGoUp} disabled={currentPath === '/'}>
            <UpIcon />
          </IconButton>
        </Box>
      </Paper>

      {/* Search */}
      <Paper sx={{ p: 2, mb: 3 }}>
        <Box display="flex" gap={2}>
          <TextField
            placeholder="Search files and folders..."
            variant="outlined"
            size="small"
            fullWidth
            value={searchTerm}
            onChange={(e) => setSearchTerm(e.target.value)}
            InputProps={{
              startAdornment: <SearchIcon sx={{ mr: 1, color: 'text.secondary' }} />,
            }}
          />
          <Button
            variant="outlined"
            onClick={() => setSearchTerm('')}
            disabled={!searchTerm}
          >
            Clear
          </Button>
        </Box>
      </Paper>

      {/* Directories */}
      {directories.length > 0 && (
        <Paper sx={{ p: 2, mb: 3 }}>
          <Typography variant="h6" gutterBottom>
            Folders ({directories.length})
          </Typography>
          <List>
            {directories.map((dir) => (
              <ListItem
                key={dir.path}
                button
                onClick={() => handleViewFile(dir)}
                sx={{
                  borderRadius: 1,
                  mb: 1,
                  '&:hover': { backgroundColor: 'rgba(33, 150, 243, 0.1)' },
                }}
              >
                <ListItemIcon>
                  <FolderIcon color="primary" />
                </ListItemIcon>
                <ListItemText
                  primary={dir.name}
                  secondary={`${formatDate(dir.modification_time)}`}
                />
                <Chip label="Directory" size="small" variant="outlined" />
              </ListItem>
            ))}
          </List>
        </Paper>
      )}

      {/* Files */}
      {fileList.length > 0 && (
        <Paper sx={{ p: 2 }}>
          <Typography variant="h6" gutterBottom>
            Files ({fileList.length})
          </Typography>
          <TableContainer>
            <Table size="small">
              <TableHead>
                <TableRow>
                  <TableCell>Name</TableCell>
                  <TableCell>Size</TableCell>
                  <TableCell>Modified</TableCell>
                  <TableCell>Type</TableCell>
                  <TableCell align="right">Actions</TableCell>
                </TableRow>
              </TableHead>
              <TableBody>
                {fileList.map((file) => (
                  <TableRow
                    key={file.path}
                    hover
                    sx={{ '&:hover': { backgroundColor: 'rgba(33, 150, 243, 0.05)' } }}
                  >
                    <TableCell>
                      <Box display="flex" alignItems="center" gap={1}>
                        <FileIcon color="action" fontSize="small" />
                        <Typography variant="body2">{file.name}</Typography>
                      </Box>
                    </TableCell>
                    <TableCell>
                      <Typography variant="body2">
                        {formatFileSize(file.size)}
                      </Typography>
                    </TableCell>
                    <TableCell>
                      <Typography variant="body2">
                        {formatDate(file.modification_time)}
                      </Typography>
                    </TableCell>
                    <TableCell>
                      <Chip
                        label={file.name.split('.').pop().toUpperCase()}
                        size="small"
                        variant="outlined"
                      />
                    </TableCell>
                    <TableCell align="right">
                      <Tooltip title="View">
                        <IconButton
                          size="small"
                          onClick={() => handleViewFile(file)}
                          sx={{ mr: 1 }}
                        >
                          <ViewIcon fontSize="small" />
                        </IconButton>
                      </Tooltip>
                      <Tooltip title="Download">
                        <IconButton
                          size="small"
                          onClick={() => handleDownload(file)}
                        >
                          <DownloadIcon fontSize="small" />
                        </IconButton>
                      </Tooltip>
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </TableContainer>
        </Paper>
      )}

      {/* Empty State */}
      {filteredFiles.length === 0 && (
        <Paper sx={{ p: 4, textAlign: 'center' }}>
          <FolderOpenIcon sx={{ fontSize: 60, color: 'text.secondary', mb: 2 }} />
          <Typography variant="h6" gutterBottom>
            No files found
          </Typography>
          <Typography variant="body2" color="textSecondary">
            {searchTerm
              ? `No files match "${searchTerm}" in ${currentPath}`
              : `Directory ${currentPath} is empty`}
          </Typography>
        </Paper>
      )}

      {/* File View Dialog */}
      <Dialog
        open={viewFileDialog}
        onClose={() => setViewFileDialog(false)}
        maxWidth="lg"
        fullWidth
      >
        <DialogTitle>
          <Box display="flex" alignItems="center" gap={1}>
            <DescriptionIcon />
            {selectedFile?.name}
          </Box>
        </DialogTitle>
        <DialogContent>
          {selectedFile?.type === 'file' ? (
            <Box sx={{ mt: 2 }}>
              <Typography variant="subtitle2" gutterBottom>
                File Information
              </Typography>
              <Grid container spacing={2} sx={{ mb: 3 }}>
                <Grid item xs={6} md={3}>
                  <Typography variant="caption" color="textSecondary">
                    Path
                  </Typography>
                  <Typography variant="body2">{selectedFile?.path}</Typography>
                </Grid>
                <Grid item xs={6} md={3}>
                  <Typography variant="caption" color="textSecondary">
                    Size
                  </Typography>
                  <Typography variant="body2">
                    {formatFileSize(selectedFile?.size)}
                  </Typography>
                </Grid>
                <Grid item xs={6} md={3}>
                  <Typography variant="caption" color="textSecondary">
                    Modified
                  </Typography>
                  <Typography variant="body2">
                    {formatDate(selectedFile?.modification_time)}
                  </Typography>
                </Grid>
                <Grid item xs={6} md={3}>
                  <Typography variant="caption" color="textSecondary">
                    Type
                  </Typography>
                  <Typography variant="body2">
                    {selectedFile?.name.split('.').pop().toUpperCase()}
                  </Typography>
                </Grid>
              </Grid>

              <Typography variant="subtitle2" gutterBottom>
                File Content (First 50 lines)
              </Typography>
              <Paper sx={{ p: 2, maxHeight: 400, overflow: 'auto' }}>
                {fileContent.length > 0 ? (
                  <TableContainer>
                    <Table size="small">
                      <TableHead>
                        <TableRow>
                          {Object.keys(fileContent[0]).map((header) => (
                            <TableCell key={header}>
                              <Typography variant="subtitle2">{header}</Typography>
                            </TableCell>
                          ))}
                        </TableRow>
                      </TableHead>
                      <TableBody>
                        {fileContent.map((row, index) => (
                          <TableRow key={index}>
                            {Object.values(row).map((value, idx) => (
                              <TableCell key={idx}>
                                <Typography variant="body2">{value}</Typography>
                              </TableCell>
                            ))}
                          </TableRow>
                        ))}
                      </TableBody>
                    </Table>
                  </TableContainer>
                ) : (
                  <Typography variant="body2" color="textSecondary" align="center">
                    No content to display
                  </Typography>
                )}
              </Paper>
            </Box>
          ) : (
            <Typography variant="body2" color="textSecondary">
              This is a directory. Click to navigate inside.
            </Typography>
          )}
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setViewFileDialog(false)}>Close</Button>
          {selectedFile?.type === 'file' && (
            <Button
              variant="contained"
              startIcon={<DownloadIcon />}
              onClick={() => handleDownload(selectedFile)}
            >
              Download
            </Button>
          )}
        </DialogActions>
      </Dialog>

      {/* HDFS Stats */}
      <Paper sx={{ p: 3, mt: 3 }}>
        <Typography variant="h6" gutterBottom>
          HDFS Statistics
        </Typography>
        <Grid container spacing={2}>
          <Grid item xs={12} md={6}>
            <Box>
              <Typography variant="subtitle2" color="primary" gutterBottom>
                Current Directory
              </Typography>
              <Typography variant="body2">{currentPath}</Typography>
              <Typography variant="caption" color="textSecondary">
                {filteredFiles.length} items ({directories.length} folders, {fileList.length} files)
              </Typography>
            </Box>
          </Grid>
          <Grid item xs={12} md={6}>
            <Box>
              <Typography variant="subtitle2" color="primary" gutterBottom>
                File Types
              </Typography>
              <Box display="flex" gap={1} flexWrap="wrap">
                {Array.from(
                  new Set(fileList.map(file => file.name.split('.').pop().toUpperCase()))
                ).map((type) => (
                  <Chip
                    key={type}
                    label={type}
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

export default HDFSBrowser;