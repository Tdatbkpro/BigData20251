import React, { useState } from 'react';
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
  Grid,
  Tabs,
  Tab,
  CircularProgress
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
import Papa from 'papaparse';

function HDFSBrowser() {
  const [currentPath, setCurrentPath] = useState('/stock_data');
  const [searchTerm, setSearchTerm] = useState('');
  const [viewFileDialog, setViewFileDialog] = useState(false);
  const [selectedFile, setSelectedFile] = useState(null);
  const [fileContent, setFileContent] = useState('');
  const [csvData, setCsvData] = useState([]);
  const [csvHeaders, setCsvHeaders] = useState([]);
  const [previewTab, setPreviewTab] = useState(0);
  const [isLoadingContent, setIsLoadingContent] = useState(false);

  // Fetch HDFS directory listing qua backend API
  const { data: files, isLoading, error, refetch } = useQuery(
    ['hdfsFiles', currentPath],
    async () => {
      try {
        console.log('Fetching HDFS directory:', currentPath);
        const response = await axios.get('/api/v1/hdfs/files', {
          params: { path: currentPath }
        });
        
        console.log('HDFS response:', response.data);
        
        if (Array.isArray(response.data)) {
          // Sort: directories first, then files, then alphabetically
          return response.data.sort((a, b) => {
            if (a.type === 'directory' && b.type === 'file') return -1;
            if (a.type === 'file' && b.type === 'directory') return 1;
            return a.name.localeCompare(b.name);
          });
        }
        return [];
      } catch (err) {
        console.error('Error fetching HDFS directory:', err);
        throw err;
      }
    },
    { 
      retry: 1,
      onError: (err) => {
        toast.error(`Failed to load HDFS: ${err.message}`);
      }
    }
  );

  // Fetch file content qua backend API
  // Fetch file content qua backend API
const fetchFileContent = async (filePath) => {
  setIsLoadingContent(true);
  setFileContent('');
  setCsvData([]);
  setCsvHeaders([]);
  
  try {
    const fileType = filePath.split('.').pop().toLowerCase();
    console.log('🔍 Fetching file:', filePath, 'Type:', fileType);
    
    const response = await axios.get('/api/v1/hdfs/read', {
      params: { 
        file_path: filePath,
        limit: 100 
      }
    });
    
    console.log('📦 File response:', response.data);
    
    // Lấy content - có thể là string hoặc array
    let content = response.data.content || '';
    
    // Nếu content là array (list of objects), convert thành CSV string
    if (Array.isArray(content)) {
      console.log('📊 Content is array, converting...');
      if (content.length > 0 && typeof content[0] === 'object') {
        // Array of objects -> CSV
        const headers = Object.keys(content[0]);
        const csvRows = [
          headers.join(','), // header row
          ...content.map(row => headers.map(header => row[header]).join(','))
        ];
        content = csvRows.join('\n');
        console.log('✅ Converted array to CSV');
      } else {
        // Array of strings
        content = content.join('\n');
      }
    }
    
    console.log('📝 Final content (first 200 chars):', content.substring(0, 200));
    
    if (fileType === 'csv') {
      try {
        console.log('📈 Parsing CSV...');
        const parsed = Papa.parse(content, {
          header: true,
          skipEmptyLines: true,
          preview: 100,
          dynamicTyping: true,
        });
        
        console.log('✅ CSV parse result:', {
          data: parsed.data,
          fields: parsed.meta?.fields,
          errors: parsed.errors
        });
        
        if (parsed.data && parsed.data.length > 0) {
          setCsvData(parsed.data.slice(0, 50));
          if (parsed.meta.fields) {
            setCsvHeaders(parsed.meta.fields);
          } else if (parsed.data[0]) {
            setCsvHeaders(Object.keys(parsed.data[0]));
          }
          setFileContent(`CSV loaded: ${parsed.data.length} rows`);
          console.log('🎉 CSV data set:', parsed.data.length, 'rows');
        } else {
          console.log('⚠️ No CSV data, showing as text');
          setFileContent(content.slice(0, 50000));
        }
      } catch (parseError) {
        console.error('❌ CSV parse failed:', parseError);
        console.error('Parse error details:', parseError.message);
        setFileContent(content.slice(0, 50000));
      }
    } else {
      console.log('📄 Showing as plain text');
      setFileContent(content.slice(0, 50000));
    }
  } catch (error) {
    console.error('💥 Error fetching file content:', error);
    console.error('Error response:', error.response);
    console.error('Error message:', error.message);
    setFileContent(`Error: ${error.message}\n\nCheck console for details.`);
  } finally {
    setIsLoadingContent(false);
    console.log('🏁 fetchFileContent completed');
  }
};

  const handleNavigate = (path) => {
    setCurrentPath(path);
    setSearchTerm('');
  };

  const handleGoUp = () => {
    const parentPath = currentPath.substring(0, currentPath.lastIndexOf('/'));
    if (parentPath === '' || parentPath === '/') {
      setCurrentPath('/');
    } else {
      setCurrentPath(parentPath);
    }
  };

  const handleRefresh = () => {
    refetch();
    toast.success('Directory refreshed');
  };

  const handleViewFile = async (file) => {
    if (file.type === 'file') {
      setSelectedFile(file);
      setViewFileDialog(true);
      setPreviewTab(0);
      await fetchFileContent(file.path);
    } else {
      handleNavigate(file.path);
    }
  };

  const handleDownload = (file) => {
  console.log('📥 Download:', file.name);
  
  // Tạo download URL
  const downloadUrl = `/api/v1/hdfs/download?file_path=${encodeURIComponent(file.path)}`;
  
  // Cách đơn giản nhất: tạo link và click
  const link = document.createElement('a');
  link.href = downloadUrl;
  link.download = file.name;  // Quan trọng: thêm thuộc tính download
  link.target = '_blank';     // Mở trong tab mới nếu cần
  link.rel = 'noopener noreferrer';
  
  // Thêm vào DOM và click
  document.body.appendChild(link);
  link.click();
  
  // Dọn dẹp sau 1 giây
  setTimeout(() => {
    document.body.removeChild(link);
  }, 1000);
  
  console.log('✅ Link clicked, URL:', downloadUrl);
  toast.success(`Download started: ${file.name}`);
};



  const formatFileSize = (bytes) => {
    if (!bytes || bytes === 0) return '0 Bytes';
    const k = 1024;
    const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
  };

  const formatDate = (timestamp) => {
    if (!timestamp) return 'Unknown';
    return new Date(timestamp).toLocaleString();
  };

  const filteredFiles = files?.filter(file =>
    file.name.toLowerCase().includes(searchTerm.toLowerCase())
  ) || [];

  const directories = filteredFiles.filter(file => file.type === 'directory');
  const fileList = filteredFiles.filter(file => file.type === 'file');

  const renderFilePreview = () => {
    if (isLoadingContent) {
      return (
        <Box sx={{ display: 'flex', flexDirection: 'column', alignItems: 'center', py: 4 }}>
          <CircularProgress />
          <Typography variant="body2" color="textSecondary" sx={{ mt: 2 }}>
            Loading file content...
          </Typography>
        </Box>
      );
    }

    if (csvData.length > 0) {
      return (
        <Box sx={{ maxHeight: 400, overflow: 'auto' }}>
          <Typography variant="subtitle2" color="textSecondary" gutterBottom>
            CSV Preview ({csvData.length} rows)
          </Typography>
          <TableContainer component={Paper} variant="outlined" sx={{ maxHeight: 350 }}>
            <Table size="small" stickyHeader>
              <TableHead>
                <TableRow>
                  {csvHeaders.map((header, index) => (
                    <TableCell key={index} sx={{ fontWeight: 'bold', bgcolor: 'background.default' }}>
                      {header}
                    </TableCell>
                  ))}
                </TableRow>
              </TableHead>
              <TableBody>
                {csvData.map((row, rowIndex) => (
                  <TableRow key={rowIndex} hover>
                    {csvHeaders.map((header, colIndex) => (
                      <TableCell key={colIndex}>
                        <Typography variant="body2" sx={{ wordBreak: 'break-all', fontSize: '12px' }}>
                          {String(row[header] || '')}
                        </Typography>
                      </TableCell>
                    ))}
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </TableContainer>
        </Box>
      );
    }

    if (fileContent) {
      return (
        <Box sx={{ maxHeight: 400, overflow: 'auto' }}>
          <Typography variant="subtitle2" color="textSecondary" gutterBottom>
            File Preview
          </Typography>
          <Paper sx={{ p: 2, bgcolor: '#f5f5f5', fontFamily: 'monospace', fontSize: '12px' }}>
            <pre style={{ margin: 0, whiteSpace: 'pre-wrap' }}>
              {fileContent}
            </pre>
          </Paper>
        </Box>
      );
    }

    return (
      <Box sx={{ textAlign: 'center', py: 4 }}>
        <Typography variant="body2" color="textSecondary">
          No preview available
        </Typography>
      </Box>
    );
  };

  if (isLoading) {
    return (
      <Box sx={{ width: '100%', p: 3 }}>
        <LinearProgress />
        <Typography variant="body2" align="center" sx={{ mt: 2 }}>
          Loading HDFS directory: {currentPath}
        </Typography>
      </Box>
    );
  }

  if (error) {
    return (
      <Alert severity="error" sx={{ mt: 2, mx: 2 }}>
        <Typography variant="subtitle1" gutterBottom>
          Cannot connect to HDFS
        </Typography>
        <Typography variant="body2" gutterBottom>
          {error.message}
        </Typography>
        <Typography variant="body2">
          Please ensure:
        </Typography>
        <ul>
          <li>Backend API is running on port 8000</li>
          <li>HDFS service is available</li>
          <li>Proxy is configured correctly</li>
        </ul>
        <Button 
          variant="contained" 
          size="small" 
          sx={{ mt: 1 }}
          onClick={() => refetch()}
        >
          Retry
        </Button>
      </Alert>
    );
  }

  return (
    <Box sx={{ p: 2 }}>
      {/* Header */}
      <Paper sx={{ p: 3, mb: 3 }}>
        <Box display="flex" justifyContent="space-between" alignItems="center" flexWrap="wrap" gap={2}>
          <Box>
            <Typography variant="h4" gutterBottom>
              HDFS File Browser
            </Typography>
            <Typography variant="body2" color="textSecondary">
              Path: {currentPath}
            </Typography>
          </Box>
          <Box display="flex" alignItems="center" gap={1}>
            <Chip
              icon={<StorageIcon />}
              label={`${files?.length || 0} items`}
              variant="outlined"
              size="small"
            />
            <Tooltip title="Refresh">
              <IconButton onClick={handleRefresh} size="small">
                <RefreshIcon />
              </IconButton>
            </Tooltip>
          </Box>
        </Box>
      </Paper>

      {/* Navigation */}
      <Paper sx={{ p: 2, mb: 3 }}>
        <Box display="flex" alignItems="center" gap={2}>
          <Tooltip title="Root">
            <IconButton onClick={() => handleNavigate('/')} size="small">
              <HomeIcon />
            </IconButton>
          </Tooltip>
          <Breadcrumbs aria-label="breadcrumb" sx={{ flexGrow: 1 }}>
            {currentPath.split('/').filter(Boolean).map((segment, index, array) => {
              const path = '/' + array.slice(0, index + 1).join('/');
              return (
                <Link
                  key={path}
                  component="button"
                  underline="hover"
                  color="inherit"
                  onClick={() => handleNavigate(path)}
                  sx={{ cursor: 'pointer', fontSize: '14px' }}
                >
                  {segment}
                </Link>
              );
            })}
          </Breadcrumbs>
          <Tooltip title="Go Up">
            <span>
              <IconButton 
                onClick={handleGoUp} 
                disabled={currentPath === '/'}
                size="small"
              >
                <UpIcon />
              </IconButton>
            </span>
          </Tooltip>
        </Box>
      </Paper>

      {/* Search */}
      <Paper sx={{ p: 2, mb: 3 }}>
        <Box display="flex" gap={2} alignItems="center">
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
          {searchTerm && (
            <Button
              variant="text"
              size="small"
              onClick={() => setSearchTerm('')}
            >
              Clear
            </Button>
          )}
        </Box>
      </Paper>

      {/* Directories */}
      {directories.length > 0 && (
        <Paper sx={{ p: 2, mb: 3 }}>
          <Typography variant="subtitle1" gutterBottom>
            📁 Folders ({directories.length})
          </Typography>
          <List dense>
            {directories.map((dir) => (
              <ListItem
                key={dir.path}
                button
                onClick={() => handleNavigate(dir.path)}
                sx={{
                  borderRadius: 1,
                  mb: 0.5,
                  '&:hover': { bgcolor: 'action.hover' },
                }}
              >
                <ListItemIcon sx={{ minWidth: 40 }}>
                  <FolderIcon color="primary" fontSize="small" />
                </ListItemIcon>
                <ListItemText
                  primary={dir.name}
                  secondary={`${formatDate(dir.modification_time)} • ${formatFileSize(dir.size)}`}
                  primaryTypographyProps={{ variant: 'body2' }}
                  secondaryTypographyProps={{ variant: 'caption' }}
                />
              </ListItem>
            ))}
          </List>
        </Paper>
      )}

      {/* Files */}
      {fileList.length > 0 && (
        <Paper sx={{ p: 2 }}>
          <Typography variant="subtitle1" gutterBottom>
            📄 Files ({fileList.length})
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
                    sx={{ '&:hover': { bgcolor: 'action.hover' } }}
                  >
                    <TableCell>
                      <Box display="flex" alignItems="center" gap={1}>
                        <FileIcon fontSize="small" color="action" />
                        <Typography variant="body2">
                          {file.name}
                        </Typography>
                        {file.name.endsWith('.csv') && (
                          <Chip label="CSV" size="small" color="primary" variant="outlined" />
                        )}
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
                        label={file.name.split('.').pop().toUpperCase() || 'FILE'}
                        size="small"
                        variant="outlined"
                      />
                    </TableCell>
                    <TableCell align="right">
                      <Tooltip title="Preview">
                        <IconButton
                          size="small"
                          onClick={() => handleViewFile(file)}
                          sx={{ mr: 0.5 }}
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

      {filteredFiles.length === 0 && !isLoading && (
        <Paper sx={{ p: 4, textAlign: 'center' }}>
          <FolderOpenIcon sx={{ fontSize: 60, color: 'text.secondary', mb: 2 }} />
          <Typography variant="h6" gutterBottom>
            {searchTerm ? 'No matching files' : 'Empty directory'}
          </Typography>
          <Typography variant="body2" color="textSecondary">
            {searchTerm
              ? `No files match "${searchTerm}" in ${currentPath}`
              : `Directory ${currentPath} is empty`}
          </Typography>
        </Paper>
      )}

      {/* File Preview Dialog */}
      <Dialog
        open={viewFileDialog}
        onClose={() => setViewFileDialog(false)}
        maxWidth="lg"
        fullWidth
        PaperProps={{ sx: { minHeight: '60vh' } }}
      >
        {selectedFile && (
          <>
            <DialogTitle>
              <Box display="flex" justifyContent="space-between" alignItems="center">
                <Box display="flex" alignItems="center" gap={1}>
                  <DescriptionIcon />
                  <Box>
                    <Typography variant="h6">{selectedFile.name}</Typography>
                    <Typography variant="caption" color="textSecondary">
                      {selectedFile.path}
                    </Typography>
                  </Box>
                </Box>
                <Chip
                  label={selectedFile.name.split('.').pop().toUpperCase() || 'FILE'}
                  size="small"
                  color="primary"
                  variant="outlined"
                />
              </Box>
            </DialogTitle>
            
            <DialogContent dividers>
              <Tabs value={previewTab} onChange={(e, newValue) => setPreviewTab(newValue)} sx={{ mb: 2 }}>
                <Tab label="Preview" />
                <Tab label="Details" />
              </Tabs>
              
              {previewTab === 0 ? (
                renderFilePreview()
              ) : (
                <Grid container spacing={2}>
                  <Grid item xs={12}>
                    <Typography variant="caption" color="textSecondary">Path</Typography>
                    <Typography variant="body2" sx={{ wordBreak: 'break-all' }}>
                      {selectedFile.path}
                    </Typography>
                  </Grid>
                  <Grid item xs={6}>
                    <Typography variant="caption" color="textSecondary">Size</Typography>
                    <Typography variant="body2">{formatFileSize(selectedFile.size)}</Typography>
                  </Grid>
                  <Grid item xs={6}>
                    <Typography variant="caption" color="textSecondary">Modified</Typography>
                    <Typography variant="body2">{formatDate(selectedFile.modification_time)}</Typography>
                  </Grid>
                  <Grid item xs={6}>
                    <Typography variant="caption" color="textSecondary">Type</Typography>
                    <Typography variant="body2">
                      {selectedFile.name.split('.').pop().toUpperCase()}
                    </Typography>
                  </Grid>
                  <Grid item xs={6}>
                    <Typography variant="caption" color="textSecondary">Owner</Typography>
                    <Typography variant="body2">{selectedFile.owner || 'Unknown'}</Typography>
                  </Grid>
                </Grid>
              )}
            </DialogContent>
            
            <DialogActions>
              <Button onClick={() => setViewFileDialog(false)}>Close</Button>
              <Button
                variant="contained"
                startIcon={<DownloadIcon />}
                onClick={() => handleDownload(selectedFile)}
              >
                Download
              </Button>
            </DialogActions>
          </>
        )}
      </Dialog>
    </Box>
  );
}

export default HDFSBrowser;