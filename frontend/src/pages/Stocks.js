import React, { useState, useMemo } from 'react';
import {
  Box,
  Paper,
  Typography,
  TextField,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  TablePagination,
  Chip,
  IconButton,
  InputAdornment,
  LinearProgress,
  Alert,
  Tooltip,
  Button,
} from '@mui/material';
import {
  Search as SearchIcon,
  TrendingUp,
  TrendingDown,
  Visibility as ViewIcon,
  FilterList,
  Sort,
} from '@mui/icons-material';
import { useQuery } from 'react-query';
import axios from 'axios';
import { Link } from 'react-router-dom';

const API_BASE_URL = process.env.REACT_APP_API_URL || '/api/v1';

function Stocks() {
  const [searchTerm, setSearchTerm] = useState('');
  const [page, setPage] = useState(0);
  const [rowsPerPage, setRowsPerPage] = useState(10);

  const { data: stocks, isLoading, error } = useQuery('stocks', async () => {
    console.log('Fetching stocks from:', `${API_BASE_URL}/stocks`);
    try {
      const response = await axios.get(`${API_BASE_URL}/stocks`);
      console.log('Stocks data:', response.data);
      return response.data;
    } catch (err) {
      console.error('Error fetching stocks:', err);
      throw err;
    }
  });

  const filteredStocks = useMemo(() => {
    if (!stocks) return [];
    
    return stocks.filter(stock => {
      const searchLower = searchTerm.toLowerCase();
      return (
        stock.symbol.toLowerCase().includes(searchLower) ||
        (stock.name && stock.name.toLowerCase().includes(searchLower)) ||
        (stock.exchange && stock.exchange.toLowerCase().includes(searchLower)) ||
        (stock.sector && stock.sector.toLowerCase().includes(searchLower)) ||
        (stock.industry && stock.industry.toLowerCase().includes(searchLower))
      );
    });
  }, [stocks, searchTerm]);

  const paginatedStocks = filteredStocks.slice(
    page * rowsPerPage,
    page * rowsPerPage + rowsPerPage
  );

  const handleChangePage = (event, newPage) => {
    setPage(newPage);
  };

  const handleChangeRowsPerPage = (event) => {
    setRowsPerPage(parseInt(event.target.value, 10));
    setPage(0);
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
        Failed to load stocks data: {error.message}
        <Box sx={{ mt: 1, fontSize: '0.875rem' }}>
          API URL: {API_BASE_URL}/stocks
        </Box>
      </Alert>
    );
  }

  if (!stocks || stocks.length === 0) {
    return (
      <Paper sx={{ p: 4, textAlign: 'center' }}>
        <Typography variant="h6" gutterBottom>
          No Stocks Found
        </Typography>
        <Typography variant="body2" color="textSecondary" sx={{ mb: 3 }}>
          The stocks database is empty or API is not responding.
        </Typography>
        <Button
          variant="contained"
          onClick={() => window.location.reload()}
        >
          Refresh
        </Button>
      </Paper>
    );
  }

  return (
    <Box>
      {/* Header */}
      <Paper sx={{ p: 3, mb: 3 }}>
        <Typography variant="h4" gutterBottom>
          Stocks List
        </Typography>
        <Typography variant="body1" color="textSecondary">
          Browse and analyze available stocks in the database
        </Typography>
      </Paper>

      {/* Search and Stats */}
      <Paper sx={{ p: 2, mb: 3 }}>
        <Box display="flex" justifyContent="space-between" alignItems="center" flexWrap="wrap" gap={2}>
          <Box display="flex" alignItems="center" gap={2} flex={1}>
            <SearchIcon color="action" />
            <TextField
              placeholder="Search by symbol, name, exchange, sector, or industry..."
              variant="outlined"
              size="small"
              value={searchTerm}
              onChange={(e) => {
                setSearchTerm(e.target.value);
                setPage(0); // Reset to first page when searching
              }}
              sx={{ flexGrow: 1 }}
              InputProps={{
                sx: { backgroundColor: 'background.paper' }
              }}
            />
          </Box>
          <Box display="flex" gap={1}>
            <Chip
              icon={<FilterList />}
              label={`Total: ${stocks.length}`}
              color="primary"
              variant="outlined"
            />
            <Chip
              icon={<Sort />}
              label={`Showing: ${filteredStocks.length}`}
              color={filteredStocks.length < stocks.length ? "secondary" : "default"}
              variant="outlined"
            />
          </Box>
        </Box>
      </Paper>

      {/* Stocks Table */}
      <Paper sx={{ overflow: 'hidden' }}>
        <TableContainer>
          <Table>
            <TableHead>
              <TableRow sx={{ backgroundColor: 'primary.dark' }}>
                <TableCell sx={{ color: 'white', fontWeight: 'bold', width: '120px' }}>
                  Symbol
                </TableCell>
                <TableCell sx={{ color: 'white', fontWeight: 'bold' }}>
                  Company Name
                </TableCell>
                <TableCell sx={{ color: 'white', fontWeight: 'bold', width: '120px' }}>
                  Exchange
                </TableCell>
                <TableCell sx={{ color: 'white', fontWeight: 'bold', width: '200px' }}>
                  Sector
                </TableCell>
                <TableCell sx={{ color: 'white', fontWeight: 'bold', width: '200px' }}>
                  Industry
                </TableCell>
                <TableCell sx={{ color: 'white', fontWeight: 'bold', width: '100px', textAlign: 'center' }}>
                  Actions
                </TableCell>
              </TableRow>
            </TableHead>
            <TableBody>
              {paginatedStocks.length > 0 ? (
                paginatedStocks.map((stock) => (
                  <TableRow 
                    key={stock.symbol}
                    hover
                    sx={{ 
                      '&:hover': { backgroundColor: 'action.hover' },
                      '&:nth-of-type(odd)': { backgroundColor: 'background.default' }
                    }}
                  >
                    <TableCell>
                      <Link
                        to={`/stocks/${stock.symbol}`}
                        style={{
                          color: '#2196f3',
                          textDecoration: 'none',
                          fontWeight: 'bold',
                          fontSize: '1rem'
                        }}
                      >
                        {stock.symbol}
                      </Link>
                    </TableCell>
                    <TableCell>
                      <Typography variant="body1" fontWeight="medium">
                        {stock.name || 'N/A'}
                      </Typography>
                    </TableCell>
                    <TableCell>
                      <Chip 
                        label={stock.exchange || 'N/A'} 
                        size="small" 
                        color="primary" 
                        variant="filled"
                        sx={{ fontWeight: 'bold' }}
                      />
                    </TableCell>
                    <TableCell>
                      <Tooltip title={stock.sector || 'Not available'}>
                        <Chip 
                          label={stock.sector ? (stock.sector.length > 20 ? stock.sector.substring(0, 20) + '...' : stock.sector) : 'N/A'} 
                          size="small" 
                          variant="outlined"
                          sx={{ 
                            maxWidth: '180px',
                            backgroundColor: stock.sector ? 'rgba(33, 150, 243, 0.1)' : 'transparent'
                          }}
                        />
                      </Tooltip>
                    </TableCell>
                    <TableCell>
                      <Tooltip title={stock.industry || 'Not available'}>
                        <Chip 
                          label={stock.industry ? (stock.industry.length > 25 ? stock.industry.substring(0, 25) + '...' : stock.industry) : 'N/A'} 
                          size="small" 
                          variant="outlined"
                          sx={{ 
                            maxWidth: '180px',
                            backgroundColor: stock.industry ? 'rgba(76, 175, 80, 0.1)' : 'transparent'
                          }}
                        />
                      </Tooltip>
                    </TableCell>
                    <TableCell align="center">
                      <Tooltip title="View Details">
                        <IconButton
                          component={Link}
                          to={`/stocks/${stock.symbol}`}
                          size="small"
                          color="primary"
                          sx={{ 
                            backgroundColor: 'primary.light',
                            '&:hover': { backgroundColor: 'primary.main' }
                          }}
                        >
                          <ViewIcon fontSize="small" />
                        </IconButton>
                      </Tooltip>
                    </TableCell>
                  </TableRow>
                ))
              ) : (
                <TableRow>
                  <TableCell colSpan={6} align="center" sx={{ py: 4 }}>
                    <Typography variant="h6" color="textSecondary" gutterBottom>
                      No stocks found
                    </Typography>
                    <Typography variant="body2" color="textSecondary">
                      Try adjusting your search term
                    </Typography>
                  </TableCell>
                </TableRow>
              )}
            </TableBody>
          </Table>
        </TableContainer>

        {/* Pagination */}
        <TablePagination
          rowsPerPageOptions={[10, 25, 50, 100]}
          component="div"
          count={filteredStocks.length}
          rowsPerPage={rowsPerPage}
          page={page}
          onPageChange={handleChangePage}
          onRowsPerPageChange={handleChangeRowsPerPage}
          labelRowsPerPage="Rows per page:"
          labelDisplayedRows={({ from, to, count }) => 
            `${from}-${to} of ${count !== -1 ? count : `more than ${to}`}`
          }
          sx={{
            backgroundColor: 'background.paper',
            borderTop: '1px solid',
            borderColor: 'divider'
          }}
        />
      </Paper>

      {/* Summary Stats */}
      <Paper sx={{ p: 3, mt: 3 }}>
        <Typography variant="h6" gutterBottom>
          Summary Statistics
        </Typography>
        <Box display="flex" flexWrap="wrap" gap={3}>
          <Box>
            <Typography variant="body2" color="textSecondary">
              Total Stocks
            </Typography>
            <Typography variant="h5">
              {stocks.length}
            </Typography>
          </Box>
          <Box>
            <Typography variant="body2" color="textSecondary">
              Unique Exchanges
            </Typography>
            <Typography variant="h5">
              {[...new Set(stocks.map(s => s.exchange))].length}
            </Typography>
          </Box>
          <Box>
            <Typography variant="body2" color="textSecondary">
              Sectors
            </Typography>
            <Typography variant="h5">
              {[...new Set(stocks.map(s => s.sector).filter(Boolean))].length}
            </Typography>
          </Box>
          <Box>
            <Typography variant="body2" color="textSecondary">
              Currently Filtered
            </Typography>
            <Typography variant="h5">
              {filteredStocks.length}
            </Typography>
          </Box>
        </Box>
      </Paper>

      {/* API Debug Info (for development only) */}
      {process.env.NODE_ENV === 'development' && (
        <Paper sx={{ p: 2, mt: 2, backgroundColor: 'warning.light' }}>
          <Typography variant="caption" color="textSecondary">
            Debug Info: API URL: {API_BASE_URL}/stocks | 
            Stocks Count: {stocks?.length || 0} | 
            First stock: {stocks?.[0]?.symbol || 'None'}
          </Typography>
        </Paper>
      )}
    </Box>
  );
}

export default Stocks;