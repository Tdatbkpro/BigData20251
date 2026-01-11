import logging
from fastapi import APIRouter, HTTPException, Depends
from sqlalchemy.orm import Session
from typing import Dict, Any, List
import psutil
import requests
from datetime import datetime
from database import get_db
import redis

logger = logging.getLogger(__name__)
router = APIRouter()

# Prometheus configuration
PROMETHEUS_URL = "http://localhost:9090"

@router.get("/monitoring/health")
async def monitoring_health_check(db: Session = Depends(get_db)):
    """Enhanced health check for monitoring dashboard"""
    try:
        health_status = {
            "status": "healthy",
            "timestamp": datetime.now().isoformat(),
            "services": {},
            "checks": []
        }
        
        # Check PostgreSQL
        try:
            start_time = datetime.now()
            db.execute("SELECT 1")
            latency = (datetime.now() - start_time).total_seconds() * 1000
            
            # Get additional PostgreSQL stats
            result = db.execute("""
                SELECT 
                    COUNT(*) as total_stocks,
                    MAX(date) as latest_date,
                    MIN(date) as earliest_date
                FROM stock_prices
            """).first()
            
            health_status["services"]["postgresql"] = "healthy"
            health_status["checks"].append({
                "service": "PostgreSQL",
                "status": "healthy",
                "latency": f"{latency:.1f}ms",
                "details": {
                    "total_stocks": result.total_stocks if result else 0,
                    "data_latest": result.latest_date.isoformat() if result and result.latest_date else None,
                    "data_earliest": result.earliest_date.isoformat() if result and result.earliest_date else None
                }
            })
        except Exception as e:
            health_status["services"]["postgresql"] = "unhealthy"
            health_status["checks"].append({
                "service": "PostgreSQL",
                "status": "unhealthy",
                "error": str(e)
            })
            health_status["status"] = "degraded"
        
        # Check Redis
        try:
            start_time = datetime.now()
            r = redis.Redis(host='redis', port=6379, socket_connect_timeout=2)
            r.ping()
            latency = (datetime.now() - start_time).total_seconds() * 1000
            
            # Get Redis info
            info = r.info()
            
            health_status["services"]["redis"] = "healthy"
            health_status["checks"].append({
                "service": "Redis",
                "status": "healthy",
                "latency": f"{latency:.1f}ms",
                "details": {
                    "used_memory_mb": info['used_memory'] / 1024 / 1024,
                    "connected_clients": info['connected_clients'],
                    "total_keys": info.get('db0', {}).get('keys', 0)
                }
            })
        except Exception as e:
            health_status["services"]["redis"] = "unhealthy"
            health_status["checks"].append({
                "service": "Redis",
                "status": "unhealthy",
                "error": str(e)
            })
            health_status["status"] = "degraded"
        
        # Check HDFS
        try:
            start_time = datetime.now()
            from services.hdfs_service import HDFSService
            hdfs = HDFSService()
            
            if hdfs.test_connection():
                latency = (datetime.now() - start_time).total_seconds() * 1000
                
                # Get HDFS directory info
                dir_info = hdfs.get_directory_info("/stock_data")
                
                health_status["services"]["hdfs"] = "healthy"
                health_status["checks"].append({
                    "service": "HDFS",
                    "status": "healthy",
                    "latency": f"{latency:.1f}ms",
                    "details": {
                        "file_count": dir_info.get("file_count", 0),
                        "total_size_gb": round(dir_info.get("total_size", 0) / 1024 / 1024 / 1024, 2),
                        "last_modified": dir_info.get("last_modified")
                    }
                })
            else:
                health_status["services"]["hdfs"] = "unhealthy"
                health_status["checks"].append({
                    "service": "HDFS",
                    "status": "unhealthy",
                    "error": "Connection failed"
                })
                health_status["status"] = "degraded"
        except Exception as e:
            health_status["services"]["hdfs"] = "unhealthy"
            health_status["checks"].append({
                "service": "HDFS",
                "status": "unhealthy",
                "error": str(e)
            })
            health_status["status"] = "degraded"
        
        # Check Spark
        try:
            start_time = datetime.now()
            response = requests.get("http://spark-master:8080", timeout=2)
            latency = (datetime.now() - start_time).total_seconds() * 1000
            
            health_status["services"]["spark"] = "healthy"
            health_status["checks"].append({
                "service": "Spark Master",
                "status": "healthy",
                "latency": f"{latency:.1f}ms",
                "details": {
                    "web_ui": "http://spark-master:8080",
                    "master_url": "spark://spark-master:7077"
                }
            })
        except Exception as e:
            health_status["services"]["spark"] = "unhealthy"
            health_status["checks"].append({
                "service": "Spark Master",
                "status": "unhealthy",
                "error": str(e)
            })
            health_status["status"] = "degraded"
        
        # Check Prometheus
        try:
            start_time = datetime.now()
            response = requests.get(f"{PROMETHEUS_URL}/api/v1/query?query=up", timeout=2)
            latency = (datetime.now() - start_time).total_seconds() * 1000
            
            if response.status_code == 200:
                health_status["services"]["prometheus"] = "healthy"
                health_status["checks"].append({
                    "service": "Prometheus",
                    "status": "healthy",
                    "latency": f"{latency:.1f}ms"
                })
            else:
                health_status["services"]["prometheus"] = "unhealthy"
                health_status["checks"].append({
                    "service": "Prometheus",
                    "status": "unhealthy",
                    "error": f"HTTP {response.status_code}"
                })
                health_status["status"] = "degraded"
        except Exception as e:
            health_status["services"]["prometheus"] = "unhealthy"
            health_status["checks"].append({
                "service": "Prometheus",
                "status": "unhealthy",
                "error": str(e)
            })
            health_status["status"] = "degraded"
        
        # Calculate uptime percentage (simulated)
        healthy_count = len([s for s in health_status["services"].values() if s == "healthy"])
        total_count = len(health_status["services"])
        uptime_percentage = (healthy_count / total_count) * 100 if total_count > 0 else 0
        
        health_status["uptime"] = f"{uptime_percentage:.1f}%"
        health_status["healthy_services"] = f"{healthy_count}/{total_count}"
        
        return health_status
        
    except Exception as e:
        logger.error(f"Health check failed: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/monitoring/metrics/system")
async def get_system_metrics():
    """Get system metrics (CPU, Memory, Disk)"""
    try:
        # Get CPU usage
        cpu_percent = psutil.cpu_percent(interval=1)
        
        # Get memory usage
        memory = psutil.virtual_memory()
        memory_percent = memory.percent
        
        # Get disk usage
        disk = psutil.disk_usage('/')
        disk_percent = disk.percent
        
        # Get network stats
        net_io = psutil.net_io_counters()
        network_in_mb = net_io.bytes_recv / 1024 / 1024
        network_out_mb = net_io.bytes_sent / 1024 / 1024
        
        # Get system uptime
        boot_time = psutil.boot_time()
        uptime_seconds = datetime.now().timestamp() - boot_time
        uptime_days = uptime_seconds / 86400
        
        # Get load average
        load_avg = psutil.getloadavg()
        
        # Get process count
        process_count = len(psutil.pids())
        
        return {
            "cpu": {
                "percent": round(cpu_percent, 2),
                "cores": psutil.cpu_count(),
                "load_avg": [round(load, 2) for load in load_avg]
            },
            "memory": {
                "percent": round(memory_percent, 2),
                "total_gb": round(memory.total / 1024 / 1024 / 1024, 2),
                "used_gb": round(memory.used / 1024 / 1024 / 1024, 2),
                "available_gb": round(memory.available / 1024 / 1024 / 1024, 2)
            },
            "disk": {
                "percent": round(disk_percent, 2),
                "total_gb": round(disk.total / 1024 / 1024 / 1024, 2),
                "used_gb": round(disk.used / 1024 / 1024 / 1024, 2),
                "free_gb": round(disk.free / 1024 / 1024 / 1024, 2)
            },
            "network": {
                "in_mb": round(network_in_mb, 2),
                "out_mb": round(network_out_mb, 2)
            },
            "system": {
                "uptime_days": round(uptime_days, 2),
                "process_count": process_count,
                "boot_time": datetime.fromtimestamp(boot_time).isoformat()
            },
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Failed to get system metrics: {str(e)}")
        # Return safe defaults
        return {
            "cpu": {"percent": 0, "cores": 0, "load_avg": [0, 0, 0]},
            "memory": {"percent": 0, "total_gb": 0, "used_gb": 0, "available_gb": 0},
            "disk": {"percent": 0, "total_gb": 0, "used_gb": 0, "free_gb": 0},
            "network": {"in_mb": 0, "out_mb": 0},
            "system": {"uptime_days": 0, "process_count": 0, "boot_time": datetime.now().isoformat()},
            "timestamp": datetime.now().isoformat()
        }

@router.get("/monitoring/metrics/services")
async def get_service_metrics(db: Session = Depends(get_db)):
    """Get service-specific metrics"""
    try:
        services = []
        
        # PostgreSQL metrics
        try:
            # Get connection count
            result = db.execute("SELECT count(*) FROM pg_stat_activity").scalar()
            connections = result if result else 0
            
            # Get database size
            result = db.execute("SELECT pg_database_size('stockdb')").scalar()
            db_size_mb = result / 1024 / 1024 if result else 0
            
            # Get table statistics
            result = db.execute("""
                SELECT 
                    COUNT(DISTINCT symbol) as unique_symbols,
                    COUNT(*) as total_records,
                    MAX(date) as latest_record
                FROM stock_prices
            """).first()
            
            services.append({
                "name": "PostgreSQL",
                "status": "healthy",
                "type": "database",
                "metrics": {
                    "connections": connections,
                    "database_size_mb": round(db_size_mb, 2),
                    "unique_symbols": result.unique_symbols if result else 0,
                    "total_records": result.total_records if result else 0,
                    "latest_record": result.latest_record.isoformat() if result and result.latest_record else None
                }
            })
        except Exception as e:
            services.append({
                "name": "PostgreSQL",
                "status": "unhealthy",
                "type": "database",
                "error": str(e)
            })
        
        # Redis metrics
        try:
            r = redis.Redis(host='redis', port=6379, socket_connect_timeout=2)
            info = r.info()
            
            services.append({
                "name": "Redis",
                "status": "healthy",
                "type": "cache",
                "metrics": {
                    "used_memory_mb": round(info['used_memory'] / 1024 / 1024, 2),
                    "connected_clients": info['connected_clients'],
                    "total_keys": info.get('db0', {}).get('keys', 0),
                    "hits": info.get('keyspace_hits', 0),
                    "misses": info.get('keyspace_misses', 0),
                    "hit_rate": round(info.get('keyspace_hits', 0) / max(1, info.get('keyspace_hits', 0) + info.get('keyspace_misses', 0)) * 100, 2)
                }
            })
        except Exception as e:
            services.append({
                "name": "Redis",
                "status": "unhealthy",
                "type": "cache",
                "error": str(e)
            })
        
        # HDFS metrics
        try:
            from services.hdfs_service import HDFSService
            hdfs = HDFSService()
            
            if hdfs.test_connection():
                dir_info = hdfs.get_directory_info("/stock_data")
                
                services.append({
                    "name": "HDFS",
                    "status": "healthy",
                    "type": "storage",
                    "metrics": {
                        "file_count": dir_info.get("file_count", 0),
                        "total_size_gb": round(dir_info.get("total_size", 0) / 1024 / 1024 / 1024, 2),
                        "last_modified": dir_info.get("last_modified"),
                        "directory_count": dir_info.get("directory_count", 0)
                    }
                })
            else:
                services.append({
                    "name": "HDFS",
                    "status": "unhealthy",
                    "type": "storage",
                    "error": "Connection failed"
                })
        except Exception as e:
            services.append({
                "name": "HDFS",
                "status": "unhealthy",
                "type": "storage",
                "error": str(e)
            })
        
        # Spark metrics (via Prometheus if available)
        try:
            response = requests.get(f"{PROMETHEUS_URL}/api/v1/query", params={
                "query": 'spark_driver_BlockManager_disk_diskSpaceUsed_MB_value'
            }, timeout=2)
            
            spark_metrics = {}
            if response.status_code == 200:
                data = response.json()
                if data['data']['result']:
                    spark_metrics['disk_used_mb'] = float(data['data']['result'][0]['value'][1])
            
            services.append({
                "name": "Spark",
                "status": "healthy",
                "type": "processing",
                "metrics": spark_metrics
            })
        except:
            # Fallback to basic check
            try:
                response = requests.get("http://spark-master:8080", timeout=2)
                services.append({
                    "name": "Spark",
                    "status": "healthy",
                    "type": "processing",
                    "metrics": {"web_ui": "accessible"}
                })
            except:
                services.append({
                    "name": "Spark",
                    "status": "unhealthy",
                    "type": "processing",
                    "error": "Cannot connect to Spark"
                })
        
        # Data Ingestion service
        try:
            # Check if data ingestion is running
            import subprocess
            result = subprocess.run(
                ["docker", "ps", "--filter", "name=data-ingestion", "--format", "{{.Status}}"],
                capture_output=True,
                text=True
            )
            
            is_running = "Up" in result.stdout
            
            services.append({
                "name": "Data Ingestion",
                "status": "healthy" if is_running else "warning",
                "type": "data_pipeline",
                "metrics": {
                    "running": is_running,
                    "status": result.stdout.strip() if result.stdout else "unknown"
                }
            })
        except Exception as e:
            services.append({
                "name": "Data Ingestion",
                "status": "unhealthy",
                "type": "data_pipeline",
                "error": str(e)
            })
        
        return {
            "services": services,
            "timestamp": datetime.now().isoformat(),
            "summary": {
                "total": len(services),
                "healthy": len([s for s in services if s.get("status") == "healthy"]),
                "unhealthy": len([s for s in services if s.get("status") == "unhealthy"]),
                "warning": len([s for s in services if s.get("status") == "warning"])
            }
        }
        
    except Exception as e:
        logger.error(f"Failed to get service metrics: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/monitoring/alerts")
async def get_monitoring_alerts(active: bool = True):
    """Get system alerts based on current metrics"""
    try:
        alerts = []
        
        # Get current metrics
        system_metrics = await get_system_metrics()
        service_response = await get_service_metrics()
        services = service_response.get("services", [])
        
        # System resource alerts
        cpu_percent = system_metrics["cpu"]["percent"]
        memory_percent = system_metrics["memory"]["percent"]
        disk_percent = system_metrics["disk"]["percent"]
        
        # CPU alerts
        if cpu_percent > 90:
            alerts.append({
                "id": "cpu_critical",
                "severity": "critical",
                "service": "System",
                "message": f"CPU usage critically high: {cpu_percent:.1f}%",
                "time": datetime.now().isoformat(),
                "resolved": False,
                "metric": "cpu",
                "value": cpu_percent,
                "threshold": 90
            })
        elif cpu_percent > 80:
            alerts.append({
                "id": "cpu_warning",
                "severity": "warning",
                "service": "System",
                "message": f"CPU usage high: {cpu_percent:.1f}%",
                "time": datetime.now().isoformat(),
                "resolved": False,
                "metric": "cpu",
                "value": cpu_percent,
                "threshold": 80
            })
        
        # Memory alerts
        if memory_percent > 95:
            alerts.append({
                "id": "memory_critical",
                "severity": "critical",
                "service": "System",
                "message": f"Memory usage critically high: {memory_percent:.1f}%",
                "time": datetime.now().isoformat(),
                "resolved": False,
                "metric": "memory",
                "value": memory_percent,
                "threshold": 95
            })
        elif memory_percent > 85:
            alerts.append({
                "id": "memory_warning",
                "severity": "warning",
                "service": "System",
                "message": f"Memory usage high: {memory_percent:.1f}%",
                "time": datetime.now().isoformat(),
                "resolved": False,
                "metric": "memory",
                "value": memory_percent,
                "threshold": 85
            })
        
        # Disk alerts
        if disk_percent > 95:
            alerts.append({
                "id": "disk_critical",
                "severity": "critical",
                "service": "System",
                "message": f"Disk usage critically high: {disk_percent:.1f}%",
                "time": datetime.now().isoformat(),
                "resolved": False,
                "metric": "disk",
                "value": disk_percent,
                "threshold": 95
            })
        elif disk_percent > 90:
            alerts.append({
                "id": "disk_warning",
                "severity": "warning",
                "service": "System",
                "message": f"Disk usage high: {disk_percent:.1f}%",
                "time": datetime.now().isoformat(),
                "resolved": False,
                "metric": "disk",
                "value": disk_percent,
                "threshold": 90
            })
        
        # Service health alerts
        for service in services:
            if service.get("status") == "unhealthy":
                alerts.append({
                    "id": f"service_{service['name'].lower().replace(' ', '_')}",
                    "severity": "critical",
                    "service": service["name"],
                    "message": f"{service['name']} is unhealthy",
                    "time": datetime.now().isoformat(),
                    "resolved": False,
                    "error": service.get("error", "Unknown error")
                })
            elif service.get("status") == "warning":
                alerts.append({
                    "id": f"service_{service['name'].lower().replace(' ', '_')}_warning",
                    "severity": "warning",
                    "service": service["name"],
                    "message": f"{service['name']} has warnings",
                    "time": datetime.now().isoformat(),
                    "resolved": False
                })
        
        # Filter resolved alerts if requested
        if active:
            alerts = [alert for alert in alerts if not alert.get("resolved", False)]
        
        return {
            "alerts": alerts,
            "total": len(alerts),
            "active": len([a for a in alerts if not a.get("resolved", False)]),
            "summary": {
                "critical": len([a for a in alerts if a.get("severity") == "critical"]),
                "warning": len([a for a in alerts if a.get("severity") == "warning"]),
                "info": len([a for a in alerts if a.get("severity") == "info"])
            },
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Failed to get alerts: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/monitoring/performance/history")
async def get_performance_history(hours: int = 24, interval_minutes: int = 5):
    """Get performance history data"""
    try:
        # In production, this would query a time-series database
        # For now, generate sample data
        
        import random
        from datetime import datetime, timedelta
        
        history = []
        now = datetime.now()
        data_points = (hours * 60) // interval_minutes
        
        for i in range(data_points):
            timestamp = now - timedelta(minutes=i * interval_minutes)
            
            # Generate realistic data patterns
            base_cpu = 30 + 10 * (i % 24) / 24  # Daily pattern
            base_memory = 40 + 5 * (i % 12) / 12  # Hourly pattern
            
            history.append({
                "timestamp": timestamp.isoformat(),
                "cpu": round(base_cpu + random.uniform(-5, 5), 2),
                "memory": round(base_memory + random.uniform(-3, 3), 2),
                "disk": round(25 + random.uniform(-2, 2), 2),
                "requests": int(1500 + random.uniform(-500, 500)),
                "response_time": round(80 + random.uniform(-20, 20), 2),
                "network_in": round(100 + random.uniform(-30, 30), 2),
                "network_out": round(70 + random.uniform(-20, 20), 2)
            })
        
        # Sort by timestamp (newest first)
        history.sort(key=lambda x: x["timestamp"], reverse=True)
        
        return {
            "history": history,
            "period_hours": hours,
            "interval_minutes": interval_minutes,
            "data_points": len(history),
            "timestamp": now.isoformat()
        }
        
    except Exception as e:
        logger.error(f"Failed to get performance history: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/monitoring/status")
async def get_monitoring_status():
    """Get comprehensive monitoring status"""
    try:
        # Gather all monitoring data
        import asyncio
        
        tasks = [
            get_system_metrics(),
            get_service_metrics(),
            get_monitoring_alerts(active=True),
            get_performance_history(hours=1, interval_minutes=1)
        ]
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        system_metrics = results[0] if not isinstance(results[0], Exception) else {}
        service_metrics = results[1] if not isinstance(results[1], Exception) else {}
        alerts = results[2] if not isinstance(results[2], Exception) else {}
        performance = results[3] if not isinstance(results[3], Exception) else {}
        
        # Determine overall status
        active_alerts = len(alerts.get("alerts", []))
        critical_alerts = len([a for a in alerts.get("alerts", []) if a.get("severity") == "critical"])
        
        if critical_alerts > 0:
            overall_status = "critical"
        elif active_alerts > 0:
            overall_status = "warning"
        else:
            overall_status = "healthy"
        
        # Get uptime from system metrics
        uptime_days = system_metrics.get("system", {}).get("uptime_days", 0)
        
        # Calculate service health
        services = service_metrics.get("services", [])
        healthy_services = len([s for s in services if s.get("status") == "healthy"])
        total_services = len(services)
        
        return {
            "status": overall_status,
            "uptime_days": uptime_days,
            "timestamp": datetime.now().isoformat(),
            "summary": {
                "cpu_usage": system_metrics.get("cpu", {}).get("percent", 0),
                "memory_usage": system_metrics.get("memory", {}).get("percent", 0),
                "disk_usage": system_metrics.get("disk", {}).get("percent", 0),
                "active_alerts": active_alerts,
                "healthy_services": healthy_services,
                "total_services": total_services,
                "service_health_percentage": round((healthy_services / total_services * 100) if total_services > 0 else 0, 1)
            },
            "system": system_metrics,
            "services": service_metrics,
            "alerts": alerts.get("alerts", []),
            "performance": performance.get("history", [])[:10]  # Last 10 data points
        }
        
    except Exception as e:
        logger.error(f"Failed to get monitoring status: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))