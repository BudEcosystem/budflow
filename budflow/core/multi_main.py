"""Multi-Main high availability setup for BudFlow."""

import asyncio
import hashlib
import json
import logging
import time
from datetime import datetime, timezone, timedelta
from enum import Enum
from typing import Dict, List, Optional, Set, Any, Callable
from uuid import uuid4

try:
    import consul
except ImportError:
    consul = None

try:
    import redis.asyncio as redis
    from redis.exceptions import ConnectionError as RedisConnectionError
except ImportError:
    redis = None
    RedisConnectionError = Exception

try:
    import psutil
except ImportError:
    psutil = None

from pydantic import BaseModel, Field, ConfigDict

logger = logging.getLogger(__name__)


class NodeStatus(str, Enum):
    """Node status enumeration."""
    
    HEALTHY = "healthy"
    UNHEALTHY = "unhealthy"
    STARTING = "starting"
    STOPPING = "stopping"
    DISCONNECTED = "disconnected"
    OVERLOADED = "overloaded"


class MultiMainConfig(BaseModel):
    """Configuration for Multi-Main high availability setup."""
    
    node_id: str
    instance_name: str = "budflow-main"
    redis_url: str = "redis://localhost:6379"
    consul_url: str = "http://localhost:8500"
    host: str = "127.0.0.1"
    port: int = 8000
    region: str = "us-east-1"
    environment: str = "production"
    
    # Health monitoring
    health_check_interval: int = 30  # seconds
    heartbeat_interval: int = 15  # seconds
    max_missed_heartbeats: int = 3
    
    # Leader election
    leader_election_ttl: int = 60  # seconds
    election_timeout: int = 30  # seconds
    
    # Load balancing
    webhook_routing_strategy: str = "consistent_hash"  # or "load_balance"
    max_cpu_threshold: float = 90.0
    max_memory_threshold: float = 85.0
    max_queue_size: int = 100
    
    # State management
    enable_state_replication: bool = True
    state_replication_ttl: int = 3600  # 1 hour
    
    # Split-brain prevention
    max_split_brain_timeout: int = 120  # seconds
    quorum_size: int = 2  # minimum nodes for decisions
    
    # Capabilities
    capabilities: List[str] = Field(default_factory=lambda: ["webhooks", "executions", "schedules"])
    version: str = "1.0.0"
    
    model_config = ConfigDict(arbitrary_types_allowed=True)


class NodeInfo(BaseModel):
    """Information about a cluster node."""
    
    node_id: str
    instance_name: str
    host: str
    port: int
    region: str
    environment: str
    status: NodeStatus
    last_heartbeat: datetime
    version: str
    capabilities: List[str] = Field(default_factory=list)
    load_metrics: Dict[str, Any] = Field(default_factory=dict)
    
    def is_healthy(self) -> bool:
        """Check if the node is healthy."""
        return self.status == NodeStatus.HEALTHY
    
    def is_overloaded(self) -> bool:
        """Check if the node is overloaded."""
        metrics = self.load_metrics
        return (
            metrics.get("cpu_percent", 0) > 90 or
            metrics.get("memory_percent", 0) > 85 or
            metrics.get("queue_size", 0) > 100
        )
    
    def get_load_score(self) -> float:
        """Calculate load score for load balancing."""
        metrics = self.load_metrics
        cpu_score = metrics.get("cpu_percent", 0) / 100.0
        memory_score = metrics.get("memory_percent", 0) / 100.0
        queue_score = min(metrics.get("queue_size", 0) / 100.0, 1.0)
        
        return (cpu_score * 0.4) + (memory_score * 0.3) + (queue_score * 0.3)
    
    model_config = ConfigDict(from_attributes=True)


class LeaderElectionError(Exception):
    """Leader election related errors."""
    pass


class HealthCheckError(Exception):
    """Health check related errors.""" 
    pass


class DistributionError(Exception):
    """Distribution related errors."""
    pass


class ReplicationError(Exception):
    """State replication related errors."""
    pass


class LeaderElection:
    """Redis-based leader election using Redlock algorithm."""
    
    def __init__(self, config: MultiMainConfig, redis_client: redis.Redis):
        self.config = config
        self.redis = redis_client
        self.is_leader = False
        self.election_token: Optional[str] = None
        self.leader_key = "budflow:leader"
        self.election_lock = "budflow:election_lock"
        
        # Redlock Lua script for atomic election
        self.election_script = """
        local key = KEYS[1]
        local token = ARGV[1]
        local ttl = tonumber(ARGV[2])
        local node_id = ARGV[3]
        
        -- Try to acquire lock
        if redis.call('SET', key, node_id, 'NX', 'EX', ttl) then
            -- Set election token
            redis.call('SET', key .. ':token', token, 'EX', ttl)
            return 1
        else
            return 0
        end
        """
        
        # Renewal script
        self.renewal_script = """
        local key = KEYS[1]
        local token = ARGV[1]
        local ttl = tonumber(ARGV[2])
        local node_id = ARGV[3]
        
        -- Check if we still hold the lock
        local current_token = redis.call('GET', key .. ':token')
        if current_token == token then
            -- Renew the lock
            redis.call('EXPIRE', key, ttl)
            redis.call('EXPIRE', key .. ':token', ttl)
            return 1
        else
            return 0
        end
        """
    
    async def start_election(self) -> bool:
        """Start leader election process."""
        try:
            self.election_token = str(uuid4())
            
            # Execute atomic election
            result = await self.redis.eval(
                self.election_script,
                1,
                self.leader_key,
                self.election_token,
                self.config.leader_election_ttl,
                self.config.node_id
            )
            
            self.is_leader = bool(result)
            
            if self.is_leader:
                logger.info(f"Node {self.config.node_id} became leader")
            else:
                logger.debug(f"Node {self.config.node_id} failed to become leader")
                
            return self.is_leader
            
        except RedisConnectionError as e:
            raise LeaderElectionError(f"Redis connection failed: {e}")
        except Exception as e:
            raise LeaderElectionError(f"Election failed: {e}")
    
    async def renew_leadership(self) -> bool:
        """Renew leadership if we are the current leader."""
        if not self.is_leader or not self.election_token:
            return False
        
        try:
            result = await self.redis.eval(
                self.renewal_script,
                1,
                self.leader_key,
                self.election_token,
                self.config.leader_election_ttl,
                self.config.node_id
            )
            
            if not result:
                self.is_leader = False
                self.election_token = None
                logger.warning(f"Node {self.config.node_id} lost leadership")
                
            return bool(result)
            
        except Exception as e:
            logger.error(f"Failed to renew leadership: {e}")
            self.is_leader = False
            self.election_token = None
            return False
    
    async def step_down(self) -> None:
        """Voluntarily step down from leadership."""
        if self.is_leader and self.election_token:
            try:
                await self.redis.delete(self.leader_key)
                await self.redis.delete(f"{self.leader_key}:token")
                logger.info(f"Node {self.config.node_id} stepped down from leadership")
            except Exception as e:
                logger.error(f"Failed to step down: {e}")
            finally:
                self.is_leader = False
                self.election_token = None
    
    async def get_current_leader(self) -> Optional[str]:
        """Get the current leader node ID."""
        try:
            leader = await self.redis.get(self.leader_key)
            if leader:
                return leader.decode() if isinstance(leader, bytes) else leader
            return None
        except Exception as e:
            logger.error(f"Failed to get current leader: {e}")
            return None


class InstanceHealthMonitor:
    """Monitor health of cluster instances."""
    
    def __init__(self, config: MultiMainConfig, redis_client: redis.Redis):
        self.config = config
        self.redis = redis_client
        self.instance_key = f"budflow:instances:{config.node_id}"
        self.heartbeat_key = "budflow:heartbeats"
        self.start_time = datetime.now(timezone.utc)
        
    async def register_instance(self, node_info: NodeInfo) -> None:
        """Register this instance in the cluster."""
        try:
            # Store instance information
            await self.redis.hset(
                f"budflow:instances",
                node_info.node_id,
                node_info.model_dump_json()
            )
            
            # Add to heartbeat tracking
            await self.redis.zadd(
                self.heartbeat_key,
                {node_info.node_id: time.time()}
            )
            
            logger.info(f"Registered instance {node_info.node_id}")
            
        except Exception as e:
            raise HealthCheckError(f"Failed to register instance: {e}")
    
    async def update_heartbeat(self, load_metrics: Optional[Dict[str, Any]] = None) -> None:
        """Update heartbeat and optionally load metrics."""
        try:
            current_time = time.time()
            
            # Update heartbeat timestamp
            await self.redis.zadd(
                self.heartbeat_key,
                {self.config.node_id: current_time}
            )
            
            # Update load metrics if provided
            if load_metrics:
                node_info = NodeInfo(
                    node_id=self.config.node_id,
                    instance_name=self.config.instance_name,
                    host=self.config.host,
                    port=self.config.port,
                    region=self.config.region,
                    environment=self.config.environment,
                    status=NodeStatus.HEALTHY,
                    last_heartbeat=datetime.now(timezone.utc),
                    version=self.config.version,
                    capabilities=self.config.capabilities,
                    load_metrics=load_metrics,
                )
                
                await self.redis.hset(
                    "budflow:instances",
                    self.config.node_id,
                    node_info.model_dump_json()
                )
            
        except Exception as e:
            logger.error(f"Failed to update heartbeat: {e}")
    
    async def get_healthy_instances(self) -> List[str]:
        """Get list of healthy instance IDs."""
        try:
            # Get instances with recent heartbeats
            cutoff_time = time.time() - (self.config.heartbeat_interval * self.config.max_missed_heartbeats)
            
            healthy_nodes = await self.redis.zrangebyscore(
                self.heartbeat_key,
                cutoff_time,
                time.time()
            )
            
            return [node.decode() if isinstance(node, bytes) else node for node in healthy_nodes]
            
        except Exception as e:
            logger.error(f"Failed to get healthy instances: {e}")
            return []
    
    async def cleanup_stale_instances(self) -> int:
        """Clean up stale instance data."""
        try:
            cutoff_time = time.time() - (self.config.heartbeat_interval * self.config.max_missed_heartbeats * 2)
            
            # Get stale nodes
            stale_nodes = await self.redis.zrangebyscore(
                self.heartbeat_key,
                0,
                cutoff_time
            )
            
            if stale_nodes:
                # Remove from heartbeat tracking
                await self.redis.zrem(self.heartbeat_key, *stale_nodes)
                
                # Remove instance data
                for node in stale_nodes:
                    node_id = node.decode() if isinstance(node, bytes) else node
                    await self.redis.hdel("budflow:instances", node_id)
                    await self.redis.delete(f"budflow:instances:{node_id}")
                
                logger.info(f"Cleaned up {len(stale_nodes)} stale instances")
            
            return len(stale_nodes)
            
        except Exception as e:
            logger.error(f"Failed to cleanup stale instances: {e}")
            return 0
    
    async def get_instance_info(self, node_id: str) -> Optional[NodeInfo]:
        """Get detailed information about an instance."""
        try:
            data = await self.redis.hget("budflow:instances", node_id)
            if data:
                return NodeInfo.model_validate_json(data)
            return None
        except Exception as e:
            logger.error(f"Failed to get instance info: {e}")
            return None
    
    async def get_health_status(self) -> Dict[str, Any]:
        """Get health status for this instance."""
        uptime = datetime.now(timezone.utc) - self.start_time
        
        # Basic metrics (would be enhanced with actual system metrics)
        metrics = {}
        if psutil:
            try:
                metrics = {
                    "cpu_percent": psutil.cpu_percent(),
                    "memory_percent": psutil.virtual_memory().percent,
                    "disk_percent": psutil.disk_usage("/").percent,
                    "load_average": psutil.getloadavg()[0] if hasattr(psutil, 'getloadavg') else 0,
                }
            except Exception:
                # Fallback metrics if psutil fails
                metrics = {"cpu_percent": 0, "memory_percent": 0, "disk_percent": 0, "load_average": 0}
        else:
            # Mock metrics if psutil not available
            metrics = {"cpu_percent": 45.0, "memory_percent": 60.0, "disk_percent": 30.0, "load_average": 0.5}
        
        return {
            "status": "healthy",
            "node_id": self.config.node_id,
            "instance_name": self.config.instance_name,
            "uptime_seconds": int(uptime.total_seconds()),
            "version": self.config.version,
            "capabilities": self.config.capabilities,
            "metrics": metrics,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }


class WebhookDistributor:
    """Distribute webhooks across cluster nodes."""
    
    def __init__(self, config: MultiMainConfig, redis_client: redis.Redis):
        self.config = config
        self.redis = redis_client
        self.webhook_routing_key = "budflow:webhook_routing"
        self.webhook_loads_key = "budflow:webhook_loads"
    
    def get_target_node(self, webhook_path: str, available_nodes: List[str]) -> str:
        """Get target node for webhook using consistent hashing."""
        if not available_nodes:
            raise DistributionError("No available nodes for webhook routing")
        
        if self.config.webhook_routing_strategy == "consistent_hash":
            # Use consistent hashing
            hash_input = f"{webhook_path}:{len(available_nodes)}"
            hash_value = int(hashlib.sha256(hash_input.encode()).hexdigest(), 16)
            node_index = hash_value % len(available_nodes)
            return available_nodes[node_index]
        else:
            # Default to first available node
            return available_nodes[0]
    
    async def get_least_loaded_node(self, available_nodes: List[str]) -> str:
        """Get the least loaded node for webhook routing."""
        if not available_nodes:
            raise DistributionError("No available nodes")
        
        try:
            # Get load metrics for all nodes
            loads = await self.redis.hgetall(self.webhook_loads_key)
            
            min_load = float('inf')
            target_node = available_nodes[0]  # Fallback
            
            for node in available_nodes:
                load = float(loads.get(node.encode(), 0))
                if load < min_load:
                    min_load = load
                    target_node = node
            
            return target_node
            
        except Exception as e:
            logger.error(f"Failed to get least loaded node: {e}")
            return available_nodes[0]  # Fallback to first node
    
    async def sync_webhook_registration(self, webhook_config: Dict[str, Any]) -> None:
        """Synchronize webhook registration across cluster."""
        try:
            # Publish webhook registration event
            event = {
                "type": "webhook_registered",
                "config": webhook_config,
                "node_id": self.config.node_id,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
            
            await self.redis.publish("budflow:webhook_events", json.dumps(event))
            
        except Exception as e:
            raise DistributionError(f"Failed to sync webhook registration: {e}")
    
    async def reassign_webhooks_from_failed_node(
        self, failed_node: str, remaining_nodes: List[str]
    ) -> int:
        """Reassign webhooks from a failed node to remaining nodes."""
        try:
            # Get all webhook assignments
            assignments = await self.redis.hgetall(self.webhook_routing_key)
            
            reassigned_count = 0
            for webhook_path, assigned_node in assignments.items():
                webhook_path = webhook_path.decode()
                assigned_node = assigned_node.decode()
                
                if assigned_node == failed_node:
                    # Reassign to a new node
                    new_node = self.get_target_node(webhook_path, remaining_nodes)
                    await self.redis.hset(self.webhook_routing_key, webhook_path, new_node)
                    reassigned_count += 1
                    
                    # Publish reassignment event
                    event = {
                        "type": "webhook_reassigned",
                        "webhook_path": webhook_path,
                        "from_node": failed_node,
                        "to_node": new_node,
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                    }
                    await self.redis.publish("budflow:webhook_events", json.dumps(event))
            
            logger.info(f"Reassigned {reassigned_count} webhooks from failed node {failed_node}")
            return reassigned_count
            
        except Exception as e:
            raise DistributionError(f"Failed to reassign webhooks: {e}")


class ScheduleDistributor:
    """Distribute scheduled jobs across cluster nodes."""
    
    def __init__(self, config: MultiMainConfig, redis_client: redis.Redis):
        self.config = config
        self.redis = redis_client
        self.schedule_assignments_key = "budflow:schedule_assignments"
        
    async def distribute_schedules(
        self, schedules: List[Dict[str, Any]], available_nodes: List[str]
    ) -> Dict[str, List[Dict[str, Any]]]:
        """Distribute schedules across available nodes."""
        if not available_nodes:
            raise DistributionError("No available nodes for schedule distribution")
        
        distribution = {node: [] for node in available_nodes}
        
        # Simple round-robin distribution
        for i, schedule in enumerate(schedules):
            target_node = available_nodes[i % len(available_nodes)]
            distribution[target_node].append(schedule)
            
            # Store assignment in Redis
            await self.redis.sadd(
                f"{self.schedule_assignments_key}:{target_node}",
                schedule["id"]
            )
        
        return distribution
    
    async def handle_node_failure(self, failed_node: str, remaining_nodes: List[str]) -> int:
        """Handle schedule redistribution when a node fails."""
        try:
            # Get schedules assigned to failed node
            failed_schedules = await self.redis.smembers(
                f"{self.schedule_assignments_key}:{failed_node}"
            )
            
            if not failed_schedules or not remaining_nodes:
                return 0
            
            reassigned_count = 0
            
            # Redistribute schedules to remaining nodes
            for i, schedule_id in enumerate(failed_schedules):
                target_node = remaining_nodes[i % len(remaining_nodes)]
                
                # Move schedule to new node
                await self.redis.sadd(
                    f"{self.schedule_assignments_key}:{target_node}",
                    schedule_id
                )
                
                # Remove from failed node
                await self.redis.srem(
                    f"{self.schedule_assignments_key}:{failed_node}",
                    schedule_id
                )
                
                reassigned_count += 1
            
            logger.info(f"Reassigned {reassigned_count} schedules from failed node {failed_node}")
            return reassigned_count
            
        except Exception as e:
            raise DistributionError(f"Failed to handle schedule failover: {e}")
    
    async def coordinate_schedule_distribution(self) -> None:
        """Coordinate schedule distribution (leader only)."""
        try:
            # Check if this node is the leader
            current_leader = await self.redis.get("budflow:leader")
            if not current_leader:
                return  # No leader
                
            leader_id = current_leader.decode() if isinstance(current_leader, bytes) else current_leader
            if leader_id != self.config.node_id:
                return  # Only leader coordinates
            
            # Publish schedule coordination event
            event = {
                "type": "schedule_coordination",
                "leader_node": self.config.node_id,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
            
            await self.redis.publish("budflow:schedule_events", json.dumps(event))
            
        except Exception as e:
            logger.error(f"Failed to coordinate schedule distribution: {e}")


class StateReplicator:
    """Replicate state across cluster nodes."""
    
    def __init__(self, config: MultiMainConfig, redis_client: redis.Redis):
        self.config = config
        self.redis = redis_client
        self.execution_state_prefix = "budflow:execution_state"
        self.session_state_prefix = "budflow:session_state"
    
    async def replicate_execution_state(self, execution_state: Dict[str, Any]) -> None:
        """Replicate execution state across cluster."""
        if not self.config.enable_state_replication:
            return
        
        try:
            execution_id = execution_state["execution_id"]
            state_key = f"{self.execution_state_prefix}:{execution_id}"
            
            # Store state with TTL
            await self.redis.hset(state_key, mapping=execution_state)
            await self.redis.expire(state_key, self.config.state_replication_ttl)
            
        except Exception as e:
            raise ReplicationError(f"Failed to replicate execution state: {e}")
    
    async def get_execution_state(self, execution_id: str) -> Optional[Dict[str, Any]]:
        """Get execution state from cluster."""
        try:
            state_key = f"{self.execution_state_prefix}:{execution_id}"
            state_data = await self.redis.hgetall(state_key)
            
            if state_data:
                # Convert bytes to strings
                return {k.decode(): v.decode() for k, v in state_data.items()}
            
            return None
            
        except Exception as e:
            logger.error(f"Failed to get execution state: {e}")
            return None
    
    async def cleanup_expired_state(self) -> int:
        """Clean up expired execution state."""
        try:
            # Find all execution state keys
            pattern = f"{self.execution_state_prefix}:*"
            keys = await self.redis.keys(pattern)
            
            expired_count = 0
            for key in keys:
                ttl = await self.redis.ttl(key)
                if ttl == -1:  # No expiration set or expired
                    await self.redis.delete(key)
                    expired_count += 1
            
            return expired_count
            
        except Exception as e:
            logger.error(f"Failed to cleanup expired state: {e}")
            return 0
    
    async def migrate_session(
        self, from_node: str, to_node: str, session_data: Dict[str, Any]
    ) -> None:
        """Migrate session from one node to another."""
        try:
            session_id = session_data.get("session_token")
            if not session_id:
                return
            
            session_key = f"{self.session_state_prefix}:{session_id}"
            
            # Store session data
            await self.redis.hset(session_key, mapping=session_data)
            
            # Publish migration event
            event = {
                "type": "session_migrated",
                "session_id": session_id,
                "from_node": from_node,
                "to_node": to_node,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
            
            await self.redis.publish("budflow:session_events", json.dumps(event))
            
        except Exception as e:
            raise ReplicationError(f"Failed to migrate session: {e}")


class MultiMainManager:
    """Main coordinator for multi-main high availability."""
    
    def __init__(self, config: MultiMainConfig, redis_client):
        self.config = config
        self.redis = redis_client
        
        # Initialize Consul client if available
        if consul:
            try:
                consul_host = config.consul_url.split("://")[1].split(":")[0]
                self.consul_client = consul.Consul(host=consul_host)
            except Exception as e:
                logger.warning(f"Failed to initialize Consul client: {e}")
                self.consul_client = None
        else:
            self.consul_client = None
        
        # Components
        self.leader_election = LeaderElection(config, redis_client)
        self.health_monitor = InstanceHealthMonitor(config, redis_client)
        self.webhook_distributor = WebhookDistributor(config, redis_client)
        self.schedule_distributor = ScheduleDistributor(config, redis_client)
        self.state_replicator = StateReplicator(config, redis_client)
        
        # State
        self.is_running = False
        self.safe_mode = False
        self.background_tasks: Set[asyncio.Task] = set()
        
    async def start(self) -> None:
        """Start multi-main coordination."""
        try:
            logger.info(f"Starting multi-main mode for node {self.config.node_id}")
            
            # Register with Consul
            await self._register_with_consul()
            
            # Register instance
            node_info = NodeInfo(
                node_id=self.config.node_id,
                instance_name=self.config.instance_name,
                host=self.config.host,
                port=self.config.port,
                region=self.config.region,
                environment=self.config.environment,
                status=NodeStatus.STARTING,
                last_heartbeat=datetime.now(timezone.utc),
                version=self.config.version,
                capabilities=self.config.capabilities,
                load_metrics={},
            )
            
            await self.health_monitor.register_instance(node_info)
            
            # Start background tasks
            self._start_background_tasks()
            
            self.is_running = True
            logger.info(f"Multi-main mode started for node {self.config.node_id}")
            
        except Exception as e:
            logger.error(f"Failed to start multi-main mode: {e}")
            raise
    
    async def stop(self) -> None:
        """Stop multi-main coordination."""
        logger.info(f"Stopping multi-main mode for node {self.config.node_id}")
        
        self.is_running = False
        
        # Step down from leadership if leader
        if self.leader_election.is_leader:
            await self.leader_election.step_down()
        
        # Cancel background tasks
        for task in self.background_tasks.copy():
            if not task.done():
                task.cancel()
        
        # Wait for task cancellation to complete
        if self.background_tasks:
            await asyncio.gather(*self.background_tasks, return_exceptions=True)
        
        # Clear the task set
        self.background_tasks.clear()
        
        # Cleanup Redis state
        try:
            await self.redis.delete(f"budflow:instances:{self.config.node_id}")
            await self.redis.zrem("budflow:heartbeats", self.config.node_id)
        except Exception as e:
            logger.error(f"Failed to cleanup Redis state: {e}")
        
        # Deregister from Consul
        if self.consul_client:
            try:
                self.consul_client.agent.service.deregister(self.config.node_id)
            except Exception as e:
                logger.error(f"Failed to deregister from Consul: {e}")
        
        logger.info(f"Multi-main mode stopped for node {self.config.node_id}")
    
    def _start_background_tasks(self) -> None:
        """Start background coordination tasks."""
        # Leader election cycle
        task = asyncio.create_task(self._election_cycle_loop())
        self.background_tasks.add(task)
        
        # Health monitoring
        task = asyncio.create_task(self._health_monitoring_loop())
        self.background_tasks.add(task)
        
        # Cleanup tasks
        task = asyncio.create_task(self._cleanup_loop())
        self.background_tasks.add(task)
    
    async def _election_cycle_loop(self) -> None:
        """Background task for leader election."""
        try:
            while self.is_running:
                try:
                    await self._election_cycle()
                    await asyncio.sleep(self.config.election_timeout // 2)
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    if self.is_running:  # Only log if not shutting down
                        logger.error(f"Election cycle error: {e}")
                    await asyncio.sleep(5)
        except asyncio.CancelledError:
            pass
    
    async def _election_cycle(self) -> None:
        """Single election cycle."""
        if self.leader_election.is_leader:
            # Try to renew leadership
            renewed = await self.leader_election.renew_leadership()
            if not renewed:
                logger.warning("Lost leadership, will try to re-elect")
        else:
            # Try to become leader
            await self.leader_election.start_election()
        
        # Check for split-brain
        if await self._detect_split_brain():
            logger.critical("Split-brain detected, entering safe mode")
            self.safe_mode = True
            await self.leader_election.step_down()
    
    async def _health_monitoring_loop(self) -> None:
        """Background task for health monitoring."""
        try:
            while self.is_running:
                try:
                    # Update heartbeat with current metrics
                    health_status = await self.health_monitor.get_health_status()
                    load_metrics = health_status.get("metrics", {})
                    
                    await self.health_monitor.update_heartbeat(load_metrics)
                    
                    await asyncio.sleep(self.config.heartbeat_interval)
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    if self.is_running:  # Only log if not shutting down
                        logger.error(f"Health monitoring error: {e}")
                    await asyncio.sleep(5)
        except asyncio.CancelledError:
            pass
    
    async def _cleanup_loop(self) -> None:
        """Background task for cleanup operations."""
        try:
            while self.is_running:
                try:
                    # Cleanup stale instances
                    await self.health_monitor.cleanup_stale_instances()
                    
                    # Cleanup expired state
                    if self.config.enable_state_replication:
                        await self.state_replicator.cleanup_expired_state()
                    
                    await asyncio.sleep(self.config.health_check_interval * 2)
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    if self.is_running:  # Only log if not shutting down
                        logger.error(f"Cleanup error: {e}")
                    await asyncio.sleep(30)
        except asyncio.CancelledError:
            pass
    
    async def _detect_split_brain(self) -> bool:
        """Detect split-brain scenarios."""
        try:
            # Get current leader from Redis
            redis_leader = await self.redis.get("budflow:leader")
            
            # Get healthy instances
            healthy_instances = await self.health_monitor.get_healthy_instances()
            
            # If we think we're leader but few nodes are visible, possible split-brain
            if (self.leader_election.is_leader and 
                len(healthy_instances) < self.config.quorum_size):
                return True
            
            # If multiple nodes think they're leader (would need more complex detection)
            # This is a simplified check
            return False
            
        except Exception as e:
            logger.error(f"Failed to detect split-brain: {e}")
            return False
    
    async def _handle_network_partition(self) -> None:
        """Handle network partition scenarios."""
        logger.warning("Handling network partition")
        
        # Enter safe mode
        self.safe_mode = True
        
        # Step down from leadership
        if self.leader_election.is_leader:
            await self.leader_election.step_down()
        
        # Wait for network recovery
        while self.is_running:
            try:
                await self.redis.ping()
                logger.info("Network connectivity restored")
                self.safe_mode = False
                break
            except RedisConnectionError:
                await asyncio.sleep(10)
    
    async def _register_with_consul(self) -> None:
        """Register service with Consul."""
        if not self.consul_client or not consul:
            logger.info("Consul not available, skipping registration")
            return
            
        try:
            self.consul_client.agent.service.register(
                name="budflow-main",
                service_id=self.config.node_id,
                address=self.config.host,
                port=self.config.port,
                tags=[
                    f"region:{self.config.region}",
                    f"environment:{self.config.environment}",
                    f"version:{self.config.version}",
                ] + [f"capability:{cap}" for cap in self.config.capabilities],
                check=consul.Check.http(
                    f"http://{self.config.host}:{self.config.port}/health",
                    interval="10s",
                    timeout="5s"
                )
            )
            logger.info(f"Registered with Consul: {self.config.node_id}")
        except Exception as e:
            logger.error(f"Failed to register with Consul: {e}")
            # Continue without Consul if it's not available
    
    async def get_cluster_status(self) -> Dict[str, Any]:
        """Get comprehensive cluster status."""
        try:
            healthy_instances = await self.health_monitor.get_healthy_instances()
            current_leader = await self.leader_election.get_current_leader()
            
            cluster_status = {
                "node_id": self.config.node_id,
                "is_leader": self.leader_election.is_leader,
                "current_leader": current_leader,
                "healthy_instances": healthy_instances,
                "total_instances": len(healthy_instances),
                "safe_mode": self.safe_mode,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
            
            # Get detailed instance information
            instances_info = {}
            for node_id in healthy_instances:
                info = await self.health_monitor.get_instance_info(node_id)
                if info:
                    instances_info[node_id] = info.model_dump()
            
            cluster_status["instances"] = instances_info
            
            return cluster_status
            
        except Exception as e:
            logger.error(f"Failed to get cluster status: {e}")
            return {
                "error": str(e),
                "node_id": self.config.node_id,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }