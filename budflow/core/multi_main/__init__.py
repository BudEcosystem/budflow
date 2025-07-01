"""Multi-Main high availability system for BudFlow."""

import asyncio
import hashlib
import json
import time
from datetime import datetime, timezone
from enum import Enum
from typing import Dict, List, Optional, Set, Any
from dataclasses import dataclass, field
from uuid import uuid4

import redis
import structlog
from pydantic import BaseModel, Field

# Import consul for tests (optional dependency)
try:
    import consul
except ImportError:
    consul = None

logger = structlog.get_logger()


class NodeStatus(str, Enum):
    """Node status enumeration."""
    HEALTHY = "healthy"
    UNHEALTHY = "unhealthy"
    DISCONNECTED = "disconnected"
    MAINTENANCE = "maintenance"


class MultiMainConfig(BaseModel):
    """Configuration for Multi-Main setup."""
    node_id: str
    redis_url: str = "redis://localhost:6379"
    consul_url: Optional[str] = "http://localhost:8500"
    instance_name: Optional[str] = None
    region: str = "us-east-1"
    environment: str = "production"
    health_check_interval: int = 30
    leader_election_ttl: int = 60
    heartbeat_interval: int = 15
    webhook_routing_strategy: str = "consistent_hash"
    enable_state_replication: bool = True
    max_split_brain_timeout: int = 120
    min_cluster_size: int = 3


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
    version: str = "1.0.0"
    capabilities: List[str] = Field(default_factory=list)
    load_metrics: Dict[str, Any] = Field(default_factory=dict)
    
    def is_healthy(self) -> bool:
        """Check if node is healthy."""
        return self.status == NodeStatus.HEALTHY
    
    def is_overloaded(self) -> bool:
        """Check if node is overloaded."""
        cpu_threshold = 90.0
        memory_threshold = 90.0
        queue_threshold = 100
        
        cpu_percent = self.load_metrics.get("cpu_percent", 0)
        memory_percent = self.load_metrics.get("memory_percent", 0)
        queue_size = self.load_metrics.get("queue_size", 0)
        
        return (cpu_percent > cpu_threshold or 
                memory_percent > memory_threshold or 
                queue_size > queue_threshold)


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
    """Replication related errors."""
    pass


class LeaderElection:
    """Leader election using Redis with distributed locks."""
    
    def __init__(self, config: MultiMainConfig, redis_client):
        self.config = config
        self.redis_client = redis_client
        self.is_leader = False
        self.election_token = None
        self.leader_key = f"budflow:leader:{config.environment}"
        self.token_key = f"budflow:leader_token:{config.environment}"
    
    async def start_election(self) -> bool:
        """Start leader election process."""
        try:
            # Lua script for atomic leader election
            election_script = """
            local leader_key = KEYS[1]
            local token_key = KEYS[2]
            local node_id = ARGV[1]
            local ttl = tonumber(ARGV[2])
            local token = ARGV[3]
            
            local current_leader = redis.call('GET', leader_key)
            if current_leader == false then
                redis.call('SETEX', leader_key, ttl, node_id)
                redis.call('SETEX', token_key, ttl, token)
                return 1
            else
                return 0
            end
            """
            
            token = str(uuid4())
            result = await self.redis_client.eval(
                election_script,
                2,  # Number of keys
                self.leader_key,
                self.token_key,
                self.config.node_id,
                self.config.leader_election_ttl,
                token
            )
            
            if result == 1:
                self.is_leader = True
                self.election_token = token
                logger.info(f"Node {self.config.node_id} became leader")
                return True
            else:
                self.is_leader = False
                logger.debug(f"Node {self.config.node_id} failed to become leader")
                return False
                
        except Exception as e:
            logger.error(f"Leader election failed: {str(e)}")
            raise LeaderElectionError(f"Redis connection failed: {str(e)}")
    
    async def renew_leadership(self) -> bool:
        """Renew leadership lease."""
        if not self.is_leader or not self.election_token:
            return False
        
        try:
            # Lua script for atomic lease renewal
            renewal_script = """
            local leader_key = KEYS[1]
            local token_key = KEYS[2]
            local node_id = ARGV[1]
            local ttl = tonumber(ARGV[2])
            local token = ARGV[3]
            
            local current_leader = redis.call('GET', leader_key)
            local current_token = redis.call('GET', token_key)
            
            if current_leader == node_id and current_token == token then
                redis.call('SETEX', leader_key, ttl, node_id)
                redis.call('SETEX', token_key, ttl, token)
                return 1
            else
                return 0
            end
            """
            
            result = await self.redis_client.eval(
                renewal_script,
                2,
                self.leader_key,
                self.token_key,
                self.config.node_id,
                self.config.leader_election_ttl,
                self.election_token
            )
            
            if result == 1:
                return True
            else:
                self.is_leader = False
                self.election_token = None
                logger.warning(f"Node {self.config.node_id} lost leadership")
                return False
                
        except Exception as e:
            logger.error(f"Leadership renewal failed: {str(e)}")
            self.is_leader = False
            self.election_token = None
            return False
    
    async def step_down(self):
        """Step down from leadership."""
        if not self.is_leader:
            return
        
        try:
            await self.redis_client.delete(self.leader_key)
            await self.redis_client.delete(self.token_key)
            self.is_leader = False
            self.election_token = None
            logger.info(f"Node {self.config.node_id} stepped down from leadership")
        except Exception as e:
            logger.error(f"Failed to step down: {str(e)}")
    
    async def get_current_leader(self) -> Optional[str]:
        """Get current cluster leader."""
        try:
            leader = await self.redis_client.get(self.leader_key)
            if leader:
                return leader.decode() if isinstance(leader, bytes) else leader
            return None
        except Exception as e:
            logger.error(f"Failed to get current leader: {str(e)}")
            return None


class InstanceHealthMonitor:
    """Monitor instance health and manage cluster membership."""
    
    def __init__(self, config: MultiMainConfig, redis_client):
        self.config = config
        self.redis_client = redis_client
        self.health_key = f"budflow:health:{config.environment}"
        self.heartbeat_key = f"budflow:heartbeat:{config.environment}"
        self.start_time = time.time()
    
    async def register_instance(self, node_info: NodeInfo):
        """Register instance in cluster."""
        try:
            # Store detailed node info
            await self.redis_client.hset(
                self.health_key,
                node_info.node_id,
                node_info.model_dump_json()
            )
            
            # Add to heartbeat tracking
            current_time = time.time()
            await self.redis_client.zadd(
                self.heartbeat_key,
                {node_info.node_id: current_time}
            )
            
            logger.info(f"Registered instance {node_info.node_id}")
        except Exception as e:
            logger.error(f"Failed to register instance: {str(e)}")
            raise HealthCheckError(f"Registration failed: {str(e)}")
    
    async def update_heartbeat(self):
        """Update instance heartbeat."""
        try:
            current_time = time.time()
            await self.redis_client.zadd(
                self.heartbeat_key,
                {self.config.node_id: current_time}
            )
        except Exception as e:
            logger.error(f"Failed to update heartbeat: {str(e)}")
    
    async def get_healthy_instances(self) -> List[str]:
        """Get list of healthy instances."""
        try:
            # Get instances with recent heartbeats
            cutoff_time = time.time() - (self.config.heartbeat_interval * 3)
            instances = await self.redis_client.zrangebyscore(
                self.heartbeat_key,
                cutoff_time,
                '+inf'
            )
            
            return [instance.decode() for instance in instances]
        except Exception as e:
            logger.error(f"Failed to get healthy instances: {str(e)}")
            return []
    
    async def cleanup_stale_instances(self) -> int:
        """Clean up stale/dead instances."""
        try:
            # Find stale instances
            cutoff_time = time.time() - (self.config.heartbeat_interval * 5)
            stale_instances = await self.redis_client.zrangebyscore(
                self.heartbeat_key,
                0,
                cutoff_time
            )
            
            if stale_instances:
                # Remove from heartbeat tracking
                await self.redis_client.zrem(
                    self.heartbeat_key,
                    *stale_instances
                )
                
                # Remove detailed info
                for instance in stale_instances:
                    await self.redis_client.delete(f"instance_info:{instance.decode()}")
                
                logger.info(f"Cleaned up {len(stale_instances)} stale instances")
                return len(stale_instances)
            
            return 0
        except Exception as e:
            logger.error(f"Failed to cleanup stale instances: {str(e)}")
            return 0
    
    async def get_instance_info(self, node_id: str) -> Optional[NodeInfo]:
        """Get detailed instance information."""
        try:
            data = await self.redis_client.hget(self.health_key, node_id)
            if data:
                return NodeInfo.model_validate_json(data)
            return None
        except Exception as e:
            logger.error(f"Failed to get instance info: {str(e)}")
            return None
    
    async def get_health_status(self) -> Dict[str, Any]:
        """Get health status for this instance."""
        uptime = time.time() - self.start_time
        
        return {
            "status": "healthy",
            "node_id": self.config.node_id,
            "uptime_seconds": int(uptime),
            "environment": self.config.environment,
            "region": self.config.region,
            "metrics": {
                "cpu_percent": 0.0,  # Would be populated by actual monitoring
                "memory_percent": 0.0,
                "active_executions": 0,
                "queue_size": 0,
            }
        }


class WebhookDistributor:
    """Distribute webhooks across cluster nodes."""
    
    def __init__(self, config: MultiMainConfig, redis_client):
        self.config = config
        self.redis_client = redis_client
        self.webhook_assignments_key = f"budflow:webhook_assignments:{config.environment}"
        self.webhook_loads_key = f"budflow:webhook_loads:{config.environment}"
    
    def get_target_node(self, webhook_path: str, instances: List[str]) -> str:
        """Get target node for webhook using consistent hashing."""
        if not instances:
            raise DistributionError("No healthy instances available")
        
        if self.config.webhook_routing_strategy == "consistent_hash":
            # Consistent hash routing
            path_hash = hashlib.md5(webhook_path.encode()).hexdigest()
            hash_value = int(path_hash, 16)
            target_index = hash_value % len(instances)
            return instances[target_index]
        else:
            # Round-robin fallback
            path_hash = hashlib.md5(webhook_path.encode()).hexdigest()
            hash_value = int(path_hash, 16)
            target_index = hash_value % len(instances)
            return instances[target_index]
    
    async def get_least_loaded_node(self, instances: List[str]) -> str:
        """Get least loaded node for load balancing."""
        try:
            loads = await self.redis_client.hgetall(self.webhook_loads_key)
            
            # Convert to dict with default load of 0
            load_map = {}
            for instance in instances:
                load_key = instance.encode()
                load_map[instance] = int(loads.get(load_key, b'0'))
            
            # Return instance with minimum load
            return min(load_map.keys(), key=lambda x: load_map[x])
        except Exception as e:
            logger.error(f"Failed to get least loaded node: {str(e)}")
            # Fallback to first instance
            return instances[0] if instances else None
    
    async def sync_webhook_registration(self, webhook_config: Dict[str, Any]):
        """Synchronize webhook registration across cluster."""
        try:
            # Publish webhook registration event
            event = {
                "type": "webhook_registration",
                "webhook_config": webhook_config,
                "timestamp": time.time(),
                "node_id": self.config.node_id
            }
            
            await self.redis_client.publish(
                f"budflow:events:{self.config.environment}",
                json.dumps(event)
            )
        except Exception as e:
            logger.error(f"Failed to sync webhook registration: {str(e)}")
    
    async def reassign_webhooks_from_failed_node(
        self, failed_node: str, remaining_instances: List[str]
    ) -> int:
        """Reassign webhooks from failed node to healthy nodes."""
        try:
            # Get current webhook assignments
            assignments = await self.redis_client.hgetall(self.webhook_assignments_key)
            
            reassigned_count = 0
            updates = {}
            
            for webhook_key, assigned_node in assignments.items():
                webhook_path = webhook_key.decode()
                current_node = assigned_node.decode()
                
                if current_node == failed_node:
                    # Reassign to a new node
                    new_node = self.get_target_node(webhook_path, remaining_instances)
                    updates[webhook_path] = new_node
                    reassigned_count += 1
            
            # Apply updates
            if updates:
                await self.redis_client.hset(self.webhook_assignments_key, mapping=updates)
                
                # Publish reassignment event
                event = {
                    "type": "webhook_reassignment",
                    "failed_node": failed_node,
                    "reassignments": updates,
                    "timestamp": time.time()
                }
                
                await self.redis_client.publish(
                    f"budflow:events:{self.config.environment}",
                    json.dumps(event)
                )
            
            logger.info(f"Reassigned {reassigned_count} webhooks from {failed_node}")
            return reassigned_count
            
        except Exception as e:
            logger.error(f"Failed to reassign webhooks: {str(e)}")
            raise DistributionError(f"Webhook reassignment failed: {str(e)}")


class ScheduleDistributor:
    """Distribute scheduled workflows across cluster nodes."""
    
    def __init__(self, config: MultiMainConfig, redis_client):
        self.config = config
        self.redis_client = redis_client
        self.schedule_assignments_key = f"budflow:schedule_assignments:{config.environment}"
    
    async def distribute_schedules(
        self, schedules: List[Dict[str, Any]], instances: List[str]
    ) -> Dict[str, List[Dict[str, Any]]]:
        """Distribute schedules across instances."""
        if not instances:
            raise DistributionError("No healthy instances available")
        
        distribution = {instance: [] for instance in instances}
        
        # Simple round-robin distribution
        for i, schedule in enumerate(schedules):
            target_instance = instances[i % len(instances)]
            distribution[target_instance].append(schedule)
            
            # Store assignment in Redis
            await self.redis_client.sadd(
                f"{self.schedule_assignments_key}:{target_instance}",
                schedule["id"]
            )
        
        return distribution
    
    async def handle_node_failure(self, failed_node: str, remaining_instances: List[str]) -> int:
        """Handle node failure for schedule redistribution."""
        try:
            # Get schedules from failed node
            failed_schedules = await self.redis_client.smembers(
                f"{self.schedule_assignments_key}:{failed_node}"
            )
            
            if not failed_schedules or not remaining_instances:
                return 0
            
            reassigned_count = 0
            
            # Redistribute schedules
            for i, schedule_id in enumerate(failed_schedules):
                target_instance = remaining_instances[i % len(remaining_instances)]
                
                # Add to new instance
                await self.redis_client.sadd(
                    f"{self.schedule_assignments_key}:{target_instance}",
                    schedule_id
                )
                
                # Remove from failed instance
                await self.redis_client.srem(
                    f"{self.schedule_assignments_key}:{failed_node}",
                    schedule_id
                )
                
                reassigned_count += 1
            
            logger.info(f"Reassigned {reassigned_count} schedules from {failed_node}")
            return reassigned_count
            
        except Exception as e:
            logger.error(f"Failed to handle node failure: {str(e)}")
            return 0
    
    async def coordinate_schedule_distribution(self):
        """Leader coordination of schedule distribution."""
        try:
            # Publish schedule coordination event
            event = {
                "type": "schedule_coordination",
                "leader_node": self.config.node_id,
                "timestamp": time.time()
            }
            
            await self.redis_client.publish(
                f"budflow:events:{self.config.environment}",
                json.dumps(event)
            )
        except Exception as e:
            logger.error(f"Failed to coordinate schedules: {str(e)}")


class StateReplicator:
    """Replicate execution state across cluster."""
    
    def __init__(self, config: MultiMainConfig, redis_client):
        self.config = config
        self.redis_client = redis_client
        self.execution_state_key = f"budflow:execution_state:{config.environment}"
        self.session_key = f"budflow:sessions:{config.environment}"
    
    async def replicate_execution_state(self, execution_state: Dict[str, Any]):
        """Replicate execution state to Redis."""
        try:
            execution_id = execution_state["execution_id"]
            
            # Store execution state
            await self.redis_client.hset(
                f"{self.execution_state_key}:{execution_id}",
                mapping={
                    "state": json.dumps(execution_state),
                    "node_id": self.config.node_id,
                    "timestamp": time.time()
                }
            )
            
            # Set expiration (24 hours)
            await self.redis_client.expire(
                f"{self.execution_state_key}:{execution_id}",
                86400
            )
            
        except Exception as e:
            logger.error(f"Failed to replicate execution state: {str(e)}")
            raise ReplicationError(f"State replication failed: {str(e)}")
    
    async def get_execution_state(self, execution_id: str) -> Optional[Dict[str, Any]]:
        """Get execution state from Redis."""
        try:
            state_data = await self.redis_client.hgetall(
                f"{self.execution_state_key}:{execution_id}"
            )
            
            if state_data:
                # Convert bytes to strings and parse JSON
                state = {}
                for key, value in state_data.items():
                    key_str = key.decode() if isinstance(key, bytes) else key
                    value_str = value.decode() if isinstance(value, bytes) else value
                    
                    if key_str == "state":
                        state.update(json.loads(value_str))
                    else:
                        state[key_str] = value_str
                
                return state
            
            return None
        except Exception as e:
            logger.error(f"Failed to get execution state: {str(e)}")
            return None
    
    async def cleanup_expired_state(self) -> int:
        """Clean up expired execution state."""
        try:
            # Find keys that might be expired
            pattern = f"{self.execution_state_key}:*"
            keys = await self.redis_client.keys(pattern)
            
            expired_count = 0
            for key in keys:
                ttl = await self.redis_client.ttl(key)
                if ttl == -1:  # No expiration set or expired
                    await self.redis_client.delete(key)
                    expired_count += 1
            
            return expired_count
        except Exception as e:
            logger.error(f"Failed to cleanup expired state: {str(e)}")
            return 0
    
    async def migrate_session(self, from_node: str, to_node: str, session_data: Dict[str, Any]):
        """Migrate session between nodes."""
        try:
            session_id = session_data["user_id"]
            
            # Store session data
            await self.redis_client.hset(
                f"{self.session_key}:{session_id}",
                mapping={
                    "data": json.dumps(session_data),
                    "from_node": from_node,
                    "to_node": to_node,
                    "migrated_at": time.time()
                }
            )
            
            # Publish migration event
            event = {
                "type": "session_migration",
                "session_id": session_id,
                "from_node": from_node,
                "to_node": to_node,
                "timestamp": time.time()
            }
            
            await self.redis_client.publish(
                f"budflow:events:{self.config.environment}",
                json.dumps(event)
            )
            
        except Exception as e:
            logger.error(f"Failed to migrate session: {str(e)}")


class MultiMainManager:
    """Main coordinator for Multi-Main high availability."""
    
    def __init__(self, config: MultiMainConfig, redis_client=None):
        self.config = config
        self.redis_client = redis_client or redis.from_url(config.redis_url)
        self.is_running = False
        self.safe_mode = False
        
        # Initialize components
        self.leader_election = LeaderElection(config, self.redis_client)
        self.health_monitor = InstanceHealthMonitor(config, self.redis_client)
        self.webhook_distributor = WebhookDistributor(config, self.redis_client)
        self.schedule_distributor = ScheduleDistributor(config, self.redis_client)
        self.state_replicator = StateReplicator(config, self.redis_client)
        
        # Background tasks
        self._tasks: Set[asyncio.Task] = set()
    
    async def start(self):
        """Start Multi-Main manager."""
        try:
            self.is_running = True
            logger.info(f"Starting Multi-Main manager for node {self.config.node_id}")
            
            # Register this instance
            node_info = NodeInfo(
                node_id=self.config.node_id,
                instance_name=self.config.instance_name or self.config.node_id,
                host="127.0.0.1",  # Would be actual host
                port=8000,  # Would be actual port
                region=self.config.region,
                environment=self.config.environment,
                status=NodeStatus.HEALTHY,
                last_heartbeat=datetime.now(timezone.utc),
                capabilities=["webhooks", "executions", "schedules"]
            )
            
            await self.health_monitor.register_instance(node_info)
            
            # Start background tasks
            self._tasks.add(asyncio.create_task(self._heartbeat_loop()))
            self._tasks.add(asyncio.create_task(self._election_loop()))
            self._tasks.add(asyncio.create_task(self._health_check_loop()))
            
        except Exception as e:
            logger.error(f"Failed to start Multi-Main manager: {str(e)}")
            raise
    
    async def stop(self):
        """Stop Multi-Main manager."""
        try:
            self.is_running = False
            logger.info(f"Stopping Multi-Main manager for node {self.config.node_id}")
            
            # Step down if leader
            if self.leader_election.is_leader:
                await self.leader_election.step_down()
            
            # Cancel background tasks
            for task in self._tasks:
                task.cancel()
            
            # Wait for tasks to complete
            if self._tasks:
                await asyncio.gather(*self._tasks, return_exceptions=True)
            
            # Cleanup
            await self.redis_client.delete(f"instance_info:{self.config.node_id}")
            
        except Exception as e:
            logger.error(f"Failed to stop Multi-Main manager: {str(e)}")
    
    async def _heartbeat_loop(self):
        """Background heartbeat loop."""
        while self.is_running:
            try:
                await self.health_monitor.update_heartbeat()
                await asyncio.sleep(self.config.heartbeat_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Heartbeat failed: {str(e)}")
                await asyncio.sleep(5)
    
    async def _election_loop(self):
        """Background election loop."""
        while self.is_running:
            try:
                await self._election_cycle()
                await asyncio.sleep(self.config.leader_election_ttl // 2)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Election cycle failed: {str(e)}")
                await asyncio.sleep(10)
    
    async def _health_check_loop(self):
        """Background health check loop."""
        while self.is_running:
            try:
                await self.health_monitor.cleanup_stale_instances()
                
                # Check for split-brain
                if self.leader_election.is_leader:
                    split_brain = await self._detect_split_brain()
                    if split_brain:
                        logger.warning("Split-brain detected, stepping down")
                        await self.leader_election.step_down()
                
                await asyncio.sleep(self.config.health_check_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Health check failed: {str(e)}")
                await asyncio.sleep(10)
    
    async def _election_cycle(self):
        """Run election cycle."""
        if not self.leader_election.is_leader:
            # Try to become leader
            success = await self.leader_election.start_election()
            if success:
                logger.info(f"Node {self.config.node_id} became leader")
        else:
            # Renew leadership
            success = await self.leader_election.renew_leadership()
            if not success:
                logger.warning(f"Node {self.config.node_id} lost leadership")
    
    async def _detect_split_brain(self) -> bool:
        """Detect split-brain scenarios."""
        try:
            healthy_instances = await self.health_monitor.get_healthy_instances()
            
            # If we can't see enough nodes, assume split-brain
            if len(healthy_instances) < (self.config.min_cluster_size // 2 + 1):
                return True
            
            return False
        except Exception as e:
            logger.error(f"Failed to detect split-brain: {str(e)}")
            return True
    
    async def _handle_network_partition(self):
        """Handle network partition scenarios."""
        self.safe_mode = True
        logger.warning("Entering safe mode due to network partition")
        
        # Step down from leadership
        if self.leader_election.is_leader:
            await self.leader_election.step_down()
    
    async def _try_become_leader(self):
        """Try to become leader."""
        return await self.leader_election.start_election()
    
    async def _maintain_leadership(self):
        """Maintain leadership."""
        return await self.leader_election.renew_leadership()
    
    async def register_webhook(self, webhook_id: str, path: str):
        """Register webhook."""
        webhook_config = {
            "webhook_id": webhook_id,
            "path": path,
            "instance_id": self.config.node_id
        }
        await self.webhook_distributor.sync_webhook_registration(webhook_config)
    
    async def register_schedule(self, schedule_id: str, cron: str):
        """Register schedule."""
        schedule_config = {
            "id": schedule_id,
            "cron": cron,
            "workflow_id": str(uuid4())
        }
        await self.schedule_distributor.coordinate_schedule_distribution()
    
    async def get_cluster_status(self) -> Dict[str, Any]:
        """Get cluster status."""
        leader = await self.leader_election.get_current_leader()
        instances = await self.health_monitor.get_healthy_instances()
        
        return {
            "leader": leader,
            "instances": [{"instance_id": inst} for inst in instances],
            "total_instances": len(instances),
            "is_leader": self.leader_election.is_leader,
            "safe_mode": self.safe_mode
        }
    
    async def _redistribute_on_failure(self):
        """Redistribute resources on node failure."""
        healthy_instances = await self.health_monitor.get_healthy_instances()
        
        # Redistribute webhooks
        webhook_count = await self.webhook_distributor.reassign_webhooks_from_failed_node(
            "failed-node", healthy_instances
        )
        
        # Redistribute schedules  
        schedule_count = await self.schedule_distributor.handle_node_failure(
            "failed-node", healthy_instances
        )
        
        logger.info(f"Redistributed {webhook_count} webhooks and {schedule_count} schedules")


# Export all classes
__all__ = [
    'MultiMainConfig',
    'NodeInfo', 
    'NodeStatus',
    'LeaderElection',
    'InstanceHealthMonitor',
    'WebhookDistributor',
    'ScheduleDistributor',
    'StateReplicator',
    'MultiMainManager',
    'LeaderElectionError',
    'HealthCheckError',
    'DistributionError',
    'ReplicationError',
]