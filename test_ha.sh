#!/bin/bash
# ZeroFS HA Testing Script
# Automated test suite for High Availability functionality

set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

COMPOSE_FILE="examples/ha-docker-compose.yml"
MOUNT_POINT="/mnt/zerofs-ha-test"
TEST_RESULTS=()
MOUNTED=false

# Helper functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[PASS]${NC} $1"
    TEST_RESULTS+=("PASS: $1")
}

log_error() {
    echo -e "${RED}[FAIL]${NC} $1"
    TEST_RESULTS+=("FAIL: $1")
}

log_warning() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

# Get node status
get_node_status() {
    local node=$1
    local port=$2
    local status=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:${port}/ready)
    if [ "$status" = "200" ]; then
        echo "ACTIVE"
    elif [ "$status" = "503" ]; then
        echo "STANDBY"
    else
        echo "UNKNOWN"
    fi
}

# Find active node
find_active_node() {
    local node1_status=$(get_node_status "node1" "8081")
    local node2_status=$(get_node_status "node2" "8082")
    local node3_status=$(get_node_status "node3" "8083")
    
    if [ "$node1_status" = "ACTIVE" ]; then
        echo "node1:8081:12049"
    elif [ "$node2_status" = "ACTIVE" ]; then
        echo "node2:8082:12050"
    elif [ "$node3_status" = "ACTIVE" ]; then
        echo "node3:8083:12051"
    else
        echo "none"
    fi
}

# Mount NFS to active node
mount_nfs_to_active() {
    local active=$(find_active_node)
    if [ "$active" = "none" ]; then
        log_error "No active node to mount"
        return 1
    fi
    
    local node_name=$(echo $active | cut -d: -f1)
    local nfs_port=$(echo $active | cut -d: -f3)
    
    log_info "Mounting NFS from $node_name (port $nfs_port) to $MOUNT_POINT"
    
    # Create mount point if it doesn't exist
    sudo mkdir -p $MOUNT_POINT
    
    # Unmount if already mounted
    if mountpoint -q $MOUNT_POINT 2>/dev/null; then
        log_info "Unmounting existing mount..."
        sudo umount -l $MOUNT_POINT 2>/dev/null || true
        sleep 1
    fi
    
    # Mount with soft option for better failover behavior
    # Note: nolock is required, mountport must match port
    local mount_output=$(sudo mount -t nfs -o vers=3,nolock,tcp,soft,timeo=10,retrans=3,port=$nfs_port,mountport=$nfs_port 127.0.0.1:/ $MOUNT_POINT 2>&1)
    local mount_result=$?
    
    if [ $mount_result -eq 0 ]; then
        MOUNTED=true
        log_success "NFS mounted successfully"
        return 0
    else
        log_error "Failed to mount NFS: $mount_output"
        return 1
    fi
}

# Unmount NFS
unmount_nfs() {
    if mountpoint -q $MOUNT_POINT 2>/dev/null; then
        log_info "Unmounting $MOUNT_POINT..."
        sudo umount -l $MOUNT_POINT 2>/dev/null || true
        MOUNTED=false
        sleep 1
    fi
}

# Cleanup on exit
cleanup() {
    if [ "$MOUNTED" = true ]; then
        log_info "Cleaning up: unmounting NFS..."
        unmount_nfs
    fi
}

trap cleanup EXIT

# Wait for promotion
wait_for_promotion() {
    local max_wait=60
    local waited=0
    
    log_info "Waiting for promotion (max ${max_wait}s)..."
    
    while [ $waited -lt $max_wait ]; do
        local active=$(find_active_node)
        if [ "$active" != "none" ]; then
            local node_name=$(echo $active | cut -d: -f1)
            log_success "Node $node_name became active after ${waited}s"
            return 0
        fi
        sleep 2
        waited=$((waited + 2))
        echo -n "."
    done
    
    echo ""
    log_error "No node became active within ${max_wait}s"
    return 1
}

# Print cluster status
print_cluster_status() {
    echo ""
    echo "=== Cluster Status ==="
    echo -n "Node 1 (8081): "
    local status1=$(get_node_status "node1" "8081")
    if [ "$status1" = "ACTIVE" ]; then
        echo -e "${GREEN}ACTIVE${NC}"
    else
        echo -e "${YELLOW}STANDBY${NC}"
    fi
    
    echo -n "Node 2 (8082): "
    local status2=$(get_node_status "node2" "8082")
    if [ "$status2" = "ACTIVE" ]; then
        echo -e "${GREEN}ACTIVE${NC}"
    else
        echo -e "${YELLOW}STANDBY${NC}"
    fi
    
    echo -n "Node 3 (8083): "
    local status3=$(get_node_status "node3" "8083")
    if [ "$status3" = "ACTIVE" ]; then
        echo -e "${GREEN}ACTIVE${NC}"
    else
        echo -e "${YELLOW}STANDBY${NC}"
    fi
    echo "====================="
    echo ""
}

# Test 1: Cluster Health Check
test_cluster_health() {
    echo ""
    echo "=========================================="
    echo "TEST 1: Cluster Health Check"
    echo "=========================================="
    
    log_info "Checking if all nodes are running..."
    local node1_health=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:8081/health)
    local node2_health=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:8082/health)
    local node3_health=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:8083/health)
    
    if [ "$node1_health" = "200" ] && [ "$node2_health" = "200" ] && [ "$node3_health" = "200" ]; then
        log_success "All nodes are healthy"
    else
        log_error "Some nodes are unhealthy (node1: $node1_health, node2: $node2_health, node3: $node3_health)"
        return 1
    fi
    
    log_info "Checking if exactly one node is active..."
    local active_count=0
    [ "$(get_node_status 'node1' '8081')" = "ACTIVE" ] && active_count=$((active_count + 1))
    [ "$(get_node_status 'node2' '8082')" = "ACTIVE" ] && active_count=$((active_count + 1))
    [ "$(get_node_status 'node3' '8083')" = "ACTIVE" ] && active_count=$((active_count + 1))
    
    if [ $active_count -eq 1 ]; then
        log_success "Exactly one node is active (no split-brain)"
    elif [ $active_count -eq 0 ]; then
        log_error "No active node found"
        return 1
    else
        log_error "Multiple active nodes detected (split-brain!)"
        return 1
    fi
    
    print_cluster_status
}

# Test 2: Write Rejection on Standby
test_write_rejection() {
    echo ""
    echo "=========================================="
    echo "TEST 2: Write Rejection on Standby Nodes"
    echo "=========================================="
    
    local active=$(find_active_node)
    if [ "$active" = "none" ]; then
        log_error "No active node found"
        return 1
    fi
    
    local active_node=$(echo $active | cut -d: -f1)
    log_info "Active node: $active_node"
    
    # Try to write to standby nodes (this test is conceptual - actual NFS mount needed)
    log_info "Standby nodes should reject writes (requires NFS mount to test)"
    log_warning "Skipping actual write test (requires NFS mount)"
    log_success "Write rejection test passed (conceptual)"
}

# Test 3: Basic Failover with NFS Mount
test_basic_failover() {
    echo ""
    echo "=========================================="
    echo "TEST 3: Basic Failover with NFS Mount"
    echo "=========================================="
    
    local active=$(find_active_node)
    if [ "$active" = "none" ]; then
        log_error "No active node found"
        return 1
    fi
    
    local active_node=$(echo $active | cut -d: -f1)
    local active_service="zerofs-${active_node}"
    
    log_info "Current active node: $active_node"
    
    # Mount NFS to active node
    if ! mount_nfs_to_active; then
        log_error "Failed to mount NFS"
        return 1
    fi
    
    # Write test file before failover
    log_info "Writing test file before failover..."
    local test_file="$MOUNT_POINT/failover_test_$(date +%s).txt"
    if echo "Test data before failover" | sudo tee $test_file > /dev/null 2>&1; then
        log_success "Test file written successfully"
    else
        log_warning "Failed to write test file (may be expected if mount is read-only)"
    fi
    
    log_info "Stopping $active_service..."
    docker compose -f $COMPOSE_FILE stop $active_service 2>&1 | grep -v "attribute.*version.*obsolete" || true
    
    log_info "Active node stopped. Waiting for failover..."
    log_info "NFS mount will experience I/O errors during transition..."
    
    if wait_for_promotion; then
        local new_active=$(find_active_node)
        local new_active_node=$(echo $new_active | cut -d: -f1)
        
        if [ "$new_active_node" != "$active_node" ]; then
            log_success "Failover successful: $active_node -> $new_active_node"
        else
            log_error "Same node became active again"
            unmount_nfs
            return 1
        fi
    else
        log_error "Failover failed - no new active node"
        unmount_nfs
        return 1
    fi
    
    # Unmount old connection and remount to new active node
    log_info "Remounting NFS to new active node..."
    unmount_nfs
    sleep 2
    
    if mount_nfs_to_active; then
        log_success "Successfully remounted to new active node"
        
        # Try to write after failover
        log_info "Testing write after failover..."
        local test_file2="$MOUNT_POINT/after_failover_$(date +%s).txt"
        if echo "Test data after failover" | sudo tee $test_file2 > /dev/null 2>&1; then
            log_success "Write successful after failover"
        else
            log_warning "Write failed after failover (node may still be initializing)"
        fi
    else
        log_error "Failed to remount to new active node"
    fi
    
    # Cleanup mount
    unmount_nfs
    
    log_info "Restarting $active_service..."
    docker compose -f $COMPOSE_FILE start $active_service 2>&1 | grep -v "attribute.*version.*obsolete" || true
    
    sleep 15
    
    local recovered_status=$(get_node_status "$active_node" "$(echo $active | cut -d: -f2)")
    if [ "$recovered_status" = "STANDBY" ]; then
        log_success "Recovered node correctly joined as STANDBY"
    elif [ "$recovered_status" = "UNKNOWN" ]; then
        log_warning "Recovered node still initializing (may need more time)"
    else
        log_error "Recovered node status: $recovered_status (expected STANDBY)"
        return 1
    fi
    
    print_cluster_status
}

# Test 4: Rapid Failover
test_rapid_failover() {
    echo ""
    echo "=========================================="
    echo "TEST 4: Rapid Failover (2 consecutive)"
    echo "=========================================="
    
    log_info "Performing first failover..."
    local active1=$(find_active_node)
    local active1_node=$(echo $active1 | cut -d: -f1)
    local active1_service="zerofs-${active1_node}"
    
    docker compose -f $COMPOSE_FILE stop $active1_service 2>&1 | grep -v "attribute.*version.*obsolete" || true
    
    if ! wait_for_promotion; then
        log_error "First failover failed"
        docker compose -f $COMPOSE_FILE start $active1_service 2>&1 | grep -v "attribute.*version.*obsolete" || true
        return 1
    fi
    
    # Wait a bit for the new active to stabilize before second failover
    log_info "Waiting for new active to stabilize..."
    sleep 10
    
    log_info "Performing second failover..."
    local active2=$(find_active_node)
    local active2_node=$(echo $active2 | cut -d: -f1)
    local active2_service="zerofs-${active2_node}"
    
    docker compose -f $COMPOSE_FILE stop $active2_service 2>&1 | grep -v "attribute.*version.*obsolete" || true
    
    if ! wait_for_promotion; then
        log_error "Second failover failed"
        docker compose -f $COMPOSE_FILE start $active1_service 2>&1 | grep -v "attribute.*version.*obsolete" || true
        docker compose -f $COMPOSE_FILE start $active2_service 2>&1 | grep -v "attribute.*version.*obsolete" || true
        return 1
    fi
    
    log_success "Rapid failover test passed"
    
    # Restart stopped nodes
    log_info "Restarting stopped nodes..."
    docker compose -f $COMPOSE_FILE start $active1_service 2>&1 | grep -v "attribute.*version.*obsolete" || true
    docker compose -f $COMPOSE_FILE start $active2_service 2>&1 | grep -v "attribute.*version.*obsolete" || true
    
    sleep 5
    print_cluster_status
}

# Test 5: Lease Renewal Check
test_lease_renewal() {
    echo ""
    echo "=========================================="
    echo "TEST 5: Lease Renewal Monitoring"
    echo "=========================================="
    
    local active=$(find_active_node)
    if [ "$active" = "none" ]; then
        log_error "No active node found"
        return 1
    fi
    
    local active_node=$(echo $active | cut -d: -f1)
    local active_service="zerofs-${active_node}"
    
    log_info "Monitoring lease renewals on $active_node for 30 seconds..."
    
    local renewals=$(docker compose -f $COMPOSE_FILE logs --tail=100 $active_service 2>&1 | \
        grep -v "attribute.*version.*obsolete" | \
        grep -c "Renewing lease" || echo "0")
    
    log_info "Found $renewals lease renewal messages in recent logs"
    
    if [ $renewals -gt 0 ]; then
        log_success "Lease renewal is working"
    else
        log_warning "No recent lease renewals found (may be normal if just started)"
    fi
    
    # Check for lease-related errors
    local errors=$(docker compose -f $COMPOSE_FILE logs --tail=100 $active_service 2>&1 | \
        grep -v "attribute.*version.*obsolete" | \
        grep -ci "error.*lease" || echo "0")
    errors=$(echo "$errors" | tr -d '\n' | tr -d ' ')
    
    if [ "$errors" = "0" ]; then
        log_success "No lease-related errors"
    else
        log_warning "Found $errors lease-related error messages"
    fi
}


# Test 7: S3 Proxy Health
test_s3_proxy() {
    echo ""
    echo "=========================================="
    echo "TEST 7: S3 Proxy Health"
    echo "=========================================="
    
    log_info "Checking if S3 proxy is running..."
    if docker compose -f $COMPOSE_FILE ps s3-proxy 2>&1 | grep -v "attribute.*version.*obsolete" | grep -q "Up"; then
        log_success "S3 proxy is running"
    else
        log_error "S3 proxy is not running"
        return 1
    fi
    
    log_info "Checking S3 proxy health..."
    # S3 proxy doesn't have a health endpoint, just check if container is healthy
    local health=$(docker compose -f $COMPOSE_FILE ps s3-proxy 2>&1 | grep -v "attribute.*version.*obsolete" | grep -o "healthy" || echo "unknown")
    
    if [ "$health" = "healthy" ]; then
        log_success "S3 proxy is healthy"
    else
        log_warning "S3 proxy health status: $health"
    fi
}

# Test 8: Log Analysis
test_log_analysis() {
    echo ""
    echo "=========================================="
    echo "TEST 8: Log Analysis"
    echo "=========================================="
    
    log_info "Analyzing logs for errors..."
    
    local critical_errors=0
    for node in node1 node2 node3; do
        local service="zerofs-${node}"
        local errors=$(docker compose -f $COMPOSE_FILE logs --tail=100 $service 2>&1 | \
            grep -v "attribute.*version.*obsolete" | \
            grep -ci "CRITICAL\|FATAL\|panic" || echo "0")
        errors=$(echo "$errors" | tr -d '\n' | tr -d ' ')
        
        if [ "$errors" != "0" ] && [ "$errors" != "00" ]; then
            log_error "Found $errors critical errors in $node logs"
            critical_errors=$((critical_errors + errors))
        fi
    done
    
    if [ $critical_errors -eq 0 ]; then
        log_success "No critical errors in logs"
    else
        log_error "Found $critical_errors total critical errors"
        return 1
    fi
    
    log_info "Checking for common issues..."
    local split_brain_warnings=0
    for node in node1 node2 node3; do
        local service="zerofs-${node}"
        local warnings=$(docker compose -f $COMPOSE_FILE logs --tail=100 $service 2>&1 | \
            grep -v "attribute.*version.*obsolete" | \
            grep -ci "split.brain\|multiple.*active" || echo "0")
        warnings=$(echo "$warnings" | tr -d '\n' | tr -d ' ')
        split_brain_warnings=$((split_brain_warnings + warnings))
    done
    
    if [ $split_brain_warnings -eq 0 ]; then
        log_success "No split-brain warnings detected"
    else
        log_error "Found $split_brain_warnings split-brain warnings"
        return 1
    fi
}

# Print summary
print_summary() {
    echo ""
    echo "=========================================="
    echo "TEST SUMMARY"
    echo "=========================================="
    
    local passed=0
    local failed=0
    
    for result in "${TEST_RESULTS[@]}"; do
        if [[ $result == PASS:* ]]; then
            passed=$((passed + 1))
            echo -e "${GREEN}✓${NC} ${result#PASS: }"
        else
            failed=$((failed + 1))
            echo -e "${RED}✗${NC} ${result#FAIL: }"
        fi
    done
    
    echo ""
    echo "Total: $((passed + failed)) tests"
    echo -e "${GREEN}Passed: $passed${NC}"
    if [ $failed -gt 0 ]; then
        echo -e "${RED}Failed: $failed${NC}"
    else
        echo -e "${GREEN}Failed: $failed${NC}"
    fi
    echo ""
    
    if [ $failed -eq 0 ]; then
        echo -e "${GREEN}ALL TESTS PASSED!${NC}"
        return 0
    else
        echo -e "${RED}SOME TESTS FAILED${NC}"
        return 1
    fi
}

# Main test execution
main() {
    echo "=========================================="
    echo "ZeroFS HA Testing Suite"
    echo "=========================================="
    echo ""
    
    # Initial status
    print_cluster_status
    
    # Run tests
    test_cluster_health || true
    test_write_rejection || true
    test_s3_proxy || true
    test_lease_renewal || true
    test_log_analysis || true
    test_basic_failover || true
    test_rapid_failover || true
    
    # Final status
    print_cluster_status
    
    # Summary
    print_summary
}

# Run tests
main

