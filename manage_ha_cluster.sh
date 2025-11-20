#!/bin/bash

# ZeroFS HA Cluster Management Script
# Interactive tool for viewing status and managing nodes during testing

set -e

COMPOSE_FILE="examples/ha-docker-compose.yml"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color
BOLD='\033[1m'

# Print colored output
print_header() {
    echo -e "\n${BLUE}╔══════════════════════════════════════════════════════════╗${NC}"
    echo -e "${BLUE}║${NC} ${BOLD}$1${NC}"
    echo -e "${BLUE}╚══════════════════════════════════════════════════════════╝${NC}\n"
}

print_success() {
    echo -e "${GREEN}✓${NC} $1"
}

print_error() {
    echo -e "${RED}✗${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}⚠${NC} $1"
}

print_info() {
    echo -e "${CYAN}ℹ${NC} $1"
}

# Get node status
get_node_status() {
    local port=$1
    local response=$(curl -s -w "\n%{http_code}" http://localhost:$port/ready 2>/dev/null || echo -e "\nERROR")
    local body=$(echo "$response" | head -n 1)
    local code=$(echo "$response" | tail -n 1)
    
    if [ "$code" = "200" ]; then
        echo "ACTIVE"
    elif [ "$code" = "503" ]; then
        if echo "$body" | grep -q "Standby"; then
            echo "STANDBY"
        elif echo "$body" | grep -q "Initializing"; then
            echo "INITIALIZING"
        else
            echo "UNAVAILABLE"
        fi
    else
        echo "DOWN"
    fi
}

# Show cluster status
show_status() {
    print_header "ZeroFS HA Cluster Status"
    
    echo -e "${BOLD}📦 Container Status:${NC}"
    for node in node1 node2 node3; do
        local service="zerofs-${node}"
        if docker compose -f $COMPOSE_FILE ps $service 2>&1 | grep -v "attribute.*version.*obsolete" | grep -q "Up"; then
            local health=$(docker compose -f $COMPOSE_FILE ps $service 2>&1 | grep -v "attribute.*version.*obsolete" | grep -o "(healthy)\|(unhealthy)\|(starting)" || echo "")
            print_success "$service $health"
        else
            print_error "$service (stopped)"
        fi
    done
    
    echo -e "\n${BOLD}🎯 Node Status:${NC}"
    local active_count=0
    for i in 1 2 3; do
        local port=$((8080 + i))
        local status=$(get_node_status $port)
        
        case $status in
            "ACTIVE")
                echo -e "  Node $i (port $port): ${GREEN}🟢 ACTIVE${NC}"
                active_count=$((active_count + 1))
                ;;
            "STANDBY")
                echo -e "  Node $i (port $port): ${YELLOW}🟡 STANDBY${NC}"
                ;;
            "INITIALIZING")
                echo -e "  Node $i (port $port): ${CYAN}🔵 INITIALIZING${NC}"
                ;;
            "DOWN")
                echo -e "  Node $i (port $port): ${RED}🔴 DOWN${NC}"
                ;;
            *)
                echo -e "  Node $i (port $port): ${RED}❓ UNKNOWN${NC}"
                ;;
        esac
    done
    
    echo -e "\n${BOLD}🔍 Cluster Health:${NC}"
    if [ $active_count -eq 1 ]; then
        print_success "Exactly one active node (healthy)"
    elif [ $active_count -eq 0 ]; then
        print_error "No active node (cluster down!)"
    else
        print_error "Multiple active nodes (SPLIT-BRAIN!)"
    fi
    
    echo -e "\n${BOLD}📊 NFS Ports:${NC}"
    echo "  Node 1: localhost:12049"
    echo "  Node 2: localhost:12050"
    echo "  Node 3: localhost:12051"
    
    echo -e "\n${BOLD}🔌 Health Check Ports:${NC}"
    echo "  Node 1: http://localhost:8081/ready"
    echo "  Node 2: http://localhost:8082/ready"
    echo "  Node 3: http://localhost:8083/ready"
}

# Stop a node
stop_node() {
    local node=$1
    local service="zerofs-node${node}"
    
    print_info "Stopping $service..."
    docker compose -f $COMPOSE_FILE stop $service 2>&1 | grep -v "attribute.*version.*obsolete" || true
    print_success "$service stopped"
}

# Start a node
start_node() {
    local node=$1
    local service="zerofs-node${node}"
    
    print_info "Starting $service..."
    docker compose -f $COMPOSE_FILE start $service 2>&1 | grep -v "attribute.*version.*obsolete" || true
    print_success "$service started"
    print_info "Waiting for node to initialize (5s)..."
    sleep 5
}

# Restart a node
restart_node() {
    local node=$1
    local service="zerofs-node${node}"
    
    print_info "Restarting $service..."
    docker compose -f $COMPOSE_FILE restart $service 2>&1 | grep -v "attribute.*version.*obsolete" || true
    print_success "$service restarted"
    print_info "Waiting for node to initialize (5s)..."
    sleep 5
}

# Kill a node (simulate crash)
kill_node() {
    local node=$1
    local service="zerofs-node${node}"
    
    print_warning "Killing $service (simulating crash)..."
    docker compose -f $COMPOSE_FILE kill $service 2>&1 | grep -v "attribute.*version.*obsolete" || true
    print_success "$service killed"
}

# View logs
view_logs() {
    local node=$1
    local lines=${2:-50}
    local service="zerofs-node${node}"
    
    print_header "Logs for $service (last $lines lines)"
    docker compose -f $COMPOSE_FILE logs --tail=$lines $service 2>&1 | grep -v "attribute.*version.*obsolete" || true
}

# Follow logs
follow_logs() {
    local node=$1
    local service="zerofs-node${node}"
    
    print_header "Following logs for $service (Ctrl+C to stop)"
    docker compose -f $COMPOSE_FILE logs -f $service 2>&1 | grep -v "attribute.*version.*obsolete" || true
}

# Watch lease activity
watch_lease() {
    local node=$1
    local service="zerofs-node${node}"
    
    print_header "Watching lease activity for $service (Ctrl+C to stop)"
    docker compose -f $COMPOSE_FILE logs -f $service 2>&1 | grep -v "attribute.*version.*obsolete" | grep --line-buffered -E "(lease|ACTIVE|STANDBY|acquire|renew|expired|promote)" || true
}

# Simulate failover scenario
simulate_failover() {
    print_header "Simulating Failover Scenario"
    
    # Find active node
    local active_node=""
    for i in 1 2 3; do
        local port=$((8080 + i))
        local status=$(get_node_status $port)
        if [ "$status" = "ACTIVE" ]; then
            active_node=$i
            break
        fi
    done
    
    if [ -z "$active_node" ]; then
        print_error "No active node found!"
        return 1
    fi
    
    print_info "Current active node: Node $active_node"
    echo ""
    read -p "Kill active node to trigger failover? (y/n): " confirm
    
    if [ "$confirm" = "y" ] || [ "$confirm" = "Y" ]; then
        kill_node $active_node
        print_info "Waiting for failover (up to 60 seconds)..."
        
        local waited=0
        local new_active=""
        while [ $waited -lt 60 ]; do
            sleep 2
            waited=$((waited + 2))
            
            for i in 1 2 3; do
                if [ $i -eq $active_node ]; then
                    continue
                fi
                local port=$((8080 + i))
                local status=$(get_node_status $port)
                if [ "$status" = "ACTIVE" ]; then
                    new_active=$i
                    break 2
                fi
            done
            echo -n "."
        done
        
        echo ""
        if [ -n "$new_active" ]; then
            print_success "Failover successful! Node $new_active is now active (took ${waited}s)"
        else
            print_error "Failover failed - no new active node after ${waited}s"
        fi
        
        echo ""
        read -p "Restart the killed node? (y/n): " restart_confirm
        if [ "$restart_confirm" = "y" ] || [ "$restart_confirm" = "Y" ]; then
            start_node $active_node
        fi
    fi
}

# Start all nodes
start_all() {
    print_header "Starting All Nodes"
    docker compose -f $COMPOSE_FILE up -d 2>&1 | grep -v "attribute.*version.*obsolete" || true
    print_success "All nodes started"
    print_info "Waiting for cluster to initialize (10s)..."
    sleep 10
}

# Stop all nodes
stop_all() {
    print_header "Stopping All Nodes"
    echo ""
    read -p "Are you sure you want to stop all nodes? (y/n): " confirm
    
    if [ "$confirm" = "y" ] || [ "$confirm" = "Y" ]; then
        docker compose -f $COMPOSE_FILE stop 2>&1 | grep -v "attribute.*version.*obsolete" || true
        print_success "All nodes stopped"
    else
        print_info "Cancelled"
    fi
}

# Restart cluster
restart_all() {
    print_header "Restarting Entire Cluster"
    docker compose -f $COMPOSE_FILE restart 2>&1 | grep -v "attribute.*version.*obsolete" || true
    print_success "Cluster restarted"
    print_info "Waiting for cluster to initialize (10s)..."
    sleep 10
}

# Show menu
show_menu() {
    clear
    echo -e "${BOLD}${BLUE}"
    cat << "EOF"
╔═══════════════════════════════════════════════════════════════╗
║                                                               ║
║        ZeroFS HA Cluster Management Console                  ║
║                                                               ║
╚═══════════════════════════════════════════════════════════════╝
EOF
    echo -e "${NC}"
    
    echo -e "${BOLD}📊 Status & Monitoring:${NC}"
    echo "  1) Show cluster status"
    echo "  2) View node logs"
    echo "  3) Follow node logs (live)"
    echo "  4) Watch lease activity"
    echo ""
    
    echo -e "${BOLD}🎮 Node Control:${NC}"
    echo "  5) Stop a node"
    echo "  6) Start a node"
    echo "  7) Restart a node"
    echo "  8) Kill a node (simulate crash)"
    echo ""
    
    echo -e "${BOLD}🔄 Cluster Operations:${NC}"
    echo "  9) Start all nodes"
    echo " 10) Stop all nodes"
    echo " 11) Restart entire cluster"
    echo ""
    
    echo -e "${BOLD}🧪 Testing Scenarios:${NC}"
    echo " 12) Simulate failover (kill active node)"
    echo " 13) Run comprehensive test suite"
    echo ""
    
    echo -e "${BOLD}❌ Exit:${NC}"
    echo "  0) Exit"
    echo ""
    
    echo -n "Select an option: "
}

# Main loop
main() {
    while true; do
        show_menu
        read choice
        
        case $choice in
            1)
                show_status
                echo ""
                read -p "Press Enter to continue..."
                ;;
            2)
                echo ""
                read -p "Which node? (1/2/3): " node
                read -p "How many lines? (default 50): " lines
                lines=${lines:-50}
                view_logs $node $lines
                echo ""
                read -p "Press Enter to continue..."
                ;;
            3)
                echo ""
                read -p "Which node? (1/2/3): " node
                follow_logs $node
                ;;
            4)
                echo ""
                read -p "Which node? (1/2/3): " node
                watch_lease $node
                ;;
            5)
                echo ""
                read -p "Which node to stop? (1/2/3): " node
                stop_node $node
                echo ""
                read -p "Press Enter to continue..."
                ;;
            6)
                echo ""
                read -p "Which node to start? (1/2/3): " node
                start_node $node
                echo ""
                read -p "Press Enter to continue..."
                ;;
            7)
                echo ""
                read -p "Which node to restart? (1/2/3): " node
                restart_node $node
                echo ""
                read -p "Press Enter to continue..."
                ;;
            8)
                echo ""
                read -p "Which node to kill? (1/2/3): " node
                kill_node $node
                echo ""
                read -p "Press Enter to continue..."
                ;;
            9)
                start_all
                echo ""
                read -p "Press Enter to continue..."
                ;;
            10)
                stop_all
                echo ""
                read -p "Press Enter to continue..."
                ;;
            11)
                restart_all
                echo ""
                read -p "Press Enter to continue..."
                ;;
            12)
                simulate_failover
                echo ""
                read -p "Press Enter to continue..."
                ;;
            13)
                print_header "Running Comprehensive Test Suite"
                if [ -f "./test_ha.sh" ]; then
                    sudo ./test_ha.sh
                else
                    print_error "test_ha.sh not found"
                fi
                echo ""
                read -p "Press Enter to continue..."
                ;;
            0)
                echo ""
                print_info "Goodbye!"
                exit 0
                ;;
            *)
                print_error "Invalid option"
                sleep 1
                ;;
        esac
    done
}

# Run main
main

