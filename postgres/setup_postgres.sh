#!/bin/bash
# ==============================================================================
# postgres/setup_postgres.sh
# ==============================================================================
# Installs PostgreSQL 16 on Amazon Linux 2023 / Ubuntu, configures it for CDC,
# optimizes memory settings, and establishes logical replication parameters.
# Run as root: sudo ./setup_postgres.sh
# ==============================================================================

set -euo pipefail

# --- Logger Setup ---
log() {
    echo -e "\033[1;34m[$(date +'%Y-%m-%dT%H:%M:%S')] $1\033[0m"
}

error() {
    echo -e "\033[1;31m[$(date +'%Y-%m-%dT%H:%M:%S')] ERROR: $1\033[0m" >&2
}

# --- Check Root ---
if [ "$EUID" -ne 0 ]; then
    error "Please run as root (sudo)"
    exit 1
fi

# --- Detect OS ---
OS_FAMILY=""
if [ -f /etc/os-release ]; then
    . /etc/os-release
    OS_FAMILY=$ID
else
    error "Cannot determine OS type. /etc/os-release not found."
    exit 1
fi

log "Detected OS Family: $OS_FAMILY"

# --- Install PostgreSQL 16 ---
if [[ "$OS_FAMILY" == "ubuntu" || "$OS_FAMILY" == "debian" ]]; then
    log "Installing PostgreSQL 16 for Debian/Ubuntu..."
    apt-get update -y
    apt-get install -y lsb-release gnupg2 wget ca-certificates
    
    # Import repo key
    install -d /etc/apt/keyrings
    wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | gpg --dearmor -o /etc/apt/keyrings/postgresql.gpg
    
    # Add PostgreSQL official repo
    echo "deb [signed-by=/etc/apt/keyrings/postgresql.gpg] http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list
    
    apt-get update -y
    apt-get install -y postgresql-16 postgresql-contrib-16
    
    PG_CONF="/etc/postgresql/16/main/postgresql.conf"
    PG_HBA="/etc/postgresql/16/main/pg_hba.conf"
    PG_SERVICE="postgresql"

elif [[ "$OS_FAMILY" == "amzn" || "$OS_FAMILY" == "rhel" ]]; then
    log "Installing PostgreSQL 16 for Amazon Linux 2023..."
    # Amazon Linux 2023 ships postgresql15 by default, to install 16 we add the community PG DG repository.
    dnf install -y https://download.postgresql.org/pub/repos/yum/reporpms/EL-9-x86_64/pgdg-redhat-repo-latest.noarch.rpm
    dnf -qy disable "postgresql" || true # disable default postgresql module if any
    dnf install -y postgresql16-server postgresql16-contrib
    
    # Initialize DB if not initialized
    if [ ! -f /var/lib/pgsql/16/data/PG_VERSION ]; then
        log "Initializing PostgreSQL 16 database..."
        /usr/pgsql-16/bin/postgresql-16-setup initdb
    fi
    
    PG_CONF="/var/lib/pgsql/16/data/postgresql.conf"
    PG_HBA="/var/lib/pgsql/16/data/pg_hba.conf"
    PG_SERVICE="postgresql-16"
else
    error "Unsupported OS: $OS_FAMILY. Please install PostgreSQL 16 manually."
    exit 1
fi

# --- Configure for CDC and Performance ---
log "Configuring PostgreSQL server at $PG_CONF..."

# Backup original configurations
cp "$PG_CONF" "${PG_CONF}.bak"
cp "$PG_HBA" "${PG_HBA}.bak"

# Append performance tuning and logical replication parameters to postgresql.conf
cat << EOF >> "$PG_CONF"

# ---------------------------------------------------------
# IoT Streaming Platform CDC & Performance Optimizations
# ---------------------------------------------------------
listen_addresses = '*'
wal_level = logical
max_replication_slots = 10
max_wal_senders = 10
wal_keep_size = 2048MB       # Retain WAL logs in case connector goes offline

# Memory Configuration
shared_buffers = 1GB         # Adjust based on EC2 size (e.g., 25% of memory)
work_mem = 64MB
maintenance_work_mem = 256MB
effective_cache_size = 3GB

# Logging Configuration
logging_collector = on
log_min_messages = warning
log_min_error_statement = error
log_min_duration_statement = 250 # Log queries taking more than 250ms
log_line_prefix = '%m [%p] %q%u@%d '
EOF

# --- Allow Inbound Network Connections in pg_hba.conf ---
log "Updating pg_hba.conf at $PG_HBA..."
cat << EOF >> "$PG_HBA"

# ---------------------------------------------------------
# Allow logical replication connections from Debezium/Kafka Connect
# ---------------------------------------------------------
# Allow remote access from inside VPC (e.g. 10.0.0.0/16 subnet)
host    all             all             10.0.0.0/16            scram-sha-256
host    replication     all             10.0.0.0/16            scram-sha-256
EOF

# --- Restart and Enable PostgreSQL Service ---
log "Restarting and enabling service: $PG_SERVICE..."
systemctl restart "$PG_SERVICE"
systemctl enable "$PG_SERVICE"

# --- Output Verification Instructions ---
log "PostgreSQL 16 configuration completed successfully!"
log "Please run the setup_db.sql script using psql:"
log "  sudo -u postgres psql -f postgres/setup_db.sql"
log "To verify logical replication properties, run:"
log "  sudo -u postgres psql -c \"SHOW wal_level;\""
