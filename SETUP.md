# 🚕 NYC Taxi HBase Analytics — Setup Guide

> **BDA Capstone Project | 23CSE352** — Full pipeline: HDFS → MapReduce → Hive → Apache HBase  
> Repo: [JSSriJayaram/bda_capstone2](https://github.com/JSSriJayaram/bda_capstone2)

---

## 📦 Software Stack (Exact Versions — Verified)

| Component | Version | Notes |
|---|---|---|
| **Docker Desktop** | 29.x+ | All services run inside Docker — no local Hadoop/HBase install needed |
| **Docker Compose** | v2 (bundled) | Use `docker compose` or `docker-compose` |
| **Java (Host)** | OpenJDK 25 / 17 / 11 / 8 | Only needed if running Maven locally. Container uses Java 8. |
| **Java (Container)** | **OpenJDK 1.8.0_352** (Temurin) | Fixed inside `hbase_docker` image |
| **Apache HBase** | **2.4.19-SNAPSHOT** | Runs inside `hbase_docker` container |
| **Apache Maven** | **3.8.6** | Runs inside container at `/opt/maven` |
| **Apache Hadoop** | **3.3.6** | Via `apache/hadoop:3.3.6` Docker image |
| **Apache Hive** | **3.1.3** | Via `apache/hive:3.1.3` Docker image |
| **Git** | 2.x+ | For cloning and pushing |

> ⚠️ **Important**: You do NOT need to install Java, Maven, Hadoop, or HBase locally.  
> Everything runs inside Docker containers. You only need **Docker** and **Git**.

---

## 🍎 macOS Setup

### Prerequisites — Install via Homebrew

```bash
# 1. Install Homebrew (if not already installed)
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"

# 2. Install Git
brew install git

# 3. Install Docker Desktop (includes Docker Compose v2)
brew install --cask docker

# 4. Launch Docker Desktop
open /Applications/Docker.app

# Wait ~30 seconds for Docker to start, then verify:
docker --version        # Docker version 29.x.x
docker compose version  # Docker Compose version v2.x.x
git --version           # git version 2.x.x
```

> **Apple Silicon (M1/M2/M3)**: The `docker-compose.yml` already has `platform: linux/amd64` on all services.  
> Docker Desktop's Rosetta 2 emulation handles this automatically — no extra config needed.

### Clone and Run

```bash
# 1. Clone the repository
git clone https://github.com/JSSriJayaram/bda_capstone2.git
cd bda_capstone2

# 2. Pull/build the hbase_docker image (first time only)
docker pull padmaganesh/hbase_docker:latest 2>/dev/null || \
  docker build -t hbase_docker ./hbase_docker/

# 3. Start HBase container
docker-compose up -d hbase

# 4. Wait ~20 seconds for HBase to initialize, then verify:
docker exec hbase hbase version
# Expected: HBase 2.4.19-SNAPSHOT

# 5. Open HBase Shell
docker exec -it hbase hbase shell
```

---

## 🪟 Windows Setup

### Prerequisites

#### Option A — Docker Desktop (Recommended)

```powershell
# 1. Install Winget (already on Windows 11 / updated Windows 10)
winget install Git.Git
winget install Docker.DockerDesktop

# 2. Restart your computer after Docker Desktop installs
# 3. Open PowerShell as Administrator and verify:
docker --version
docker compose version
git --version
```

> **WSL2 Required**: Docker Desktop on Windows requires WSL2 backend.  
> If not installed, run in PowerShell (Administrator):
> ```powershell
> wsl --install
> # Restart, then re-open Docker Desktop
> ```

#### Option B — Chocolatey

```powershell
# Install Chocolatey first (run in Admin PowerShell):
Set-ExecutionPolicy Bypass -Scope Process -Force
[System.Net.ServicePointManager]::SecurityProtocol = [System.Net.ServicePointManager]::SecurityProtocol -bor 3072
iex ((New-Object System.Net.WebClient).DownloadString('https://community.chocolatey.org/install.ps1'))

# Then install tools:
choco install git docker-desktop -y
```

### Clone and Run (PowerShell)

```powershell
# 1. Clone
git clone https://github.com/JSSriJayaram/bda_capstone2.git
cd bda_capstone2

# 2. Start HBase (use docker-compose or docker compose)
docker-compose up -d hbase
# OR:
docker compose up -d hbase

# 3. Wait ~20 seconds, then open HBase Shell
docker exec -it hbase hbase shell
```

> **Line Endings Note**: On Windows, Git may auto-convert line endings.  
> To prevent issues with shell scripts, run:
> ```powershell
> git config --global core.autocrlf false
> ```
> Do this **before** cloning.

---

## 🐧 Arch Linux Setup

### Prerequisites — Install via Pacman

```bash
# 1. Update system
sudo pacman -Syu

# 2. Install Git
sudo pacman -S git

# 3. Install Docker and Docker Compose
sudo pacman -S docker docker-compose

# 4. Enable and start Docker daemon
sudo systemctl enable docker
sudo systemctl start docker

# 5. Add your user to the docker group (avoids needing sudo for docker commands)
sudo usermod -aG docker $USER
newgrp docker          # Apply group change in current shell

# 6. Verify
docker --version       # Docker version 29.x.x
docker compose version # Docker Compose version v2.x.x
git --version          # git version 2.x.x
```

> **Note for Arch users**: If `docker-compose` (v1) is not found, use `docker compose` (v2) instead.  
> Both work identically for this project.

### Clone and Run

```bash
# 1. Clone
git clone https://github.com/JSSriJayaram/bda_capstone2.git
cd bda_capstone2

# 2. Start HBase
docker compose up -d hbase

# 3. Wait ~20 seconds, open HBase Shell
docker exec -it hbase hbase shell
```

---

## 🚀 Running the Full Project

### Step 1: Start All Services

```bash
# Start ALL services (Hadoop + Hive + HBase)
docker-compose up -d

# Or start only HBase (for BDA Review 2):
docker-compose up -d hbase
```

### Step 2: Check All Containers Are Running

```bash
docker ps
```

Expected containers:

| Container | Service | Port |
|---|---|---|
| `hbase` | Apache HBase 2.4.x + ZooKeeper | 2181, 16010, 16020 |
| `taxi-namenode` | Hadoop HDFS NameNode | 9871 |
| `taxi-datanode` | Hadoop HDFS DataNode | — |
| `taxi-resourcemanager` | YARN ResourceManager | 8089 |
| `taxi-nodemanager` | YARN NodeManager | — |
| `taxi-hive-server` | Apache Hive 3.1.3 Metastore | 9084 |

### Step 3: HBase Shell — Run Common Setup

```bash
# Enter HBase shell
docker exec -it hbase hbase shell
```

Paste these commands inside the shell:

```ruby
# Create the table with 4 column families
create 'nyc_taxi', 'trip_info', 'financials', 'timing', 'metrics'

# Verify
list
describe 'nyc_taxi'
```

### Step 4: Build the Java Project

```bash
# Build inside the container (no local Java/Maven needed)
docker exec hbase mvn clean package -f /root/nyc-taxi-hbase/pom.xml
```

### Step 5: Run Student Java APIs

```bash
# Student 1 — Trip Volume & Revenue Report by Vendor
docker exec hbase java -cp /root/nyc-taxi-hbase/target/nyc-taxi-hbase-analytics-1.0.jar \
  com.nyctaxi.hbase.CreateTable

# Student 2 — Insert 12 records + High Revenue Trip Finder (total > $40)
docker exec hbase java -cp /root/nyc-taxi-hbase/target/nyc-taxi-hbase-analytics-1.0.jar \
  com.nyctaxi.hbase.InsertData

# Student 3 — Avg Speed & Fare Audit + Delete Lifecycle (run AFTER Student 2)
docker exec hbase java -cp /root/nyc-taxi-hbase/target/nyc-taxi-hbase-analytics-1.0.jar \
  com.nyctaxi.hbase.GetAndDelete

# Student 4 — Multi-Filter Revenue Dashboard
docker exec hbase java -jar /root/nyc-taxi-hbase/target/nyc-taxi-hbase-analytics-1.0.jar
```

> **Run order**: Student 2 (`InsertData`) must run **before** Students 1, 3, 4.  
> Student 2 inserts the 12 trip records that the other students analyse.

### Step 6: Run HBase Shell Analytics Scripts

```bash
# Common setup (create table + insert all 12 records via HBase shell)
docker exec -it hbase hbase shell < /root/nyc-taxi-hbase/scripts/00_common_setup.hbaseshell

# Student 3 shell commands (filters + delete lifecycle)
docker exec -it hbase hbase shell < /root/nyc-taxi-hbase/scripts/student3_distance_mutations.hbaseshell
```

---

## 🌐 Web UIs (Open in Browser After Starting Containers)

| UI | URL | Description |
|---|---|---|
| HBase Master | http://localhost:16010 | HBase cluster status, regions |
| HDFS NameNode | http://localhost:9871 | HDFS file browser |
| YARN ResourceManager | http://localhost:8089 | MapReduce job history |

---

## 🔧 Troubleshooting

### Container name conflict on restart

```bash
# If you get "container name already in use":
docker stop hbase && docker rm hbase
docker-compose up -d hbase
```

### HBase not ready yet

```bash
# Wait a bit and check logs:
docker logs hbase --tail 30

# Look for: "Master has completed initialization"
# Then retry: docker exec -it hbase hbase shell
```

### Column family error (`NoSuchColumnFamilyException`)

```bash
# If the table has wrong schema, drop and recreate:
docker exec -it hbase hbase shell
# Inside shell:
disable 'nyc_taxi'
drop 'nyc_taxi'
# Then re-run Step 3 above
```

### Port already in use

```bash
# Check which process is using port 2181 (ZooKeeper):
# macOS/Linux:
lsof -i :2181
# Windows PowerShell:
netstat -ano | findstr :2181
```

### Docker not found on macOS

```bash
# Docker Desktop installs to /Applications — add to PATH:
export PATH="/usr/local/bin:$PATH"
# Or use full path:
/usr/local/bin/docker ps
```

---

## 📁 Project Structure

```
bda_capstone2/
├── docker-compose.yml          ← All services: Hadoop, Hive, HBase
├── hadoop.env                  ← Hadoop/YARN configuration
├── SETUP.md                    ← This file
├── README.md                   ← Project overview
│
├── hbase/                      ← BDA Review 2 — HBase analytics
│   ├── pom.xml                 ← Maven build (Java 8, HBase 2.4.18)
│   ├── scripts/
│   │   ├── 00_common_setup.hbaseshell     ← create + put + get (ALL students)
│   │   ├── student1_ddl_ranges.hbaseshell
│   │   ├── student2_payments_reverse.hbaseshell
│   │   ├── student3_distance_mutations.hbaseshell ← YOUR file (Student 3)
│   │   ├── student4_pagination_metrics.hbaseshell
│   │   └── run_all_demos.sh
│   └── src/main/java/com/nyctaxi/hbase/
│       ├── CreateTable.java    ← Student 1: Vendor Revenue Report
│       ├── InsertData.java     ← Student 2: Insert + High Revenue Filter
│       ├── GetAndDelete.java   ← Student 3: Audit + Delete Lifecycle
│       └── ScanWithFilter.java ← Student 4: Multi-Filter Dashboard
│
├── hive/                       ← BDA Review 1 — Hive analytics
├── mapreduce/                  ← MapReduce jobs
└── data/                       ← Dataset placeholder (CSV not committed)
```

---

## 📋 Quick Reference — HBase Shell Commands

```ruby
# Table Management
create 'nyc_taxi', 'trip_info', 'financials', 'timing', 'metrics'
list
describe 'nyc_taxi'
disable 'nyc_taxi'
drop 'nyc_taxi'

# Data Operations
put 'nyc_taxi', 'row_key', 'cf:col', 'value'
get 'nyc_taxi', 'row_key'
get 'nyc_taxi', 'row_key', {COLUMN => 'financials'}
scan 'nyc_taxi'
scan 'nyc_taxi', {LIMIT => 5}
count 'nyc_taxi'
delete 'nyc_taxi', 'row_key', 'financials:tip_amount'
deleteall 'nyc_taxi', 'row_key'

# Filters (Student 3 — your analytics)
scan 'nyc_taxi', {FILTER => "PrefixFilter('1_')", LIMIT => 5}
scan 'nyc_taxi', {
  COLUMNS => ['trip_info', 'metrics'],
  FILTER => "SingleColumnValueFilter('trip_info', 'passenger_count', =, 'regexstring:^[1-2]$')"
}
```

---

*Last updated: October 2026 | 23CSE352 — Big Data Analytics | BDA Project Review 2*
