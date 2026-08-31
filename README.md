<div align="center">
  <h1>🏎️ Formula 1 Data Engineering Project</h1>
  <p><i>An End-to-End Azure Databricks Pipeline using the Medallion Architecture</i></p>

  ![Azure](https://img.shields.io/badge/Azure-0089D6?style=for-the-badge&logo=microsoft-azure&logoColor=white)
  ![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=for-the-badge&logo=databricks&logoColor=white)
  ![PySpark](https://img.shields.io/badge/PySpark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)
  ![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)
  ![Delta Lake](https://img.shields.io/badge/Delta_Lake-00A4EF?style=for-the-badge&logo=databricks&logoColor=white)
</div>

---

> **🚀 Advanced Exploration**: I have explored this project further and implemented **Incremental Data Loading** techniques in another repository. Please check out the advanced implementation here: [Formula1-project-incremental-load](https://github.com/Ganateja19/Formula1-project-incremental-load)

## 🏗️ Project Architecture & Data Flow

This project is built on the highly scalable **Medallion Architecture**, progressing raw files all the way to curated business-level aggregates.

```mermaid
graph LR
    %% Modern Canvas-Friendly Styling
    classDef source fill:#1e1e1e,stroke:#0078d4,stroke-width:2px,color:#fff,rx:5px,ry:5px;
    classDef bronze fill:#cd7f32,stroke:#8b4513,stroke-width:2px,color:#fff,rx:5px,ry:5px;
    classDef silver fill:#c0c0c0,stroke:#808080,stroke-width:2px,color:#111,rx:5px,ry:5px;
    classDef gold fill:#ffd700,stroke:#b8860b,stroke-width:2px,color:#111,rx:5px,ry:5px;
    classDef analytics fill:#00aba9,stroke:#006a68,stroke-width:2px,color:#fff,rx:5px,ry:5px;

    %% Nodes
    A[(fa:fa-cloud Azure ADLS<br/>Landing Zone)]:::source
    B[fa:fa-filter Bronze Layer<br/>Raw Data]:::bronze
    C[fa:fa-cogs Silver Layer<br/>Cleansed Data]:::silver
    D[fa:fa-database Gold Layer<br/>Aggregated Data]:::gold
    E[fa:fa-chart-bar Analytics<br/>Views & Reports]:::analytics

    %% Relationships
    A -->|JSON/CSV Ingestion| B
    B -->|PySpark Transforms| C
    C -->|Fact/Dim Modelling| D
    D -->|SQL Dashboards| E
```

<details>
<summary><b>Click to expand detailed Data Lineage</b></summary>

```mermaid
flowchart TD
    %% Lineage Styles
    classDef default fill:#2c3e50,stroke:#34495e,stroke-width:2px,color:#fff;
    classDef highlight fill:#e74c3c,stroke:#c0392b,stroke-width:2px,color:#fff;

    subgraph Bronze [Raw Ingestion]
        B1(Circuits)
        B2(Races)
        B3(Constructors)
        B4(Drivers)
        B5(Results/Sprints)
    end

    subgraph Silver [Cleansed Transformation]
        S1(Cleaned Circuits)
        S2(Cleaned Races)
        S3(Cleaned Constructors)
        S4(Cleaned Drivers)
        S5(Cleaned Results/Sprints)
    end

    subgraph Gold [Dimensional Aggregation]
        G1[(Races Dim)]
        G2[(Constructors Dim)]
        G3[(Drivers Dim)]
        G4[(Results Fact)]
    end

    B1 --> S1
    B2 --> S2
    B3 --> S3
    B4 --> S4
    B5 --> S5

    S1 -.-> G1
    S2 -.-> G1
    S3 -.-> G2
    S4 -.-> G3
    S5 -.-> G4
```
</details>

---

## 📂 Project Structure

A clean, modular directory structure ensuring maintainability and scalability across the data lifecycle.

| Directory | Purpose | Key Contents |
| :--- | :--- | :--- |
| ⚙️ **`00-common`** | Shared configurations and utilities | `environment_config`, `bronze_helpers` |
| 🛠️ **`01-setup`** | Environment initialization and DDL | Unity Catalog setups, External Locations, Schemas |
| 🥉 **`02-bronze`** | Landing to Bronze ingestion scripts | Raw delta table ingestion logic |
| 🥈 **`03-silver`** | Bronze to Silver transformations | Data cleansing, schema enforcement, null handling |
| 🥇 **`04-gold`** | Silver to Gold aggregations | Dimensional modeling (Facts & Dimensions) |
| 📊 **`05-analytics`** | Business insights and reporting | Standings SQL Views, Dominant Team Analysis |

---

## 🚀 Automated Workflows (Databricks Jobs)

The execution of this pipeline is entirely automated using Databricks Workflows, guaranteeing data reliability and timely processing.

#### 🔄 Data Ingestion and Transformation Workflow
<img src="Job1.png" alt="Databricks Job 1" width="800">

#### 📈 Data Aggregation and Analytics Workflow
<img src="job2.png" alt="Databricks Job 2" width="800">

---

## 📊 Analytics Dashboards

We visualized the insights gathered from our Gold layer to produce interactive business-level dashboards.

<p align="center">
  <img src="F1.png" alt="Analytics Dashboard 1" width="45%">
  &nbsp;
  <img src="F2.png" alt="Analytics Dashboard 2" width="45%">
</p>
<p align="center">
  <img src="F3.png" alt="Analytics Dashboard 3" width="45%">
  &nbsp;
  <img src="F4.png" alt="Analytics Dashboard 4" width="45%">
</p>

---

## 🏆 Standings & Results

Detailed leaderboards constructed from our analytical views. 

#### 🏎️ Driver Standings
<img src="Driver%20Standing.png" alt="Driver Standings" width="800">

#### 🏎️ Constructor Standings
<img src="Constructor%20Standing.png" alt="Constructor Standings" width="800">

---
<div align="center">
  <i>Built with ❤️ for Data Engineering & Formula 1</i>
</div>
