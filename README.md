# Formula 1 Data Engineering Project (Azure Databricks)

This repository contains a complete Data Engineering project built using **Azure Databricks**, **PySpark**, and **Unity Catalog** to process and analyze Formula 1 data. The project follows the **Medallion Architecture** (Bronze, Silver, and Gold layers) for structuring data in the data lakehouse.

> **Note**: I have explored this project further and implemented **Incremental Data Loading** techniques in another repository. Please check out the advanced implementation here: [Formula1-project-incremental-load](https://github.com/Ganateja19/Formula1-project-incremental-load)

## 🏗️ Project Architecture & Data Flow

The project is structured into multiple stages from raw file ingestion to actionable analytics.

```mermaid
flowchart TD
    %% Define Styles
    classDef storage fill:#282a36,stroke:#bd93f9,stroke-width:2px,color:#f8f8f2;
    classDef bronze fill:#44475a,stroke:#ff5555,stroke-width:2px,color:#f8f8f2;
    classDef silver fill:#44475a,stroke:#f1fa8c,stroke-width:2px,color:#f8f8f2;
    classDef gold fill:#44475a,stroke:#50fa7b,stroke-width:2px,color:#f8f8f2;
    classDef analytics fill:#44475a,stroke:#8be9fd,stroke-width:2px,color:#f8f8f2;

    %% Data Sources
    A[fa:fa-database Azure Data Lake Storage<br>Landing Zone / Volumes]:::storage

    %% Ingestion
    subgraph Bronze [Bronze Layer - Raw Data Ingestion]
        direction TB
        B1(Circuits):::bronze
        B2(Races):::bronze
        B3(Constructors):::bronze
        B4(Drivers):::bronze
        B5(Results & Sprints):::bronze
    end

    %% Transformation
    subgraph Silver [Silver Layer - Cleansed & Transformed]
        direction TB
        S1(Cleaned Circuits):::silver
        S2(Cleaned Races):::silver
        S3(Cleaned Constructors):::silver
        S4(Cleaned Drivers):::silver
        S5(Cleaned Results & Sprints):::silver
    end

    %% Aggregation
    subgraph Gold [Gold Layer - Dimensional Data]
        direction TB
        G1[(Races Dimension)]:::gold
        G2[(Constructors Dimension)]:::gold
        G3[(Drivers Dimension)]:::gold
        G4[(Results Fact)]:::gold
    end

    %% Analytics
    subgraph Analytics [Analytics & Reporting]
        direction TB
        An1[Driver Standings]:::analytics
        An2[Constructor Standings]:::analytics
        An3[Dominant Drivers Analysis]:::analytics
        An4[Dominant Constructors Analysis]:::analytics
    end

    A ==> Bronze
    B1 --> S1
    B2 --> S2
    B3 --> S3
    B4 --> S4
    B5 --> S5

    Silver ==> Gold
    
    S1 -.-> G1
    S2 -.-> G1
    S3 -.-> G2
    S4 -.-> G3
    S5 -.-> G4

    Gold ==> Analytics
```

## 📂 Project Structure

```mermaid
mindmap
  root((Formula 1<br/>Project))
    00_Common
      environment_config
      bronze_helpers
    01_Setup
      Project_Environment_SQL
    02_Bronze
      Ingest_Raw_Files
    03_Silver
      Transform_Data
    04_Gold
      Build_Dimensions
      Build_Facts
    05_Analytics
      Standings_Views
      Dominant_Analysis
```

### 1. Setup & Common (00-common, 01-setup)
- Configuring the Databricks environment.
- Creating the Unity Catalog, External Locations (`databrickscoursextdl1_formula1`), and Schemas (`landing`, `bronze`, `silver`, `gold`).
- Reusable helper scripts.

### 2. Bronze Layer (02-bronze)
Ingesting raw data files (JSON, CSV) from the Landing Zone into the Databricks Bronze layer as Delta Tables.
- Circuits, Races, Constructors, Drivers, Results, and Sprints.

### 3. Silver Layer (03-silver)
Applying transformations, cleansing, handling nulls, and standardizing schemas.
- Data is read from the Bronze layer, transformed using PySpark, and written back to the Silver layer as Delta Tables.

### 4. Gold Layer (04-gold)
Building Fact and Dimension tables for Business Intelligence (BI) and reporting.
- Joining and aggregating Silver layer tables to create business-level metrics.
- `Races`, `Constructors`, `Drivers` dimensions, and `Results` fact table.

### 5. Analytics (05-analytics)
Extracting insights and presenting the data through SQL Views and PySpark DataFrames.
- Driver and Constructor standings.
- Analyzing historically dominant drivers and constructors.

## 🛠️ Technologies Used
* **Cloud Platform**: Microsoft Azure
* **Compute**: Azure Databricks
* **Storage**: Azure Data Lake Storage Gen2 (ADLS Gen2)
* **Data Governance**: Databricks Unity Catalog
* **Language**: PySpark (Python) & SQL
* **Data Format**: Delta Lake (Delta Tables)

## 🚀 Automated Workflows (Databricks Jobs)

The data pipeline execution is orchestrated using Databricks Workflows (Jobs). Below are the visualizations of the job runs demonstrating the successful orchestration of tasks across the Medallion architecture.

### Data Ingestion and Transformation Workflow
![Databricks Job 1](Job1.png)

### Data Aggregation and Analytics Workflow
![Databricks Job 2](job2.png)
