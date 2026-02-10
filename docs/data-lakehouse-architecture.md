# LOSS-J: Data Lakehouse Architecture

**Version:** 3 | **Date:** 30 January 2026 | **Authors:** Professor Rui Pereira, Ricardo Yang

**Purpose:** This technical specification document establishes standardized architecture principles and data governance guidelines for PDF-to-knowledge-artifact transformation pipelines. It defines immutable ingestion protocols, transformation layer specifications, and consumption-ready output interfaces to ensure reproducible, auditable, and maintainable document processing workflows suitable for Retrieval-Augmented Generation (RAG) systems.

**Scope:** The specification defines mandatory standards for the **Bronze Layer** (immutable raw extraction) and **Gold Layer** (consumption-ready output), while permitting flexibility in internal processing layers (Silver) to accommodate diverse pipeline implementations. This document serves as both a reference implementation and a governance framework for maintaining data quality and consistency across ETL operations.

---

## 1. Architectural Methodology
The preprocessing stage implements a **Data Lakehouse** architecture using the [Medallion](https://www.databricks.com/glossary/medallion-architecture) design pattern. This tiered approach manages the refinement of unstructured data (PDFs) into structured knowledge artifacts. However, in our implementation, the output is unstructured data (Markdowns).

The architecture consists of three logical layers:
* **Layer 1: Bronze** (`01_bronze`) - Immutable raw extraction with content-addressed storage (CAS).
* **Layer 2: Silver** (`02_silver`) - Curated and enriched internal processing (Internal).
* **Layer 3: Gold** (`03_gold`) - Consumption-ready output.

> **Note:** The initial upload/ingestion interface serves as the entry point for source documents. Layer 1 (Bronze) implements immutable storage with content-addressed archiving. Layer 2 (Silver) does not have a fixed structure as it is an internal processing layer. Its schema and organization are customized based on pipeline-specific requirements and may vary between implementations. Only the Bronze and Gold Layer interfaces are standardized for consistency across all pipelines.

---

## 2. Layer 1: Bronze (`01_bronze`)
Layer 1 (Bronze), also known as the **"Landing Zone"**, is the immutable entry point for all source documents. It uses **Content-Addressable Storage (CAS)** to ensure data integrity and deduplication.

### 2.1 Storage Standards
* **Naming Convention:** Files are renamed to the **SHA-256 hash** of their binary content (e.g., `168138...e67.pdf`).
* **Uniqueness:** Duplicate uploads result in the same hash, ensuring physically unique storage.
* **Immutability:** Binary content in this layer is never modified.

### 2.2 Directory Structure
The directory is flat, containing hashed binaries and a single metadata registry.
```bash
data_lakehouse/
└── 01_bronze/
    ├── _catalog.json   # Central Metadata Registry
    ├── 1681386fcb2bccf5c02fd9ea7a95ef...pdf
    ├── 323936109818a136e8d6d47f3b160f...pdf
    └── ...
```

### 2.3 The Bronze Catalog (`_catalog.json`)
Since filenames are hashed, human-readable metadata is decoupled and stored in `_catalog.json`. This file is a dictionary where Keys are file hashes and Values are metadata objects.

Schema Fields:
|Field	|Type	|Nullable	|Description|
|-|-|-|-|
|original_filename	|String	|No	|Filename at upload (e.g., "Report_2024.pdf").|
|title	|String	|No	|Sanitized, human-readable title.|
|description	|String	|No	|Contextual summary.|
|category	|List[String]	|No	|Classification tags (e.g., ["General", "Finance"]).|
|uploaded_by	|String	|No	|User ID or System Agent.|
|uploaded_at	|ISO 8601	|No	|Timestamp of ingestion.|
|active	|Boolean	|No	|Soft-delete flag.|
|processed_at	|ISO 8601	|Yes	|Timestamp of last ETL trigger.|

Example: 
```json
{
    "1681386fcb2bccf5c02fd9ea7a95efd856ef9466501ea854a4f570c5d16a1e67": {
        "original_filename": "2024_Remunerações_Militares.pdf",
        "title": "2024 Remunerações Militares",
        "description": "",
        "category": ["Geral"],
        "uploaded_by": "system",
        "uploaded_at": "2025-12-20T22:21:29.271299",
        "active": true,
        "processed_at": "2025-12-20T22:21:29.271301"
    },
    ...
}
```

---

## 3. Layer 2: Silver (`02_silver`)
Layer 2 (Silver) is the internal processing layer where curated and enriched data is prepared for final consumption. This layer does not have a standardized structure, allowing flexibility for pipeline-specific implementations.

---

## 4. Layer 3: Gold Layer (03_gold)
The Gold Layer is the final output optimized for RAG indexing. It is organized into **Document Bundles**, where each document is expanded into a dedicated directory containing processed text, extracted assets, and enrichment data.

### 4.1 Directory Structure
Each document has a directory named after its **SHA-256 Hash**.

```bash
data_lakehouse/
└── 03_gold/
    ├── _catalog.json 
    └── 0bd754bf200c4e019b8b4cb50886c.../
        ├── 0bd754bf...183.md   # 1. Cleaned Content
        ├── 0bd754bf...183.pdf  # 2. Source Copy
        ├── metadata.json       # 3. Image Enrichment Data
        └── images/
            └── 2c81b329d1fceb1369...f88.jpg
```

### 4.2 Bundle Components
* **Content (`.md`):** The clean Markdown text. Image references point to the local images/ directory.
* **Source (`.pdf`):** A copy of the original binary from landing zone.
* **Assets (`images/`):** Extracted figures and charts saved as `.jpg`. Filenames are the hash of the image content.
* **Enrichment (`metadata.json`):** A sidecar file mapping image filenames to AI-generated descriptions.

Enrichment Schema (`metadata.json`):
```json
{
  "images": {
    "2c81b329d1fceb1369...f88.jpg": {
      "description": "Diagram showing organizational structure.",
      "tags": ["diagram", "structure", "military"]
    },
    "1a2b3c4d5e6f7g8h9i0j...k11.jpg": {
      "description": "Personnel organizational chart.",
      "tags": ["chart", "organization"]
    },
    ...
  }
}
```

### 4.3 The Gold Catalog (`_catalog.json`)
Located at `03_gold/_catalog.json`, this registry mirrors the Bronze catalog but includes processing metrics.

Schema Fields:
|Field	|Type	|Nullable	|Description|
|-|-|-|-|
|original_filename	|String	|No	|Filename at upload (e.g., "Report_2024.pdf").|
|title	|String	|No	|Sanitized, human-readable title.|
|description	|String	|No	|Contextual summary.|
|category	|List[String]	|No	|Classification tags (e.g., ["General", "Finance"]).|
|uploaded_by	|String	|No	|User ID or System Agent.|
|uploaded_at	|ISO 8601	|No	|Timestamp of ingestion.|
|processed_at	|ISO 8601	|No	|Timestamp of last ETL processing.|
|images	|Integer	|No	|Count of extracted images in the document bundle.|

Example Entry:
```json
{
    "0bd754bf200c4e019b8b4cb50886ca50081fa589870825e6267a10c84f4fe183": {
        "original_filename": "MAN 2018RHV02.pdf",
        "title": "MAN 2018RHV02 Saídas de Pessoal Militar v1",
        "description": "Manual on personnel departure.",
        "category": ["Geral"],
        "uploaded_by": "system",
        "uploaded_at": "2024-10-05T14:23:45.123456",
        "processed_at": "2024-11-05T14:23:45.123456",
        "images": 3
    },
    ...
}
```


