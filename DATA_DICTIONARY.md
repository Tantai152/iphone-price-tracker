# Data Dictionary - iPhone Price Analytics Project

## 1. Fact Table: fact_prices
This table stores the daily recorded price of various iPhone models across different sources.

| Column Name | Data Type | Nullable? | Description |
| :--- | :--- | :--- | :--- |
| `price_id` | BIGSERIAL | NOT NULL | [cite_start]Primary Key - Unique identifier for each price record[cite: 33]. |
| `model_id` | INT | NOT NULL | [cite_start]Foreign Key - Reference to `dim_model(model_id)`[cite: 33]. |
| `source_id` | INT | NOT NULL | [cite_start]Foreign Key - Reference to `dim_source(source_id)` (TGDD, FPT, CellphoneS)[cite: 33]. |
| `date_id` | INT | NOT NULL | [cite_start]Foreign Key - Reference to `dim_date(date_id)`[cite: 33]. |
| `price_vnd` | BIGINT | NOT NULL | [cite_start]Current listed price in Vietnamese Dong[cite: 33]. |
| `is_on_sale` | BOOLEAN | NOT NULL | [cite_start]True if the product is currently discounted[cite: 33]. |
| `discount_pct` | NUMERIC(5,2) | NULL | [cite_start]Percentage of discount compared to the original price[cite: 33]. |
| `is_anomaly` | BOOLEAN | NOT NULL | [cite_start]Flagged by Data Scientist for abnormal price detection[cite: 33]. |
| `scraped_at` | TIMESTAMPTZ | NOT NULL | [cite_start]Actual timestamp when the data was scraped[cite: 33]. |

---

## 2. Dimension Tables

### dim_model
Stores detailed specifications of iPhone models.
| Column Name | Data Type | Description |
| :--- | :--- | :--- |
| `model_id` | SERIAL | Primary Key - Unique identifier for the model. |
| `series` | VARCHAR(50) | [cite_start]Series of iPhone (e.g., iPhone 15, iPhone 14)[cite: 12, 47]. |
| `model_name` | VARCHAR(100) | [cite_start]Specific model name (e.g., Pro Max, Plus). |
| `storage_gb` | INT | Storage capacity in GB (e.g., 128, 256, 512). |

### dim_source
Stores info about retailers/e-commerce platforms.
| Column Name | Data Type | Description |
| :--- | :--- | :--- |
| `source_id` | SERIAL | Primary Key - Unique identifier for the source. |
| `source_name` | VARCHAR(50) | [cite_start]Name of the retailer (e.g., TGDD, FPT Shop, CellphoneS)[cite: 33, 47]. |

### dim_date
Time dimension for historical analysis.
| Column Name | Data Type | Description |
| :--- | :--- | :--- |
| `date_id` | INT | Primary Key (Format: YYYYMMDD). |
| `full_date` | DATE | Standard date format (YYYY-MM-DD). |
| `day` | INT | Day of the month. |
| `month` | INT | Month of the year. |
| `year` | INT | Year. |
| `is_weekend` | BOOLEAN | True if the date falls on Saturday or Sunday. |

---

## 3. ML Output Table: ml_predictions
[cite_start]Table used by the Data Scientist to store price forecasts for the next 7 days[cite: 34, 35].

| Column Name | Data Type | Description |
| :--- | :--- | :--- |
| `prediction_id` | BIGSERIAL | [cite_start]Primary Key[cite: 35]. |
| `model_id` | INT | Foreign Key - Reference to `dim_model(model_id)`[cite: 35]. |
| `source_id` | INT | [cite_start]Foreign Key - Reference to `dim_source(source_id)`[cite: 35]. |
| `forecast_date` | DATE | [cite_start]The specific date being predicted (next 7 days)[cite: 35]. |
| `predicted_price`| BIGINT | [cite_start]The forecasted price in VND[cite: 35]. |
| `lower_bound` | BIGINT | [cite_start]Lower confidence interval (80%)[cite: 35]. |
| `upper_bound` | BIGINT | [cite_start]Upper confidence interval (80%)[cite: 35]. |
| `model_version` | VARCHAR(50) | [cite_start]Version of the model used (e.g., "prophet_v1.2")[cite: 35]. |
| `trained_at` | TIMESTAMPTZ | [cite_start]Timestamp when the model was trained[cite: 35]. |