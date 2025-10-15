# deel-take-home

# Payment Acceptance Analysis (dbt Project)

This dbt project models and analyzes payment acceptance data from two source datasets: `acceptance_report` and `chargeback_report`.  
The goal is to understand the drivers behind declined payments and analyze country-level acceptance performance.

## Data Architecture

Raw Sources (BigQuery)
├── acceptance_report
└── chargeback_report
│
▼
Staging Layer (Views)
├── staging_acceptance
└── staging_chargeback
│
▼
Analytics Layer (Tables)
├── payment_acceptance
└── acceptance_by_country

## Layers and Logic

### 1. Source Layer
- Defined in `sources.yml`.
- Represents the raw tables loaded into BigQuery.
- Used as the foundation for all downstream models.

### 2. Staging Layer
- Models: `staging_acceptance.sql`, `staging_chargeback.sql`.
- Cleans and standardizes raw data:
  - Renames and casts columns.
  - Validates fields such as `cvv_provided` and `country`.
  - Parses the `rates` column and converts `amount` to `amount_usd`.
- Materialized as views to remain lightweight and easy to recompute.

### 3. Analytics Layer
- Models: `payment_acceptance.sql`, `acceptance_by_country.sql`.
- Joins and aggregates data for analysis.
- Provides metrics such as:
  - Acceptance rates
  - Decline rates
  - Total and converted payment amounts
- Materialized as tables for improved performance in BI tools.

## Final Output

The final analytics tables are designed for BI and reporting use cases:

| Column | Description |
|---------|-------------|
| country | Transaction country |
| total_payments | Number of transactions |
| accepted_payments | Count of successful transactions |
| acceptance_rate | Percentage of accepted payments |
| total_amount_usd | Total transaction amount in USD |


## Design Rationale
- Modular and layered for clarity.
- Traceable data lineage using dbt `ref()` relationships.
- Lightweight staging views and performant analytics tables.

## Next Steps
- Add more dbt tests for data quality (e.g., null, uniqueness, and accepted values).
- Implement incremental models for larger datasets.
- Connect final models to Looker Studio for interactive dashboards.
- Transform the `rates` column to JSON instead of STRING data_type