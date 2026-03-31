# Query Cookbook

Ready-to-run DuckDB SQL queries for the AER Data Pipeline. Connect to the database with:

```python
import duckdb
conn = duckdb.connect('aer_data.duckdb', read_only=True)
```

Or from the command line:

```bash
duckdb aer_data.duckdb
```

---

## 1. Top 10 crude oil producers (latest month)

```sql
SELECT
    TRIM(w.licensee) AS operator,
    COUNT(DISTINCT p.uwi) AS wells,
    ROUND(SUM(p.oil_prod_vol), 0) AS oil_m3
FROM production p
JOIN wells w ON p.uwi = w.uwi
WHERE p.productionmonth = (SELECT MAX(productionmonth) FROM production WHERE province = 'AB')
  AND p.province = 'AB'
  AND p.oil_prod_vol > 0
GROUP BY TRIM(w.licensee)
ORDER BY oil_m3 DESC
LIMIT 10;
```

## 2. Top 10 natural gas producers (latest month)

```sql
SELECT
    TRIM(w.licensee) AS operator,
    COUNT(DISTINCT p.uwi) AS wells,
    ROUND(SUM(p.gas_prod_vol), 0) AS gas_e3m3
FROM production p
JOIN wells w ON p.uwi = w.uwi
WHERE p.productionmonth = (SELECT MAX(productionmonth) FROM production WHERE province = 'AB')
  AND p.province = 'AB'
  AND p.gas_prod_vol > 0
GROUP BY TRIM(w.licensee)
ORDER BY gas_e3m3 DESC
LIMIT 10;
```

## 3. Production decline curve for a specific well

```sql
-- Replace the UWI with your target well
SELECT
    productionmonth,
    oil_prod_vol AS oil_m3,
    gas_prod_vol AS gas_e3m3,
    water_prod_vol AS water_m3
FROM production
WHERE uwi = '00/06-34-042-04W5/0'
ORDER BY productionmonth;
```

## 4. New well licences by operator (last 30 days)

```sql
SELECT
    licensee,
    COUNT(*) AS licences_issued,
    MIN(issue_date) AS earliest,
    MAX(issue_date) AS latest
FROM well_licences
WHERE issue_date >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY licensee
ORDER BY licences_issued DESC
LIMIT 20;
```

## 5. Spud-to-first-production cycle time

```sql
SELECT
    s.uwi,
    s.spud_date,
    MIN(p.productionmonth) AS first_prod_month,
    DATEDIFF('month', s.spud_date, CAST(MIN(p.productionmonth) || '-01' AS DATE)) AS months_to_prod
FROM spud_activity s
JOIN production p ON s.uwi = p.uwi AND p.oil_prod_vol > 0
WHERE s.spud_date >= '2024-01-01'
GROUP BY s.uwi, s.spud_date
ORDER BY months_to_prod
LIMIT 50;
```

## 6. Active vs. suspended vs. abandoned well counts by operator

```sql
SELECT
    TRIM(licensee) AS operator,
    COUNT(*) AS total,
    SUM(CASE WHEN mode NOT IN ('ABD', 'SUS') THEN 1 ELSE 0 END) AS active,
    SUM(CASE WHEN mode = 'SUS' THEN 1 ELSE 0 END) AS suspended,
    SUM(CASE WHEN mode = 'ABD' THEN 1 ELSE 0 END) AS abandoned,
    ROUND(SUM(CASE WHEN mode = 'ABD' THEN 1.0 ELSE 0 END) / COUNT(*) * 100, 1) AS pct_abandoned
FROM wells
WHERE licensee IS NOT NULL AND TRIM(licensee) != ''
GROUP BY TRIM(licensee)
HAVING total > 50
ORDER BY total DESC
LIMIT 20;
```

## 7. Water cut analysis by operator

```sql
SELECT
    TRIM(w.licensee) AS operator,
    COUNT(DISTINCT p.uwi) AS wells,
    ROUND(SUM(p.water_prod_vol) / NULLIF(SUM(p.oil_prod_vol + p.water_prod_vol), 0) * 100, 1) AS water_cut_pct
FROM production p
JOIN wells w ON p.uwi = w.uwi
WHERE p.productionmonth = (SELECT MAX(productionmonth) FROM production WHERE province = 'AB')
  AND p.province = 'AB'
  AND (p.oil_prod_vol > 0 OR p.water_prod_vol > 0)
GROUP BY TRIM(w.licensee)
HAVING wells > 10
ORDER BY water_cut_pct DESC
LIMIT 20;
```

## 8. Gas-oil ratio trends for a well

```sql
-- Replace UWI with target well
SELECT
    productionmonth,
    oil_prod_vol AS oil_m3,
    gas_prod_vol AS gas_e3m3,
    CASE WHEN oil_prod_vol > 0
         THEN ROUND(gas_prod_vol / oil_prod_vol, 1)
         ELSE NULL END AS gor
FROM production
WHERE uwi = '00/06-34-042-04W5/0'
  AND oil_prod_vol > 0
ORDER BY productionmonth;
```

## 9. Operator activity by township and range

```sql
SELECT
    SUBSTRING(uwi, 10, 3) AS township,
    SUBSTRING(uwi, 14, 5) AS range_mer,
    TRIM(licensee) AS operator,
    COUNT(*) AS wells
FROM wells
WHERE fluid LIKE 'CR%'
  AND province = 'AB'
GROUP BY township, range_mer, TRIM(licensee)
HAVING wells >= 5
ORDER BY wells DESC
LIMIT 30;
```

## 10. SK vs. AB production comparison by month

```sql
SELECT
    productionmonth,
    province,
    COUNT(DISTINCT uwi) AS producing_wells,
    ROUND(SUM(oil_prod_vol)) AS total_oil_m3,
    ROUND(SUM(gas_prod_vol)) AS total_gas_e3m3
FROM production
WHERE oil_prod_vol > 0 OR gas_prod_vol > 0
GROUP BY productionmonth, province
ORDER BY productionmonth DESC, province
LIMIT 24;
```

## 11. Pipeline density by substance

```sql
SELECT
    substance,
    status,
    COUNT(*) AS segments,
    ROUND(SUM(length_m) / 1000, 0) AS total_km
FROM sk_pipelines
GROUP BY substance, status
ORDER BY segments DESC;
```

## 12. Month-over-month production changes (decline detection)

```sql
WITH monthly AS (
    SELECT
        uwi,
        productionmonth,
        oil_prod_vol,
        LAG(oil_prod_vol) OVER (PARTITION BY uwi ORDER BY productionmonth) AS prev_oil
    FROM production
    WHERE province = 'AB' AND oil_prod_vol > 0
)
SELECT
    uwi,
    productionmonth,
    ROUND(oil_prod_vol, 0) AS oil_m3,
    ROUND(prev_oil, 0) AS prev_oil_m3,
    ROUND((oil_prod_vol - prev_oil) / NULLIF(prev_oil, 0) * 100, 1) AS mom_change_pct
FROM monthly
WHERE prev_oil > 100
  AND productionmonth = (SELECT MAX(productionmonth) FROM production WHERE province = 'AB')
ORDER BY mom_change_pct ASC
LIMIT 20;
```

## 13. Confidential well tracking

```sql
SELECT *
FROM confidential_wells
WHERE is_currently_confidential = true
ORDER BY first_seen DESC
LIMIT 20;
```

## 14. Cross-reference new licences with existing wells on the same legal

```sql
SELECT
    l.licence_number,
    l.uwi AS new_uwi,
    l.licensee,
    l.issue_date,
    w.uwi AS existing_uwi,
    w.fluid,
    w.mode
FROM well_licences l
JOIN wells w ON SUBSTRING(l.uwi, 4, 11) = SUBSTRING(w.uwi, 4, 11)
    AND l.uwi != w.uwi
WHERE l.issue_date >= CURRENT_DATE - INTERVAL '90 days'
ORDER BY l.issue_date DESC
LIMIT 30;
```

## 15. Provincial production summary by fluid type

```sql
SELECT
    province,
    w.fluid,
    COUNT(DISTINCT p.uwi) AS wells,
    ROUND(SUM(p.oil_prod_vol)) AS oil_m3,
    ROUND(SUM(p.gas_prod_vol)) AS gas_e3m3
FROM production p
JOIN wells w ON p.uwi = w.uwi
WHERE p.productionmonth = (SELECT MAX(productionmonth) FROM production)
GROUP BY province, w.fluid
HAVING oil_m3 > 0 OR gas_e3m3 > 0
ORDER BY oil_m3 DESC;
```

## 16. Year-over-year operator production growth

```sql
WITH yearly AS (
    SELECT
        TRIM(w.licensee) AS operator,
        SUBSTRING(p.productionmonth, 1, 4) AS year,
        SUM(p.oil_prod_vol) AS oil
    FROM production p
    JOIN wells w ON p.uwi = w.uwi
    WHERE p.province = 'AB'
    GROUP BY operator, year
)
SELECT
    operator,
    MAX(CASE WHEN year = '2025' THEN ROUND(oil) END) AS oil_2025,
    MAX(CASE WHEN year = '2024' THEN ROUND(oil) END) AS oil_2024,
    ROUND((MAX(CASE WHEN year = '2025' THEN oil END) -
           MAX(CASE WHEN year = '2024' THEN oil END)) /
           NULLIF(MAX(CASE WHEN year = '2024' THEN oil END), 0) * 100, 1) AS yoy_pct
FROM yearly
WHERE year IN ('2024', '2025')
GROUP BY operator
HAVING oil_2025 > 10000
ORDER BY yoy_pct DESC
LIMIT 20;
```

## 17. Wells with zero production in last 3 months

```sql
WITH recent AS (
    SELECT DISTINCT uwi
    FROM production
    WHERE productionmonth >= (
        SELECT MAX(productionmonth) FROM production
    )::VARCHAR
    AND (oil_prod_vol > 0 OR gas_prod_vol > 0)
)
SELECT
    w.uwi,
    w.name AS well_name,
    TRIM(w.licensee) AS operator,
    w.fluid,
    w.mode,
    MAX(p.productionmonth) AS last_producing_month
FROM wells w
JOIN production p ON w.uwi = p.uwi AND (p.oil_prod_vol > 0 OR p.gas_prod_vol > 0)
WHERE w.uwi NOT IN (SELECT uwi FROM recent)
  AND w.mode NOT IN ('ABD', 'SUS')
  AND w.province = 'AB'
GROUP BY w.uwi, w.name, w.licensee, w.fluid, w.mode
ORDER BY last_producing_month DESC
LIMIT 30;
```

## 18. New operator entrants (first licence in province)

```sql
WITH first_licence AS (
    SELECT
        licensee,
        MIN(issue_date) AS first_date
    FROM well_licences
    GROUP BY licensee
)
SELECT
    licensee,
    first_date
FROM first_licence
WHERE first_date >= CURRENT_DATE - INTERVAL '365 days'
ORDER BY first_date DESC;
```

## 19. Cumulative production leaders (all-time top wells)

```sql
SELECT
    p.uwi,
    w.name AS well_name,
    TRIM(w.licensee) AS operator,
    w.fluid,
    ROUND(SUM(p.oil_prod_vol)) AS cumulative_oil_m3,
    ROUND(SUM(p.gas_prod_vol)) AS cumulative_gas_e3m3,
    MIN(p.productionmonth) AS first_month,
    MAX(p.productionmonth) AS last_month,
    COUNT(DISTINCT p.productionmonth) AS producing_months
FROM production p
JOIN wells w ON p.uwi = w.uwi
WHERE p.province = 'AB'
GROUP BY p.uwi, w.name, w.licensee, w.fluid
ORDER BY cumulative_oil_m3 DESC
LIMIT 20;
```

## 20. Drilling activity summary (recent 90 days)

```sql
SELECT
    drilling_contractor,
    COUNT(*) AS spuds,
    MIN(spud_date) AS earliest,
    MAX(spud_date) AS latest
FROM spud_activity
WHERE spud_date >= CURRENT_DATE - INTERVAL '90 days'
GROUP BY drilling_contractor
ORDER BY spuds DESC
LIMIT 15;
```
