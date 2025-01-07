## Homework Solution 

## Pipelines:
1. Profit
2. Growth
3. Engagement 
4. Aggregate Profit and Growth to Investors
5. Aggregate Engagement to Investors

## Who is the primary and secondary owners of these pipelines?

###  Pipeline 1: Profit
Owners: Finance Team/Risk Team
Secondary Owner: Data Engineering Team
###  Pipeline 2: Growth
Owners: Accounts Team
Secondary Owner: Data Engineering Team
###  Pipeline 3: Engagement
Owners: Software Frontend Team
Secondary Owner: Data Engineering Team
###  Pipeline 4: Aggregate Profit and Growth to Investors
Owners: Business Analytics Team
Secondary Owner: Data Engineering Team
###  Pipeline 5: Aggregate Engagement to Investors
Owners: Business Analytics Team
Secondary Owner: Data Engineering Team


## Team Composition: 4 Data Engineers (rotating roles)
### Engineer 1: Primary contact for Profit and Growth pipelines.
### Engineer 2: Backup for Profit and Growth; primary for Engagement pipeline.
### Engineer 3: Focused on Aggregate Profit and Growth pipeline.
### Engineer 4: Responsible for Aggregate Engagement pipeline.

## Rotation Plan:
* Weekly rotation of roles to ensure fairness.
* Engineers switch responsibilities each Monday to balance workload and knowledge.

## On-Call Schedule (thinking holidays too)

###  Pipeline 1: Profit
* Weekly rotation among the Finance and Data Engineering Teams.
* Weekdays: 8 AM - 6 PM by Finance Team.
* Weekends/Holidays: Data Engineering Team provides escalation support (9 AM - 3 PM).
* Critical Failures: Immediate fixes required for investor reporting.

###  Pipeline 2: Growth
* Weekly ownership rotation among the Accounts Team.
* Weekdays: Business hours only (9 AM - 5 PM).
* Weekends/Holidays: No formal on-call, but any escalations handled by Data Engineering Team during the next working day.

###  Pipeline 3: Engagement
* Weekly rotation among the Data Engineering Team.
* Weekdays: Monitored from 8 AM - 6 PM.
* Weekends/Holidays: Secondary monitoring by Software Frontend Team with Data Engineering backup.
* Weekly onboarding meetings ensure smooth handoffs.

###  Pipeline 4: Aggregate Profit and Growth to Investors
* Focused monitoring during the last week of the month.
* Daily checks during the last week of the month.
* Weekends/Holidays: On-call rotation among Business Analytics and Data Engineering Teams for escalations.

###  Pipeline 5: Aggregate Engagement to Investors
* Weekly reviews conducted by Business Analytics Team.
* Daily monitoring during the last week of the month.
* Weekends/Holidays: Escalations handled jointly by Business Analytics and Data Engineering Teams.

## Runbooks

###  Pipeline 1

1. Pipeline Name: Profit
2. Types of data:
* Revenue from accounts including monthly subscriptions and one-time purchases.
* Operational expenses such as salaries, server costs, and marketing spend.
* Unit-level profit metrics including cost per account and profit margins.
3. Owners: 
* Primary Owner: Finance Team/Risk Team
* Secondary Owner: Data Engineering Team
4. Common Issues:
* Data mismatches with filings requiring accountant verification.
* Missing cost or revenue data leading to incomplete reports.
* Calculation errors due to changes in accounting rules or tax policies.
5. SLA’s:
* Monthly review of data by Finance and Accounts teams.
* Pipeline checks must complete by the 3rd business day of the month.
6. Oncall schedule
* Rotating weekly watch by BI and Data Engineering teams.
* Immediate fixes for critical failures impacting investor reports.

###  Pipeline 2

1. Pipeline Name: Growth
2. Types of data:
* Account changes, including upgrades, downgrades, cancellations, and renewals.
* Revenue growth and churn metrics for investor reporting.
* Time-series data tracking trends across multiple months.
3. Owners: 
* Primary Owner: Accounts Team
* Secondary Owner: Data Engineering Team
4. Common Issues:
* Missing updates in time-series data due to human error.
* Sequence errors caused by out-of-order updates.
* Duplicates or incorrect timestamps in renewal data.
5. SLA’s:
* Weekly updates of account statuses completed by Friday 5 PM.
6. Oncall schedule
* Debugging during working hours.
* No dedicated on-call outside business hours unless investor reports are impacted.

### Pipeline 3
1. Pipeline Name: Engagement
2. Types of data:
* Clickstream data including page views, clicks, and feature usage.
* User activity metrics such as session duration and frequency.
* Aggregated engagement levels by department and region.
3. Owners: 
* Primary Owner: Software Frontend Team
* Secondary Owner: Data Engineering Team
4. Common Issues:
* Delayed or lost clickstream data due to Kafka outages.
* Duplicate events requiring deduplication.
* Incomplete sessions caused by dropped events
5. SLA’s:
* Data arrival within 48 hours.
* Fixes completed within 1 week.
6. Oncall schedule
* Weekly ownership rotation in Data Engineering Team.
* Cross-team collaboration with Software Frontend Team.
* 30-minute onboarding meetings for knowledge transfer.

### Pipeline 4

1. Pipeline Name: Aggregate Profit and Growth to Investors
2. Types of data:
* Aggregated revenue, profit, and growth metrics at the company level.
* Breakdowns by product, region, and customer segment.
3. Owners: 
* Primary Owner: Business Analytics Team
* Secondary Owner: Data Engineering Team
4. Common Issues:
* Failed Spark joins due to large data volumes leading to OOM errors.
* Stale data requiring periodic queue backfills.
* Missing data causing NA or divide-by-zero errors.
5. SLA’s:
* Fixes by end of the month for executive reports.
* Pre-validation checks completed by the 25th of each month.
6. Oncall schedule
* Focused monitoring during the last week of the month.
* Additional support available from BI and Finance teams.

### Pipeline 5
1. Pipeline Name: Aggregate Engagement to Investors
2. Types of data:
* Aggregated engagement metrics across all accounts.
* Comparisons to previous periods for trend analysis.
3. Owners: 
* Primary Owner: Business Analytics Team
* Secondary Owner: Data Engineering Team
4. Common Issues:
* Similar issues as Engagement pipeline (duplicates, missing data).
* Data discrepancies between investor-facing and experimental datasets.
5. SLA’s:
* Fixes by end of the month for executive reports.
* Pre-validation checks completed by the 25th of each month.
6. Oncall schedule
* Focused monitoring during the last week of the month.
* Weekly review meetings with Business Analytics team for issues.