
import psycopg2
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Database Connection
conn_params = {
    'dbname': 'jiracloud',
    'user': '',
    'password': '',
    'host': '',
    'port': ''
}
conn = psycopg2.connect(**conn_params)

# 'INTBIA'
main_data_query = """
SELECT * FROM main_data
WHERE project = 'INTBIA';
"""
main_data_df = pd.read_sql(main_data_query, conn)

transitions_data_query = """
SELECT * FROM transitions_data
WHERE project = 'INTBIA';
"""
transitions_data_df = pd.read_sql(transitions_data_query, conn)

linked_issues_query = """
SELECT * FROM linked_issues
WHERE project = 'INTBIA';
"""
linked_issues_df = pd.read_sql(linked_issues_query, conn)

labels_data_query = """
SELECT * FROM labels_data
WHERE project = 'INTBIA';
"""
labels_data_df = pd.read_sql(labels_data_query, conn)

custom_fields_data_query = """
SELECT * FROM custom_fields_data
WHERE project = 'INTBIA';
"""
custom_fields_data_df = pd.read_sql(custom_fields_data_query, conn)

# Monthly revenue calculation
monthly_revenue_query = """
SELECT DATE_TRUNC('month', date) AS month, SUM(revenue) AS total_revenue
FROM main_data
WHERE project = 'INTBIA'
GROUP BY month
ORDER BY month;
"""
monthly_revenue_df = pd.read_sql(monthly_revenue_query, conn)

# Customer retention analysis based on issue reopens
customer_retention_query = """
SELECT issue_id, COUNT(*) AS reopen_count
FROM transitions_data
WHERE project = 'INTBIA' AND status = 'Reopened'
GROUP BY issue_id
ORDER BY reopen_count DESC;
"""
customer_retention_df = pd.read_sql(customer_retention_query, conn)

# Product performance evaluation using linked_issues
product_performance_query = """
SELECT product, COUNT(*) AS issue_count
FROM linked_issues
WHERE project = 'INTBIA'
GROUP BY product
ORDER BY issue_count DESC;
"""
product_performance_df = pd.read_sql(product_performance_query, conn)

# Issue status distribution by month
issue_status_distribution_query = """
SELECT DATE_TRUNC('month', created_date) AS month, status, COUNT(*) AS issue_count
FROM transitions_data
WHERE project = 'INTBIA'
GROUP BY month, status
ORDER BY month;
"""
issue_status_distribution_df = pd.read_sql(issue_status_distribution_query, conn)
conn.close()

#Monthly Revenue Trend
plt.figure(figsize=(12, 6))
sns.lineplot(x='month', y='total_revenue', data=monthly_revenue_df, marker='o', color='blue')
plt.title('INTBIA Monthly Revenue Trend')
plt.xlabel('Month')
plt.ylabel('Total Revenue')
plt.xticks(rotation=45)
plt.grid(True)
plt.show()

#Top 10 Issues by Reopen Count
plt.figure(figsize=(12, 6))
sns.barplot(x='issue_id', y='reopen_count', data=customer_retention_df.head(10), palette='viridis')
plt.title('Top 10 INTBIA Issues by Reopen Count')
plt.xlabel('Issue ID')
plt.ylabel('Reopen Count')
plt.xticks(rotation=45)
plt.grid(True)
plt.show()

# Product Performance by Issue Count
plt.figure(figsize=(12, 6))
sns.barplot(x='product', y='issue_count', data=product_performance_df, palette='muted')
plt.title('INTBIA Product Performance by Issue Count')
plt.xlabel('Product')
plt.ylabel('Issue Count')
plt.xticks(rotation=45)
plt.grid(True)
plt.show()

#Issue Status Distribution by Month
plt.figure(figsize=(12, 6))
sns.barplot(x='month', y='issue_count', hue='status', data=issue_status_distribution_df, palette='coolwarm')
plt.title('INTBIA Issue Status Distribution by Month')
plt.xlabel('Month')
plt.ylabel('Issue Count')
plt.xticks(rotation=45)
plt.grid(True)
plt.legend(title='Status')
plt.show()

#Labels Distribution for INTBIA Project
plt.figure(figsize=(12, 6))
labels_count_df = labels_data_df['label'].value_counts().reset_index()
labels_count_df.columns = ['label', 'count']
sns.barplot(x='label', y='count', data=labels_count_df.head(10), palette='pastel')
plt.title('Top 10 Labels for INTBIA Project')
plt.xlabel('Label')
plt.ylabel('Count')
plt.xticks(rotation=45)
plt.grid(True)
plt.show()
