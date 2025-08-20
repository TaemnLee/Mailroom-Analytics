import pandas as pd
import os

# ============================================================
# Load and clean mailroom package data for 2023–24 and 2024–25
# ============================================================

# Check if both data files exist
if not os.path.exists('mailroomData/packages_2324.csv') or not os.path.exists('mailroomData/packages_2425.csv'):
    raise FileNotFoundError("Missing file: packages_2324.csv or packages_2425.csv in mailroomData directory.")

# Load package data from CSV files
df_2324 = pd.read_csv('mailroomData/packages_2324.csv')
df_2425 = pd.read_csv('mailroomData/packages_2425.csv')

# Convert 'Received' column to datetime format; ignore invalid entries
df_2324['Received'] = pd.to_datetime(df_2324['Received'], format='%a, %d %b %Y %I:%M %p', errors='coerce')
df_2425['Received'] = pd.to_datetime(df_2425['Received'], format='%a, %d %b %Y %I:%M %p', errors='coerce')

# Drop rows with invalid or missing Received timestamps
df_2324.dropna(subset=['Received'], inplace=True)
df_2425.dropna(subset=['Received'], inplace=True)

# Filter out any data before July 1, 2023 (Denison's fiscal year start))
cutoff_date = pd.Timestamp('2023-07-01')
df_2324 = df_2324[df_2324['Received'] >= cutoff_date]
df_2425 = df_2425[df_2425['Received'] >= cutoff_date]

# Add new time-based columns for grouping
for df in [df_2324, df_2425]:
    df['YearMonth'] = df['Received'].dt.to_period('M')   # e.g., '2023-10'
    df['YearWeek'] = df['Received'].dt.to_period('W')   # e.g., '2023-42'
    df['Date'] = df['Received'].dt.date                 # e.g., '2023-10-31'

# ============================================================
# Compute package volumes by month, week, and day
# ============================================================

# Monthly package counts
month_count_2324 = df_2324['YearMonth'].value_counts().sort_index()
month_count_2425 = df_2425['YearMonth'].value_counts().sort_index()

# Weekly package counts
week_count_2324 = df_2324['YearWeek'].value_counts().sort_index()
week_count_2425 = df_2425['YearWeek'].value_counts().sort_index()

# Daily package counts
day_count_2324 = df_2324['Date'].value_counts().sort_index()
day_count_2425 = df_2425['Date'].value_counts().sort_index()

# ============================================================
# Print summary statistics to terminal
# ============================================================

print("📦 Monthly Package Counts — 2023-24:")
print(month_count_2324, '\n')

print("📦 Monthly Package Counts — 2024-25:")
print(month_count_2425, '\n')

print("Total packages — 2023-24:", month_count_2324.sum())
print("Total packages — 2024-25:", month_count_2425.sum())

print("📦 Weekly Package Counts — 2023-24:")
print(week_count_2324, '\n')

print("📦 Weekly Package Counts — 2024-25:")
print(week_count_2425, '\n')

print("📦 Daily Package Counts — 2023-24:")
print(day_count_2324, '\n')

print("📦 Daily Package Counts — 2024-25:")
print(day_count_2425, '\n')

# ============================================================
# Export basic counts for external visualization
# ============================================================

month_count_2324.to_csv('mailroomData/monthly_counts_2324.csv', header=['Count'])
month_count_2425.to_csv('mailroomData/monthly_counts_2425.csv', header=['Count'])

week_count_2324.to_csv('mailroomData/weekly_counts_2324.csv', header=['Count'])
week_count_2425.to_csv('mailroomData/weekly_counts_2425.csv', header=['Count'])

day_count_2324.to_csv('mailroomData/daily_counts_2324.csv', header=['Count'])
day_count_2425.to_csv('mailroomData/daily_counts_2425.csv', header=['Count'])

# ============================================================
# Merge daily counts by year for total volume comparison
# ============================================================

# === NEW: Merge raw package dataframes before aggregation ===
df_combined = pd.concat([df_2324, df_2425])

# Assign academic year based on actual date, not source file
def get_academic_year(date):
    if pd.Timestamp('2023-07-01') <= date <= pd.Timestamp('2024-06-30'):
        return '2023–24'
    elif pd.Timestamp('2024-07-01') <= date <= pd.Timestamp('2025-06-30'):
        return '2024–25'
    else:
        return None

df_combined['academic_year'] = df_combined['Received'].apply(get_academic_year)
df_combined = df_combined[df_combined['academic_year'].notnull()]

# Extract clean date
df_combined['Date'] = df_combined['Received'].dt.date

# Aggregate by Date and academic year
daily_counts = df_combined.groupby(['Date', 'academic_year']).size().reset_index(name='Count')

# Pivot: each academic year becomes a column
merged_daily = daily_counts.pivot(index='Date', columns='academic_year', values='Count').fillna(0).astype(int)

# Reset index and rename columns to match expected output
merged_daily = merged_daily.reset_index()
merged_daily = merged_daily.rename(columns={
    '2023–24': 'count_2324',
    '2024–25': 'count_2425'
})

# Ensure Date column is datetime
merged_daily['Date'] = pd.to_datetime(merged_daily['Date'])

# Sort by date
merged_daily = merged_daily.sort_values(by='Date')

# Export merged daily totals
merged_daily.to_csv('cleanData/merged_by_day.csv', index=False)
print("✅ Exported merged_by_day.csv (fixed academic year logic)")

# ============================================================
# Create daily carrier breakdown with total column
# ============================================================

# Combine both datasets for full-range carrier analysis
df_all = pd.concat([df_2324, df_2425])

# Normalize carrier names (strip spaces, make uppercase)
df_all['Carrier'] = df_all['Carrier'].str.strip().str.upper()

# Map known variations to standard carrier names
carrier_clean_map = {
    'USPS': 'USPS',
    'UNITED STATES POSTAL SERVICE': 'USPS',
    'UPS': 'UPS',
    'UNITED PARCEL SERVICE': 'UPS',
    'FEDEX': 'FEDEX',
    'AMAZON': 'AMAZON'
}

df_all['Carrier'] = df_all['Carrier'].replace(carrier_clean_map)

# Replace missing or unknown carriers with 'OTHER'
df_all['Carrier'] = df_all['Carrier'].fillna('OTHER')
df_all.loc[~df_all['Carrier'].isin(['USPS', 'UPS', 'FEDEX', 'AMAZON']), 'Carrier'] = 'OTHER'

# Group by date and carrier
carrier_daily = df_all.groupby(['Date', 'Carrier']).size().reset_index(name='Count')

# Pivot so each carrier becomes a column
carrier_pivot = carrier_daily.pivot(index='Date', columns='Carrier', values='Count').fillna(0).astype(int)

# Reset index to make Date a column again
carrier_pivot = carrier_pivot.reset_index()

# Add total package count per day by summing across carrier columns
carrier_pivot['Total'] = carrier_pivot[['USPS', 'UPS', 'FEDEX', 'AMAZON', 'OTHER']].sum(axis=1)

# Export carrier-level breakdown
carrier_pivot.to_csv('cleanData/merged_by_day_carrier.csv', index=False)
print("✅ Exported merged_by_day_carrier.csv")
