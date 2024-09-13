
import pandas as pd
#load the data
df = pd.read_excel("sample_ecommerce_data.xlsx")


# Inspect data types
print(df.dtypes)


# Identify missing values count
print(df.isnull().sum())


#fill missing values
df.dropna(inplace=True)
df['Transaction Date'] = pd.to_datetime(df['Transaction Date'], errors='coerce')
df['Product Price'] = df['Product Price'].astype(float)
df['Quantity Sold'] = df['Quantity Sold'].astype(int)
df['Total Sales'] = df['Quantity Sold']*df['Product Price'].astype(float)
df['Transaction Month'] =  df['Transaction Date'].dt.month
df['Transaction Month'] = df['Transaction Month'].astype(int)


# Replace spaces with underscores in column names
df.columns = df.columns.str.replace(' ', '_')
df.to_csv('outputdata.csv', index=False)
