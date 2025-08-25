import pandas as pd
import xmlrpc.client
import ssl
from datetime import datetime, timezone
import openpyxl

try:
    from odoosys import url, db, username, password
except ImportError:
    url = "https://nothing"
    db = 'wd250721d1'
    username = 'joel.patrick@gmail.com'
    password = 'kc4yC7792dd5D8'

common = xmlrpc.client.ServerProxy('{}/xmlrpc/2/common'.format(url), use_datetime=True,context=ssl._create_unverified_context())
uid = common.authenticate(db, username, password, {})
models = xmlrpc.client.ServerProxy('{}/xmlrpc/2/object'.format(url), use_datetime=True,context=ssl._create_unverified_context())

# Contacts - Add better error handling and show actual contact names
try:
    polling = models.execute_kw(db, uid, password, 'res.partner', 'search', [[['type', '=', 'contact']]])
    print(f"Found {len(polling)} contacts")
    
    if len(polling) > 0:
        contacts = models.execute_kw(db, uid, password, 'res.partner', 'read', [polling], {'fields': ['name', 'city', 'street']})
        df_contacts = pd.DataFrame(contacts)
        print("df_contacts columns:", df_contacts.columns.tolist())
        print("df_contacts shape:", df_contacts.shape)
        print("Existing contact names in Odoo:")
        if 'name' in df_contacts.columns:
            for name in df_contacts['name']:
                print(f"  - '{name}'")
        else:
            print("  No 'name' column found")
    else:
        df_contacts = pd.DataFrame()
        print("No contacts found in Odoo system")
        
except Exception as e:
    print(f"Error querying contacts: {e}")
    df_contacts = pd.DataFrame()

# Orders - Try different search criteria
try:
    # First, try to get all sale orders without filtering
    polling = models.execute_kw(db, uid, password, 'sale.order', 'search', [[]])
    print(f"Found {len(polling)} sale orders total")
    
    if len(polling) == 0:
        # If no orders exist, try with a different search
        polling = models.execute_kw(db, uid, password, 'sale.order', 'search', [[['state', '!=', 'cancel']]])
        print(f"Found {len(polling)} non-cancelled sale orders")
    
    if len(polling) > 0:
        orders = models.execute_kw(db, uid, password, 'sale.order', 'read', [polling], {'fields': ['name', 'partner_id', 'state','date_order']})
        df_orders = pd.DataFrame(orders)
        print("df_orders columns:", df_orders.columns.tolist())
        print("df_orders shape:", df_orders.shape)
        print("df_orders sample data:")
        print(df_orders.head())
    else:
        df_orders = pd.DataFrame()
        print("No sale orders found in Odoo system")
        
except Exception as e:
    print(f"Error querying sale orders: {e}")
    df_orders = pd.DataFrame()

# Read orders_export.csv from current directory
try:
    df_in = pd.read_csv("orders_export.csv", sep=",")
    print(f"Successfully read {len(df_in)} rows from orders_export.csv")
    print("Available columns in CSV:", df_in.columns.tolist())
except Exception as e:
    print(f"Error reading orders_export.csv: {e}")
    exit(1)

#
# CREATE Orders import candidate
#
print("\n=== PROCESSING ORDERS ===")

# Create and refine orders dataframe, check for pre-existing orders in df_orders and delete these from import.
keep_col = ['Name','Billing Name','Paid at','Lineitem quantity','Lineitem price','Lineitem sku']
df = pd.DataFrame(df_in, columns=keep_col)
print(f"Initial order rows before processing: {len(df)}")

df = df.fillna('')
print(f"After fillna: {len(df)}")

# Remove rows where we don't have essential info (need at least SKU)
df = df[df['Lineitem sku'] != '']
print(f"After filtering rows with missing SKU: {len(df)}")

# FIX: Check if df_orders has data and the correct column name
if not df_orders.empty and 'name' in df_orders.columns:
    df['Exist'] = df['Name'].isin(df_orders['name']).astype(int)
    existing_orders = df['Exist'].sum()
    print(f"Found {existing_orders} existing orders to skip")
    df = df[df['Exist'] != 1]
    del df['Exist']
    print(f"After removing existing orders: {len(df)}")
else:
    print("Warning: df_orders is empty or doesn't have 'name' column. Skipping duplicate check.")

# Create the order structure where only the first line of each order has header info
df_final = pd.DataFrame({
    'Order Reference': df['Name'],
    'Delivery Address': df['Billing Name'], 
    'Customer': 'Shopify',  # Always 'Shopify' as per hand-built example
    'Order Date': df['Paid at'],
    'OrderLines/Quantity': df['Lineitem quantity'],
    'OrderLines/Price_unit': df['Lineitem price'],
    'Order Lines/Product': df['Lineitem sku']
})

# For multi-line orders, clear header info for subsequent lines
df_final['is_first_line'] = ~df_final.duplicated(subset=['Order Reference'], keep='first')
df_final.loc[~df_final['is_first_line'], ['Order Reference', 'Delivery Address', 'Customer', 'Order Date']] = ''

# Drop the helper column
df_final = df_final.drop('is_first_line', axis=1)

# Clean up date format
df_final['Order Date'] = df_final['Order Date'].str.replace(r' -0500', r'')
df_final['Order Date'] = df_final['Order Date'].str.replace(r' -0400', r'')

print(f"Final order rows to export: {len(df_final)}")
if len(df_final) > 0:
    print("Sample order data:")
    print(df_final.head(10))  # Show more lines to see the pattern

df_final.to_csv('02_orders_upload.csv', index=False, header=True)
print(f"Saved {len(df_final)} order lines to 02_orders_upload.csv")

#
# CREATE Shipto_Parties_Upload_Candidate
#
print("\n=== PROCESSING CONTACTS ===")

# Create and refine contacts dataframe - use BILLING info for contacts
keep_col = ['Email','Billing Name','Billing Street','Billing City','Billing Zip','Billing Province','Billing Country','Billing Phone']
df = pd.DataFrame(df_in, columns=keep_col)
print(f"Initial contact rows before processing: {len(df)}")

df = df.fillna('')
print(f"After fillna: {len(df)}")

# Filter out rows with empty Billing Name
df_before_filter = df.copy()
df = df[df['Billing Name'] != ""]
print(f"After filtering empty Billing Name: {len(df)} (removed {len(df_before_filter) - len(df)} rows)")

# Show what billing names we found
print("Billing Names found:")
unique_names = df['Billing Name'].unique()
for name in unique_names:
    print(f"  - '{name}'")

# Rename columns to match Odoo import format
dict_rename = {
    'Billing Name': 'Name',
    'Billing Street': 'Street', 
    'Billing City': 'City',
    'Billing Zip': 'Zip',
    'Billing Province': 'State',
    'Billing Country': 'Country',
    'Billing Phone': 'Phone'
}
df.rename(columns=dict_rename, inplace=True)
print(f"After renaming columns: {len(df)}")

# Remove duplicates within the CSV itself (same person might have multiple orders)
df = df.drop_duplicates(subset=['Name'], keep='first')
print(f"After removing internal duplicates: {len(df)}")

# Add required fields
df['Is a company'] = '0'
df['Address type'] = 'Contact'

# Check for duplicates against existing contacts
print(f"Existing contacts in Odoo: {len(df_contacts) if not df_contacts.empty else 0}")
if not df_contacts.empty and 'name' in df_contacts.columns:
    existing_names = df_contacts['name'].tolist()
    print(f"Existing contact names: {existing_names}")
    
    df['Exist'] = df['Name'].isin(df_contacts['name']).astype(int)
    duplicates_count = df['Exist'].sum()
    print(f"Found {duplicates_count} duplicate contacts")
    
    # Show which ones are duplicates
    if duplicates_count > 0:
        duplicate_names = df[df['Exist'] == 1]['Name'].tolist()
        print(f"Duplicate contact names: {duplicate_names}")
    
    df = df[df['Exist'] != 1]
    del df['Exist']
    print(f"After removing duplicates: {len(df)}")
else:
    print("Warning: df_contacts is empty or doesn't have 'name' column. Skipping duplicate check.")

print(f"Final contact rows to export: {len(df)}")
if len(df) > 0:
    print("Sample contact data:")
    print(df.head())

df.to_csv('01_contacts_upload.csv', index = False, header = True)
print(f"Saved {len(df)} contacts to 01_contacts_upload.csv")

print("Script completed successfully!")
print("Generated files: 01_contacts_upload.csv, 02_orders_upload.csv")
