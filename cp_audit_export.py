import mysql.connector
import csv
import pandas as pd

#Establish Connection
db = mysql.connector.connect(
    host="localhost",
    user="openspecimen",
    password='openspecimen',
    database="openspecimen"
)

cursor = db.cursor()

print("Connection Successful")

#Execute a query
query = "select aud.*, os.user_id, concat(mod_user.first_name,' ',mod_user.last_name) as 'User', revtstmp, concat(cu.first_name ,' ',cu.last_name) as 'Principal Investigator' from cat_collection_protocol_aud aud join os_revisions os on os.rev=aud.rev join catissue_user cu on aud.principal_investigator_id = cu.identifier join catissue_user mod_user on os.user_id = mod_user.identifier;"

cursor.execute(query)

#fetch all rows
row = cursor.fetchall()

#get column names
col_names = []

for i in cursor.description:
    col_names.append(i[0])

# Write to csv
with open('output.csv',mode='w',newline='') as file:
    writer = csv.writer(file)
    writer.writerow(col_names)
    writer.writerows(row)
 
print("Data exported to output.csv successfully!")

# Clean up
cursor.close()
db.close()
df = pd.read_csv('output.csv')


def create_detailed_changes_df(df, group_col='IDENTIFIER'):
    """
    Create a detailed DataFrame with before/after values using pandas shift()
    Only compares with the previous row having the same identifier
    """
    if group_col not in df.columns:
        print(f"Column '{group_col}' not found in DataFrame")
        return None
    
    detailed_changes = []
    
    # Process each group separately - only compare within same identifier
    for identifier, group in df.groupby(group_col, sort=False):
        # Keep original order within the group
        group_df = group.copy()
        
        # Skip if only one record in group
        if len(group_df) <= 1:
            print(f"Identifier '{identifier}': Only 1 record - no comparison possible")
            continue
        
        
        # Create shifted dataframe (previous row values) within the same group
        shifted_df = group_df.shift(1)
        
        # Create mask for changes (handles NaN values properly), gives a table with true if the value was changed else false
        changes_mask = (group_df != shifted_df) & (group_df.notna() | shifted_df.notna())
        
        
        # Process each row starting from index 1 (since index 0 has no previous row)
        for idx in range(1, len(group_df)):
            
            for col in group_df.columns:
                if col == group_col:  # Skip the identifier column itself
                    continue
                    
                if changes_mask.iloc[idx][col]:
                    old_val = shifted_df.iloc[idx][col]
                    new_val = group_df.iloc[idx][col]
                    
                    detailed_changes.append({
                        'CP_ID': identifier,
                        'CPE_ID': None,
                        'SR_ID' : None,
                        # 'sequence_in_group': idx + 1,  # 1-based sequence within group
                        'REV': group_df.iloc[idx]['REV'],  # Add REV for timestamp mapping
                        'column_name': col,
                        'old_value': old_val,
                        'new_value': new_val,
                        'user': group_df.iloc[idx]['User'],
                    })
    
    return pd.DataFrame(detailed_changes)


def create_new_cp_records(df, newcp_indx_list, group_col='IDENTIFIER'):
    """
    Create records for new collection protocols (identifiers with only one record)
    in the same format as the detailed changes DataFrame
    """
    new_cp_records = []
    
    for identifier in newcp_indx_list:
        # Get the single record for this identifier
        record = df[df[group_col] == identifier].iloc[0]
        
        # Create entries for all relevant columns (excluding certain system columns)
        for col in df.columns:
            if col in [group_col, 'REV', 'user_id', 'revtstmp']:  # Skip these columns
                continue
            if col.lower().startswith('mod') or col.lower().startswith('rev'):  # Skip mod/rev columns
                continue
                
            # Only add if the value is not null/empty
            if pd.notna(record[col]) and record[col] != '':
                new_cp_records.append({
                    'CP_ID': identifier,
                    'CPE_ID': None,
                    'SR_ID' : None,
                    'REV': record['REV'],  # Add REV for new records
                    'column_name': col,
                    'old_value': None,  # No old value for new records
                    'new_value': record[col],
                    'user': record['User'],
                    'revtstmp': record['revtstmp']
                })
    
    return pd.DataFrame(new_cp_records)


detailed_changes_df = create_detailed_changes_df(df, 'IDENTIFIER')
print(detailed_changes_df)

newcp_indx_list = []

identifier_count = df['IDENTIFIER'].value_counts()
new_count = [identifier_count[identifier_count == 1].index]
for i in new_count:
    for j in i:
        newcp_indx_list.append(j)

if detailed_changes_df is not None and not detailed_changes_df.empty:
    # Create mapping for timestamp using REV (now available in latest_changes_df)
    timestamp_mapping = df[['REV','revtstmp']].drop_duplicates()

    detailed_changes_df = detailed_changes_df.merge(
        timestamp_mapping,
        left_on='REV',
        right_on='REV',
        how='left'  # Changed to left join to preserve all records
    )

    # Filter out mod/rev columns
    df_filtered = detailed_changes_df[~detailed_changes_df['column_name'].str.contains('mod',case=False)]
    df_filtered = df_filtered[~df_filtered['column_name'].str.contains('rev',case=False)]
    print(df_filtered)


    # Modified: Only drop 'sequence_in_group', keep 'REV' in the final output
    columns_to_drop = ['sequence_in_group']
    df_filtered = df_filtered.drop(columns=columns_to_drop, errors='ignore')
    
    # Create new CP records with the same structure
    new_cp_df = create_new_cp_records(df, newcp_indx_list, 'IDENTIFIER')
    
    # Combine the filtered changes with new CP records
    if not new_cp_df.empty:
        # Ensure both DataFrames have the same columns
        combined_df = pd.concat([df_filtered, new_cp_df], ignore_index=True)
        combined_df = combined_df[~combined_df['column_name'].str.contains('mod',case=False)]
        combined_df = combined_df[~combined_df['column_name'].str.contains('rev',case=False)]

        combined_df[['old_value','new_value']] = combined_df[['old_value','new_value']].replace({0: False, 1: True})

        print(f"\nCombined DataFrame with {len(df_filtered)} changes and {len(new_cp_df)} new CP records:")
        print(combined_df)

        combined_df = combined_df.sort_values('revtstmp')
        
        # Save combined results to CSV
        combined_df.to_csv('latest_changes_user_time.csv', index=False)
        print(f"\nCombined changes and new CP records saved to 'latest_changes_user_time.csv'")
    else:
        # Save only the filtered changes if no new CP records
        df_filtered.to_csv('latest_changes_user_time.csv', index=False)
        print(f"\nLatest changes saved to 'latest_changes_user_time.csv'")
        
else:
    print("No changes detected or no data to analyze.")