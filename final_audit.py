import mysql.connector
import csv
import pandas as pd

def connect(id):
    db = mysql.connector.connect(
        host='localhost',
        user='openspecimen',
        password='openspecimen',
        database='openspecimen'
    )

    cursor = db.cursor()
    print('Connection Successful')

    # First query
    query1 = """
        SELECT cp.collection_protocol_id as cpid, cp.identifier as cpe_id,
               aud.*, CONCAT(cu.first_name, ' ', cu.last_name) AS `User`, os.revtstmp as time
        FROM catissue_coll_prot_event_aud aud
        JOIN os_revisions os ON aud.rev = os.rev
        JOIN catissue_user cu ON os.user_id = cu.identifier
        JOIN catissue_coll_prot_event cp ON cp.identifier = aud.identifier
        WHERE cp.collection_protocol_id = %s;
    """

    cursor.execute(query1, (id,))
    rows1 = cursor.fetchall()
    cols1 = [desc[0] for desc in cursor.description]

    df1 = pd.DataFrame(rows1, columns=cols1)
    df1.to_csv('cpe_audit.csv', index=False)

    # Second query
    query2 = """
        SELECT aud.*, os.user_id, os.revtstmp,
               event.identifier as cpeid, event.collection_protocol_id as cpid,
               CONCAT(usr.first_name, ' ', usr.last_name) AS `User`,
               cpv.value as value, pv.value as specimen_type
        FROM catissue_cp_req_specimen_aud aud
        JOIN os_revisions os ON aud.rev = os.rev
        JOIN catissue_cp_req_specimen req ON req.identifier = aud.identifier
        JOIN catissue_coll_prot_event event ON req.collection_protocol_event_id = event.identifier
        JOIN catissue_user usr ON usr.identifier = os.user_id
        JOIN catissue_permissible_value cpv ON cpv.identifier = aud.pathological_status_id
        JOIN catissue_permissible_value pv ON pv.identifier = aud.specimen_type_id
        WHERE req.parent_specimen_id = %s;
    """

    cursor.execute(query2, (id,))
    rows2 = cursor.fetchall()
    cols2 = [desc[0] for desc in cursor.description]

    df2 = pd.DataFrame(rows2, columns=cols2)
    df2.to_csv('sr_audit.csv', index=False)

    # Third query
    query3 = """select aud.*, os.user_id, concat(mod_user.first_name,' ',mod_user.last_name) as 'User', 
    revtstmp as timestamp, concat(cu.first_name ,' ',cu.last_name) as 'Principal Investigator' from cat_collection_protocol_aud aud join os_revisions os on
    os.rev=aud.rev join catissue_user cu on aud.principal_investigator_id = cu.identifier join catissue_user mod_user on os.user_id = mod_user.identifier
    WHERE aud.identifier = %s;
    """

    cursor.execute(query3,(id,))
    rows3 = cursor.fetchall()
    cols3 = [desc[0] for desc in cursor.description]

    df3 = pd.DataFrame(rows3, columns=cols3)
    df3.to_csv('cp_audit.csv', index=False)

    cursor.close()
    db.close()
    print("Both queries executed and results saved to CSV.")
    return df3


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
            if col in [group_col, 'REV', 'user_id', 'timestamp']:  # Skip these columns
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
                    'timestamp': record['timestamp']
                })
    
    return pd.DataFrame(new_cp_records)



def merge_newdf_with_old(df):

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
        timestamp_mapping = df[['REV','timestamp']].drop_duplicates()

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
            # combined_df.to_csv('latest_changes_user_time.csv', index=False)
            print(f"\nCombined changes and new CP records saved to 'latest_changes_user_time.csv'")
            return combined_df
        else:
            # Save only the filtered changes if no new CP records
            # df_filtered.to_csv('latest_changes_user_time.csv', index=False)
            print(f"\nLatest changes saved to 'latest_changes_user_time.csv'")
            return df_filtered
            
    else:
        print("No changes detected or no data to analyze.")



def create_sr_audit(df):
    if df is None or df.empty:
        print("No data found")
        return pd.DataFrame()  # Return empty DataFrame instead of string
    
    print(f"DataFrame shape: {df.shape}")
    print(f"DataFrame columns: {df.columns.tolist()}")
    
    detailed_changes = []
    new_df = df.copy()

    if len(df) <= 1:
        print("Only 1 record, no comparison possible")
        return pd.DataFrame()  # Return empty DataFrame

    # # Sort by timestamp to ensure proper chronological order
    # if 'time' in new_df.columns:
    #     new_df = new_df.sort_values('time').reset_index(drop=True)
    # elif 'revtstmp' in new_df.columns:
    #     new_df = new_df.sort_values('revtstmp').reset_index(drop=True)
    
    shift_df = new_df.shift(1)
    change_mask = (new_df != shift_df) & (new_df.notna() | shift_df.notna())

    for i in range(1, len(new_df)):
        for col in new_df.columns:
            if change_mask.iloc[i][col]:
                old_val = shift_df.iloc[i][col]
                new_val = new_df.iloc[i][col]

                # Get the timestamp column (check which one exists)
                timestamp_col = 'time' if 'time' in new_df.columns else 'revtstmp'
                
                detailed_changes.append({
                    'CP_ID': new_df.iloc[i]['cpid'],
                    'CPE_ID': new_df.iloc[i]['cpeid'],
                    'SR_ID': new_df.iloc[i]['IDENTIFIER'],
                    'column_name': col,
                    'old_value': old_val,
                    'new_value': new_val,
                    'REV': new_df.iloc[i]['REV'] if 'REV' in new_df.columns else new_df.iloc[i]['rev'],
                    'user': new_df.iloc[i]['User'],
                    'timestamp': new_df.iloc[i][timestamp_col]
                })
    
    print(f"Found {len(detailed_changes)} changes")
    return pd.DataFrame(detailed_changes)



def create_cpe_audit(df):

    if df is None:
        return('No data found')
    
    detailed_changes = []

    new_df = df.copy()
    

    if len(df) <= 1:
        print("only 1 record no comparison possible")

    shift_df = new_df.shift(1)

    change_mask = (new_df != shift_df) & (shift_df.notna() | new_df.notna())

    for i in range(1, len(new_df)):
        for col in new_df.columns:
            if change_mask.iloc[i][col]:
                old_val = shift_df.iloc[i][col]
                new_val = new_df.iloc[i][col]
            
                detailed_changes.append(
                    {
                        'CP_ID': new_df.iloc[i]['cpid'],
                        'CPE_ID': new_df.iloc[i]['cpe_id'],
                        'SR_ID' : '',
                        # 'sequence_in_group': i + 1,  # 1-based sequence within group
                        'column_name': col,
                        'old_value': old_val,
                        'new_value': new_val,
                        'REV': new_df.iloc[i]['REV'],  # Add REV for timestamp mapping
                        'user': new_df.iloc[i]['User'],  # Add user_id directly
                        'timestamp': new_df.iloc[i]['time']
                    }
                )

        
    return pd.DataFrame(detailed_changes)


if __name__ == '__main__':

    df = connect(1)
    cp_df = merge_newdf_with_old(df)
    df = pd.read_csv('cpe_audit.csv')
    cpe_res = create_cpe_audit(df)
    cpe_res = cpe_res[~cpe_res['column_name'].str.contains('mod',case=False)]
    cpe_res = cpe_res[~cpe_res['column_name'].str.contains('rev',case=False)]
    cpe_res = cpe_res[~cpe_res['column_name'].str.contains('time',case=False)]
    # print(cpe_res)
    cpe_res = cpe_res.sort_values('timestamp')
    
    # Read the CSV file
    try:
        df = pd.read_csv('sr_audit.csv')
        print("Successfully read CSV file")
    except Exception as e:
        print(f"Error reading CSV: {e}")
        exit(1)
    
    # Create audit comparison
    res = create_sr_audit(df)
    
    # Check if we have results before filtering
    if res.empty:
        print("No changes detected or insufficient data for comparison")
    else:
        print(f"Initial results shape: {res.shape}")
        
        # Filter out unwanted columns
        res = res[~res['column_name'].str.contains('mod', case=False)]
        res = res[~res['column_name'].str.contains('rev', case=False)]
        res = res[~res['column_name'].str.contains('time', case=False)]
        res = res[~res['column_name'].str.contains('user', case=False)]
        res = res[~res['column_name'].str.contains('identifier', case=False)]
    
    # print(f'CPE audit \n{cpe_res.head(5)}')
    # print(f'SR audit \n{res.head(5)}')

    result = pd.concat([cpe_res,res], ignore_index=True)
    result = pd.concat([cp_df,result],ignore_index=True)
    print(result)
    result.to_csv('final_audit.csv', index=False)