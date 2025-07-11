import mysql.connector
import csv
import pandas as pd


def get_cpe_audit(id):
    db = mysql.connector.connect(
        host = 'localhost',
        user = 'openspecimen',
        password = 'openspecimen',
        database = 'openspecimen'
    )

    cursor = db.cursor()
    print('Connection Successful')

    query = "SELECT aud.*, os.user_id,os.revtstmp, event.identifier as cpeid,event.collection_protocol_id as cpid, CONCAT(usr.first_name, ' ', usr.last_name) AS `User`, cpv.value as value, pv.value as specimen_type FROM catissue_cp_req_specimen_aud aud JOIN os_revisions os ON aud.rev = os.rev JOIN catissue_cp_req_specimen req ON req.identifier = aud.identifier JOIN catissue_coll_prot_event event ON req.collection_protocol_event_id = event.identifier JOIN catissue_user usr ON usr.identifier = os.user_id join catissue_permissible_value cpv on cpv.identifier = aud.pathological_status_id join catissue_permissible_value pv on pv.identifier = aud.specimen_type_id where req.parent_specimen_id=%s;"

    cursor.execute(query, (id,))
    rows = cursor.fetchall()

    col_names = []
    for i in cursor.description:
        col_names.append(i[0])

    with open('sr_audit.csv', mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(col_names)
        writer.writerows(rows)

    cursor.close()
    db.close()
    
    return len(rows)  # Return number of rows for debugging


def create_audit(df):
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


if __name__ == '__main__':
    # Get audit data
    row_count = get_cpe_audit(3)
    print(f"Retrieved {row_count} rows from database")
    
    # Read the CSV file
    try:
        df = pd.read_csv('sr_audit.csv')
        print("Successfully read CSV file")
    except Exception as e:
        print(f"Error reading CSV: {e}")
        exit(1)
    
    # Create audit comparison
    res = create_audit(df)
    
    # Check if we have results before filtering
    if res.empty:
        print("No changes detected or insufficient data for comparison")
    else:
        print(f"Initial results shape: {res.shape}")
        
        # Filter out unwanted columns
        # res = res[~res['column_name'].str.contains('mod', case=False)]
        res = res[~res['column_name'].str.contains('rev', case=False)]
        res = res[~res['column_name'].str.contains('time', case=False)]
        res = res[~res['column_name'].str.contains('user', case=False)]
        res = res[~res['column_name'].str.contains('identifier', case=False)]

        res[['old_value','new_value']] = res[['old_value','new_value']].replace({0: False, 1: True})
        print(f"Filtered results shape: {res.shape}")
        
        if not res.empty:
            res = res.sort_values('timestamp')
            res.to_csv('final_sr_audit.csv', index=False)
            print("Results saved to final_sr_audit.csv")
        else:
            print("No relevant changes found after filtering")