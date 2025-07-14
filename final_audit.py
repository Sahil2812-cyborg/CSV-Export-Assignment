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

    cursor.close()
    db.close()
    print("Both queries executed and results saved to CSV.")


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

    connect(1)
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
    print(result)