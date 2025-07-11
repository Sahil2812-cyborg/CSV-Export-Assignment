import mysql.connector
import csv
import pandas as pd


def connect(id):
    db = mysql.connector.connect(
        host = 'localhost',
        user = 'openspecimen',
        password = 'openspecimen',
        database = 'openspecimen'

    )

    cursor = db.cursor()

    print('Connection Successfull')

    query = 'SELECT cp.collection_protocol_id as cpid,cp.identifier as cpe_id,aud.*, CONCAT(cu.first_name, \' \', cu.last_name) AS `User`, os.revtstmp as time FROM catissue_coll_prot_event_aud aud JOIN os_revisions os ON aud.rev = os.rev JOIN catissue_user cu ON os.user_id = cu.identifier join catissue_coll_prot_event cp on cp.identifier = aud.identifier WHERE cp.collection_protocol_id = %s;'
    cursor.execute(query,(id,))

    rows = cursor.fetchall()

    col_names = []

    for i in cursor.description:
        col_names.append(i[0])

    with open('cpe_audit.csv',mode='w',newline='') as file:
        writer = csv.writer(file)
        writer.writerow(col_names)
        writer.writerows(rows)

    cursor.close()
    db.close()



def create_audit(df):

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
    res = create_audit(df)
    res = res[~res['column_name'].str.contains('mod',case=False)]
    res = res[~res['column_name'].str.contains('rev',case=False)]
    res = res[~res['column_name'].str.contains('time',case=False)]
    # print(res)
    res = res.sort_values('timestamp')
    res.to_csv('final_cpe_audit.csv')


