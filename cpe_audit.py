import mysql.connector
import csv


def get_cpe_audit(id):
    db = mysql.connector.connect(
        host = 'localhost',
        user = 'openspecimen',
        password = 'openspecimen',
        database = 'openspecimen'

    )

    cursor = db.cursor()

    print('Connection Successfull')

    query = 'SELECT aud.*, CONCAT(first_name, \' \', last_name) AS `User`, revtstmp FROM catissue_coll_prot_event_aud aud JOIN os_revisions os ON aud.rev = os.rev JOIN catissue_user cu ON os.user_id = cu.identifier WHERE aud.identifier = %s;'

    cursor.execute(query,(id,))

    rows = cursor.fetchall()

    col_names = []

    for i in cursor.description:
        col_names.append(i[0])

 

    with open('cpe_audit.csv',mode='w',newline='') as file:
        writer = csv.writer(file)
        writer.writerow(col_names)
        writer.writerows(rows)



get_cpe_audit(1)