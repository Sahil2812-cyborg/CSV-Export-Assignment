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

    query = 'SELECT aud.IDENTIFIER, aud.REV, aud.REVTYPE, aud.COLLECTION_POINT_LABEL, aud.STUDY_CALENDAR_EVENT_POINT, aud.ACTIVITY_STATUS, aud.CLINICAL_DIAGNOSIS, aud.CLINICAL_STATUS, aud.DEFAULT_SITE_ID, aud.CODE, aud.VISIT_NAME_PRINT_COPIES, aud.EVENT_POINT_UNIT, aud.CLINICAL_STATUS_ID, aud.CLINICAL_DIAGNOSIS_ID, CONCAT(cu.first_name, \' \', cu.last_name) AS `User`, os.revtstmp FROM catissue_coll_prot_event_aud aud JOIN os_revisions os ON aud.rev = os.rev JOIN catissue_user cu ON os.user_id = cu.identifier WHERE aud.identifier = %s;'
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