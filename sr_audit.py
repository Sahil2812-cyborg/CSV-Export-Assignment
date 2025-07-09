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

    query = "SELECT aud.IDENTIFIER, aud.REV, aud.REVTYPE, aud.SPEC_REQ_LABEL, aud.LINEAGE, aud.SPECIMEN_CLASS, aud.SPECIMEN_TYPE, aud.TISSUE_SITE, aud.TISSUE_SIDE, aud.PATHOLOGICAL_STATUS, aud.STORAGE_TYPE, aud.INITIAL_QUANTITY, aud.CONCENTRATION, aud.COLLECTION_PROCEDURE, aud.COLLECTION_CONTAINER, aud.LABELFORMAT, aud.ACTIVITY_STATUS, aud.COLLECTOR_ID, aud.RECEIVER_ID, aud.SORT_ORDER, aud.CODE, aud.POOLED_SPMN_REQ_ID, aud.LABEL_PRINT_COPIES, aud.ANATOMIC_SITE_ID, aud.LATERALITY_ID, aud.COLLECTION_PROCEDURE_ID, aud.COLLECTION_CONTAINER_ID, aud.SPECIMEN_CLASS_ID, aud.SPECIMEN_TYPE_ID, aud.PATHOLOGICAL_STATUS_ID, aud.DEFAULT_CUSTOM_FIELD_VALUES, aud.PRE_BARCODED_TUBE, CONCAT(cu.first_name, ' ', cu.last_name) AS `User`, os.revtstmp FROM catissue_cp_req_specimen_aud aud JOIN os_revisions os ON aud.rev = os.rev JOIN catissue_user cu ON os.user_id = cu.identifier WHERE aud.identifier = %s;"

    cursor.execute(query,(id,))

    rows = cursor.fetchall()

    col_names = []

    for i in cursor.description:
        col_names.append(i[0])

 

    with open('sr_audit.csv',mode='w',newline='') as file:
        writer = csv.writer(file)
        writer.writerow(col_names)
        writer.writerows(rows)



get_cpe_audit(1)