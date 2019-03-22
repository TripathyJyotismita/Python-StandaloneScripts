import idc_sync_ventana
import dbutils
import logutil
import socket
import whcfg
import datetime
from django_email import DjangoEmail
import shlex

#NOTIFICATION_EMAILS = 'data_launch_team@castlighthealth.com,rstewart@castlighthealth.com,sgopal@castlighthealth.com'
NOTIFICATION_EMAILS = 'achopra@castlighthealth.com'
T_ENV = 'preprod'
LOG = logutil.initlog('idc_sync')

class IDCSyncManager(object):
    
    def __init__(self, icf_ids, file_type):
        template_dir = '%s/claims/import/common/templates' % whcfg.providerhome

        self.django_email = DjangoEmail(template_dir, logutil, LOG)
        self.icf_ids = icf_ids
        self.file_type = file_type
        self.label_ids = icf_ids
        self.label_type = file_type
        
    def sync_files(self):
        """call the sync script
        """
        logutil.log(LOG, logutil.INFO, "Started syncing %s for files %s " % (self.icf_ids, self.file_type))
        idc_sync_ventana.main(shlex.split("""-m sync_files -i"-i %s -t %s -d %s -n -m %s" """ % (self.icf_ids, self.file_type, T_ENV, NOTIFICATION_EMAILS)))       
        logutil.log(LOG, logutil.INFO, "%s: Completed calling syncing script for files %s " % (self.icf_ids, self.file_type))

    def sync_claims(self):
        """call the sync script
        """
        logutil.log(LOG, logutil.INFO, "Started syncing %s for files %s " % (self.label_ids, self.label_type))
        idc_sync_ventana.main(shlex.split("""-m sync_claims -i"-l %s -t %s -d %s -n -m %s" """ % (self.label_ids, self.label_type, T_ENV, NOTIFICATION_EMAILS)))
        logutil.log(LOG, logutil.INFO, "%s: Completed calling syncing script for labels %s " % (self.label_ids, self.label_type))
        

if __name__ == "__main__":
    """Calls the sync script for all the files that have been exported and sync_status is 0
    """
    logutil.log(LOG, logutil.INFO, "Starting processing files ready for idc_sync")
    conn = None
    conn = dbutils.getDBConnection(dbname = whcfg.claims_master_schema,
                                   host = whcfg.claims_master_host,
                                   user = whcfg.claims_master_user,
                                   passwd = whcfg.claims_master_password,
                                   useDictCursor = True)
    idc_sync_query="""SELECT GROUP_CONCAT(distinct icf.id ORDER BY icf.id) as icf_ids,uf.file_type
                FROM imported_claim_files icf
                JOIN uploaded_files uf ON uf.prod_imported_claim_file_id = icf.id
                where uf.prod_status = 'claims-exported' and icf.id IN (6424,6425,6426,6427,6578)
                GROUP BY uf.file_type"""

    label_sync_query="""SELECT GROUP_CONCAT(distinct lbl.id ORDER BY lbl.id) as label_ids, lbl.type
                FROM idc_claim_labels lbl
                where lbl.sync_status = 0
                GROUP BY lbl.type"""
    try:
        icf_cursor=conn.cursor()
        icf_cursor.execute(idc_sync_query)
        icf_ids = icf_cursor.fetchall()
        logutil.log(LOG, logutil.INFO, "Identified the files ready for syncing")
        for index, icf_id_row in enumerate(icf_ids):
            icf_id = icf_id_row['icf_ids']
            file_type = icf_id_row['file_type']
            IDCSyncManager(icf_id, file_type).sync_files()

        idc_sync_ventana.IDCSyncFilesController.sent_sync_summary(conn, [each_user.strip() for each_user in NOTIFICATION_EMAILS.split(',')])

        icf_cursor.execute(label_sync_query)
        lbl_ids = icf_cursor.fetchall()
        logutil.log(LOG, logutil.INFO, "Identified the labels ready for syncing")
        for index, lbl_id_row in enumerate(lbl_ids):
            label_id = lbl_id_row['label_ids']
            label_type = lbl_id_row['type']
            #IDCSyncManager(label_id, label_type).sync_claims()

        #idc_sync_ventana.IDCSyncClaimsController.sent_sync_summary(conn, [each_user.strip() for each_user in NOTIFICATION_EMAILS.split(',')])
    except Exception as e:
        logutil.log(LOG, logutil.ERROR, "Error occurred during idc_sync process: %s" % e )
    finally:
        icf_cursor.close()
        if conn:
            conn.close()
