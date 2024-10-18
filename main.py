import logging
from environs import Env
from pocketbase_utils import (
    initialize_pocketbase_client,
    get_filtered_collection,
    get_full_collection,
    update_record_status,
)
from aws_utils import (
    initialize_aws_client,
    download_file_from_s3,
    send_email_with_attachment,
)
from excel_utils import update_excel_sheet

# Configure logging
logging.basicConfig(
    filename='script.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

if __name__ == "__main__":
    logging.info("Starting script execution")
    env = Env()
    env.read_env()

    logging.info("Load the environment variables")
    AWS_ACCESS_KEY_ID = env.str("ACCESS_KEY")
    AWS_SECRET_ACCESS_KEY = env.str("SECRET_KEY")
    AWS_REGION = env.str("REGION")
    AWS_S3_BUCKET_NAME = env.str("AWS_S3_BUCKET_NAME")
    AWS_S3_FILE_NAME = env.str("AWS_S3_FILE_NAME")
    AWS_SES_SENDER = env.str("AWS_SES_SENDER")
    AWS_SES_SUBJECT = env.str("AWS_SES_SUBJECT")
    AWS_SES_BODY = env.str("AWS_SES_BODY")
    POCKETBASE_URL = env.str("POCKETBASE_URL")

    client = initialize_pocketbase_client(POCKETBASE_URL)

    # A queue contains several records. A record contains several fields. One of the fields is form_data.
    project_records = get_filtered_collection(
        client, "projects", query_params={"filter": 'status="uploaded"'}
    )

    if project_records:
        logging.info("Pop out the record")
        first_uploaded_record = project_records[0]

        logging.info("Pop out the form data")
        form_data = first_uploaded_record.form_data

        logging.info("Download the transfer table from pockethost")
        cell_table_records = get_full_collection(client, "cellTable")
        cell_table_dict = {
            record.name: record.cell_index for record in cell_table_records
        }

        logging.info("Merge the form data with transfer table")
        excel_sheet_content = {}
        for name, cell_index in cell_table_dict.items():
            if name in form_data["software"]:
                excel_sheet_content[cell_index] = form_data["software"][name]

        logging.info("Download application template from AWS S3")
        s3_client = initialize_aws_client(
            "s3", AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION
        )
        application_template = download_file_from_s3(
            s3_client, AWS_S3_BUCKET_NAME, AWS_S3_FILE_NAME
        )

        logging.info("Write the table content into Excel sheet")
        updated_application = update_excel_sheet(
            application_template, excel_sheet_content
        )

        logging.info("Get ready to send the email")
        sender = AWS_SES_SENDER
        recipient = form_data["header"]["mailAddresses"]
        subject = AWS_SES_SUBJECT
        body = AWS_SES_BODY
        filename = AWS_S3_FILE_NAME

        ses_client = initialize_aws_client(
            "ses", AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION
        )

        response = send_email_with_attachment(
            ses_client, sender, recipient, subject, body, updated_application, filename
        )

        if response:
            logging.info(f"Email sent! Message ID: {response['MessageId']}")

        record_id = first_uploaded_record.id
        updated_record = update_record_status(client, record_id, "processed")
        if updated_record:
            logging.info(f"Record {record_id} status updated to 'processed'")
        else:
            logging.error(f"Failed to update record {record_id}")
    else:
        logging.info("No record with status 'uploaded' found.")
