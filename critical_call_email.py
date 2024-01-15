import os
import oracledb
from dotenv import dotenv_values
from datetime import datetime, timedelta, date
import pandas as pd
from mskpymail import send_email

__location__ = os.path.realpath(os.getcwd())
config = dotenv_values(__location__ + "/.env")

oracledb.init_oracle_client(lib_dir="C:/Oracle/instantclient_21_9/")
connection = oracledb.connect(
    user=config["DB_USER"], password=config["DB_PASS"], dsn=config["DB_NAME"]
)

yesterday = date.today() - timedelta(days=1)

from_date = datetime.strftime(yesterday, "%Y-%m-%d") + " 00:00:00"
to_date = datetime.strftime(yesterday, "%Y-%m-%d") + " 23:59:59"

parms = {":from_date": from_date, ":to_date": to_date}

print("from_date", from_date)
print("to_date", to_date)

query = """
SELECT
  "CUST_CALL_REQUEST_REPORT"."ACCESSION",
  "CUST_CALL_REQUEST_REPORT"."DEPT_DISPLAY_NAME",
  "CUST_CALL_REQUEST_REPORT"."MRN",
  "CUST_CALL_REQUEST_REPORT"."PATIENT_NAME",
  "CUST_CALL_REQUEST_REPORT"."LOC_NURSE_UNIT",
  "CUST_CALL_REQUEST_REPORT"."TEST",
  "CUST_CALL_REQUEST_REPORT"."SERVICE_RESOURCE",
  "CUST_CALL_REQUEST_REPORT"."PERFORM_DT_TM",
  "CUST_CALL_REQUEST_REPORT"."COMPLETED_DT_TM",
  "CUST_CALL_REQUEST_REPORT"."TEST_RESULT",
  "CUST_CALL_REQUEST_REPORT"."TEST_RESULT_PREV",
  "CUST_CALL_REQUEST_REPORT"."PERFORM_DT_TM_PREV",
  "CUST_CALL_REQUEST_REPORT"."COMPLETED_BY",
  "CUST_CALL_REQUEST_REPORT"."PROVIDER",
  "CUST_CALL_REQUEST_REPORT"."TAT_MINUTES",
  "CUST_CALL_REQUEST_REPORT"."COMMENTS",
  "CUST_CALL_REQUEST_REPORT"."PROVIDER_ORDER",
  "CUST_CALL_REQUEST_REPORT"."VERIFIED_DT_TM",
  "CUST_CALL_REQUEST_REPORT"."SERVICE_RESOURCE_DEPT",
  "CUST_CALL_REQUEST_REPORT"."CREATED_DT_TM",
  "CUST_CALL_REQUEST_REPORT"."CALL_REQUEST_STATUS",
  "CUST_CALL_REQUEST_REPORT"."SERVICE_RESOURCE_SUBSECTION",
  "CUST_CALL_REQUEST_REPORT"."MICRO_REPORT_TEXT"
FROM
  "CUST_MSK"."CUST_CALL_REQUEST_REPORT" "CUST_CALL_REQUEST_REPORT"
WHERE
  ( ( "CUST_CALL_REQUEST_REPORT"."CALL_REQUEST_STATUS" = 'Not Needed'
      AND ( "CUST_CALL_REQUEST_REPORT"."CREATED_DT_TM" >= cclsql_cnvtdatetimeutc(TO_DATE(:from_date, 'YYYY-MM-DD HH24:MI:SS'), 1, 126,1)
            AND "CUST_CALL_REQUEST_REPORT"."CREATED_DT_TM" < cclsql_cnvtdatetimeutc(TO_DATE(:to_date, 'YYYY-MM-DD HH24:MI:SS'), 1, 126,1) ) )
    OR ( "CUST_CALL_REQUEST_REPORT"."COMPLETED_DT_TM" IS NOT NULL
         AND ( "CUST_CALL_REQUEST_REPORT"."COMPLETED_DT_TM" >= cclsql_cnvtdatetimeutc(TO_DATE(:from_date, 'YYYY-MM-DD HH24:MI:SS'), 1, 126,1)
               AND "CUST_CALL_REQUEST_REPORT"."COMPLETED_DT_TM" < cclsql_cnvtdatetimeutc(TO_DATE(:to_date, 'YYYY-MM-DD HH24:MI:SS'), 1, 126,1) ) ) )
  AND "CUST_CALL_REQUEST_REPORT"."SERVICE_RESOURCE_DEPT" = 'CLM Microbiology'
ORDER BY
  "CUST_CALL_REQUEST_REPORT"."VERIFIED_DT_TM"
"""
cur = connection.cursor()
result = cur.execute(query, parms).fetchall()
columns = [desc[0] for desc in cur.description]
df = pd.DataFrame(result, columns=columns)
cur.close()
connection.close()

df_col = df[
    [
        "ACCESSION",
        "DEPT_DISPLAY_NAME",
        "MRN",
        "PATIENT_NAME",
        "COMPLETED_BY",
        "PROVIDER",
        "PROVIDER_ORDER",
        "TAT_MINUTES",
        "COMMENTS",
        "VERIFIED_DT_TM",
        "MICRO_REPORT_TEXT",
    ]
]


late = df_col[df_col["TAT_MINUTES"] > 60]

for index, row in late.iterrows():
    # Convert the current row to an HTML table
    html_table = row.to_frame().to_html(header=False)

    send_email(
        to=["clarkb@mskcc.org"],
        subject=f"Late Critical Call {yesterday}: {row[0]}",
        body=html_table,
        username=config["AD_USERNAME"],
        password=config["AD_PASSWORD"],
    )
