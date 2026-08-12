import argparse
import sys
import uuid
import polars as pl
import warnings

from google.cloud import bigquery
from datetime import timezone, datetime
from pathlib import Path

from pandas.core import config_init

warnings.filterwarnings(
    "ignore",
    message="Your application has authenticated using end user credentials"
)

warnings.filterwarnings(
    "ignore",
    message="pkg_resources is deprecated as an API"
)

ENVIRONMENT = {
    "stg" : {
        "project_id" : "tvc-stg"
    },
    "prod": {
        "project_id" : "tvc-prod-core"
    }
}

execution_time = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%S")
def get_all_active_sites_sql(project_id: str) -> str:
    GET_ALL_ACTIVE_SITES = f"""
        SELECT *
            FROM `{project_id}.tvc_facility.site`
        WHERE business_unit_id = @business_unit_id
             and is_active = @active_status
             and site_type = @site_type
        ORDER BY site_id
    """
    return GET_ALL_ACTIVE_SITES

def get_last_n_ccs_for_each_site_sql(project_id: str) -> str:
    GET_LAST_N_CCS_FOR_EACH_SITE =  f"""
    SELECT *
    FROM `{project_id}.tvc_cycle_count.cycle_count_status`
    WHERE
      business_unit_id = @business_unit_id
      AND status = @status
      AND site_id in unnest(@site_ids)
    QUALIFY row_number() OVER (PARTITION BY site_id ORDER BY status_date DESC) <= 3
    ORDER BY site_id, status_date DESC
    """

    return GET_LAST_N_CCS_FOR_EACH_SITE

def get_latest_submit_for_each_cc_sql(project_id: str) -> str:
    GET_LATEST_SUBMIT_FOR_EACH_CC = f"""
        select * from `{project_id}.tvc_cycle_count.cycle_count_status`
        where business_unit_id = @business_unit_id
        and cc_id in unnest(@cc_ids)
        and status = @status
        qualify row_number() over (partition by cc_id order by status_date desc) = 1
        order by site_id, cc_id
    """

    return GET_LATEST_SUBMIT_FOR_EACH_CC

def get_epc_read_data_sql(project_id: str) -> str:
    GET_EPC_READ_DATA = f"""
        select * from `{project_id}.tvc_cycle_count.epc_submit`
        where business_unit_id = @business_unit_id
        and site_id = @site_id
        and cc_id in unnest(@cc_ids)
        and cc_submitted_date in unnest(@cc_submitted_dates)
        and zone_id is not null 
        and sku is not null
    """

    return GET_EPC_READ_DATA

def get_epc_read_data_1_sql(project_id:str) -> str:
    GET_EPC_READS_DATA = f"""
        with epc_submit_data as (
            SELECT *
                FROM `{project_id}.tvc_cycle_count.epc_submit`
            WHERE
              business_unit_id = @business_unit_id 
              AND site_id = @site_id 
              AND cc_id IN UNNEST(@cc_ids)
              AND cc_submitted_date IN UNNEST(@cc_submitted_dates)
              AND sku is not null 
              AND zone_id is not null
        )
        SELECT
          ccs.cc_id,
          st.site_code,
          zn.zone_name,
          es.sku,
          UPPER(to_hex(FROM_BASE64(epc))) AS epc_hex,
          es.read_date,
          es.cc_submitted_date,
          total_count,
          ccs.cc_approved_date
        FROM epc_submit_data es
        INNER JOIN `{project_id}.tvc_facility.site` st
          ON st.site_id = es.site_id AND st.business_unit_id = es.business_unit_id
        LEFT JOIN `{project_id}.tvc_facility.zone` zn
          ON zn.zone_id = es.zone_id
        INNER JOIN `{project_id}.tvc_cycle_count.cycle_count` ccs
          ON ccs.cc_id = es.cc_id 
        WHERE
          es.business_unit_id = @business_unit_id 
          AND st.site_id = @site_id
    """

    return GET_EPC_READS_DATA
def validate_uuid(value: str) -> str:
    try:
        uuid.UUID(value)
        return value
    except ValueError:
        raise argparse.ArgumentTypeError(
            f"'{value}' is not a valid UUID."
        )

def read_bq_to_polars(
    query: str,
    job_config: bigquery.job.QueryJobConfig,
    dry_run: bool = False,
) -> pl.DataFrame | int | pl.Series:
    client = bigquery.Client()

    if dry_run:
        dry_run_config = bigquery.QueryJobConfig(
            dry_run=True,
            use_query_cache=False,
        )

        # Preserve any settings from the supplied job_config
        dry_run_config.query_parameters = job_config.query_parameters
        dry_run_config.default_dataset = job_config.default_dataset
        query_job = client.query(query, job_config=dry_run_config)

        bytes_processed = query_job.total_bytes_processed
        print(f"Estimated bytes processed: {bytes_processed:,}")
        print(f"Estimated MB processed: {bytes_processed / (1024 ** 2):.2f}")
        print(f"Estimated GB processed: {bytes_processed / (1024 ** 3):.2f}")

        return bytes_processed

    arrow_table = (
        client.query(query, job_config=job_config)
        .to_arrow(create_bqstorage_client=True)
    )
    return pl.from_arrow(arrow_table)

def main():
    parser = argparse.ArgumentParser(
        description="EPC submit data extraction subscript"
    )

    parser.add_argument(
        "-e", "--env",
        choices=["stg", "prod"],
        required=True,
        help="Target environment.",
    )

    parser.add_argument(
        "-b", "--buid",
        type=validate_uuid,
        required=True,
        help="Business Unit ID (UUID).",
    )

    parser.add_argument(
        "-s", "--site-ids",
        nargs="+",
        type=uuid.UUID,
        required=False,
        help="List of site id separated by spaces.",
    )


    parser.add_argument(
        "-lb", "--lower-bound-timestamp",
        required=True,
        help="Lower bound timestamp in ISO-8601 format, e.g. 2026-07-08T00:00:00Z"
    )

    parser.add_argument(
        "-ub" ,"--upper-bound-timestamp",
        required=False,
        help="Upper bound timestamp in ISO-8601 format, e.g. 2026-07-08T00:00:00Z"
    )

    args = parser.parse_args()

    config = ENVIRONMENT[args.env]
    project_id = config["project_id"]

    print(f"Environment : {args.env}")
    print(f"Business Unit ID : {args.buid}")
    print(f"Project ID : {project_id}")
    print(f"Site IDS: {args.site_ids}")
    print(f"lower bound timestamp: {args.lower_bound_timestamp}")
    print(f"upper bound timestamp: {args.upper_bound_timestamp}")

    job_config_active_sites = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter(
                "business_unit_id",
                "STRING",
                args.buid
            ),
            bigquery.ScalarQueryParameter(
                "active_status",
                "BOOL",
                True
            ),
            bigquery.ScalarQueryParameter(
                "site_type",
                "STRING",
                "STORE"
            )
        ]
    )
    print("get_all_active_sites_sql")
    print(get_all_active_sites_sql(project_id));

    print("get_last_n_ccs_for_each_site_sql")
    print(get_last_n_ccs_for_each_site_sql(project_id));

    print("get_latest_submit_for_each_cc_sql")
    print(get_latest_submit_for_each_cc_sql(project_id));

    print("get_epc_read_data_sql")
    print(get_epc_read_data_sql(project_id))

    sites_df = read_bq_to_polars(get_all_active_sites_sql(project_id),job_config_active_sites)
    print(sites_df)

    site_ids = sites_df.select(pl.col("site_id")).to_series().to_list()

    if not site_ids:
        print("No active sites found or check the environment")
        sys.exit(0)

    job_config_last_n_cc = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter(
                "business_unit_id",
                "STRING",
                args.buid
            ),
            bigquery.ScalarQueryParameter(
                "status",
                "STRING",
                "COMPLETE"
            ),
            bigquery.ArrayQueryParameter(
                "site_ids",
                "STRING",
                site_ids
            )
        ]
    )

    last_n_cc_for_all_sites = read_bq_to_polars(get_last_n_ccs_for_each_site_sql(project_id), job_config_last_n_cc)
    print(last_n_cc_for_all_sites)

    cc_ids = last_n_cc_for_all_sites.select(pl.col("cc_id")).to_series().to_list()
    if not cc_ids:
        print("No completed CCs found.")
        sys.exit(0)


    job_config_latest_submit_each_cc = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter(
                "business_unit_id",
                "STRING",
                args.buid
            ),
            bigquery.ScalarQueryParameter(
                "status",
                "STRING",
                "SUBMITTED"
            ),
            bigquery.ArrayQueryParameter(
                "cc_ids",
                "STRING",
                cc_ids
            )
        ]
    )

    latest_submits_for_each_ccs = read_bq_to_polars(get_latest_submit_for_each_cc_sql(project_id), job_config_latest_submit_each_cc, False)
    print(latest_submits_for_each_ccs)

    if latest_submits_for_each_ccs.is_empty():
        print("No submit data retrieved, please check the query")
        sys.exit(0)

    output_dir = Path(execution_time) / args.buid
    output_dir.mkdir(parents=True, exist_ok=True)

    for row in sites_df.iter_rows(named=True):
        site_id = row["site_id"]
        site_code = row["site_code"]

        output_site_dir = output_dir / site_code
        output_site_dir.mkdir(parents=True, exist_ok=True)

        latest_submiited_cc_per_site = latest_submits_for_each_ccs.filter(pl.col("site_id") == site_id)
        latest_submitted_cc_ids_per_site = latest_submiited_cc_per_site.select(pl.col("cc_id")).to_series().to_list()
        latest_submitted_cc_submitted_dates_per_site = latest_submiited_cc_per_site.select(pl.col("status_date")).to_series().to_list()

        print(latest_submitted_cc_ids_per_site)
        print(latest_submitted_cc_submitted_dates_per_site)

        job_config_epc_submit = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter(
                    "business_unit_id",
                    "STRING",
                    args.buid
                ),
                bigquery.ScalarQueryParameter(
                    "site_id",
                    "STRING",
                    site_id
                ),
                bigquery.ArrayQueryParameter(
                    "cc_ids",
                    "STRING",
                    latest_submitted_cc_ids_per_site
                ),
                bigquery.ArrayQueryParameter(
                    "cc_submitted_dates",
                    "TIMESTAMP",
                    latest_submitted_cc_submitted_dates_per_site
                )
            ]
        )

        print(get_epc_read_data_1_sql(project_id))
        epc_submit_df = read_bq_to_polars(get_epc_read_data_1_sql(project_id), job_config_epc_submit)
        epc_submit_cc_df = epc_submit_df.group_by(pl.col("cc_id")).agg(pl.len())
        print(epc_submit_cc_df)


        for cc_id in latest_submitted_cc_ids_per_site:
            if not epc_submit_cc_df.is_empty():
                temp = epc_submit_df.filter(pl.col("cc_id") == cc_id)
                if temp.is_empty():
                    print("No epc data")
                    print("Might be an empty cc or check the query for the environment")
                    continue

                cc_approved_date = temp.select(pl.col("cc_approved_date").unique()).item().date().strftime("%Y-%m-%d")
                temp.write_csv(output_site_dir / f"{cc_approved_date}_{cc_id}.csv")
                print(temp.shape)
            else:
                print("No epc data site")

if __name__ == '__main__':
    main()