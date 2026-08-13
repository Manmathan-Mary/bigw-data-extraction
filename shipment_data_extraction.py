import argparse
import uuid
from msilib import schema
from pathlib import Path

import yaml

from sqlalchemy import create_engine, text
from logging_config import setup_logger
from datetime import datetime, timezone
import polars as pl


logger = setup_logger()
SHIPMENT_DATABASE = "shipment"
FACILITY_DATABASE = "facility"
execution_time = datetime.now().strftime("%Y%m%d_%H%M%S")

def get_all_active_sites(engine, params):
   sql = """
            SELECT
                site.site_id, 
                site.site_name, 
                site.site_code, 
                floor.floor_id, 
                floor.floor_name,
                zone.zone_name, 
                zone.zone_id, 
                zone.zone_type
            FROM
              site
            LEFT JOIN floor using (site_id)
            LEFT JOIN zone using (floor_id)
            WHERE
              site.business_unit_id = :business_unit_id 
              AND is_active = TRUE
              AND site_type = 'STORE'
   """

   return execute_query_return_dataframe(sql, engine, params=params, schema={})

def get_shipment_receive_data(engine, params):
   sql = """
    SELECT
      rh.site_id, 
      rh.zone_id, 
      sh.shipment_barcode_id, 
      ct.container_barcode_id,  
      rhe.*
    FROM
      shipment sh
    JOIN shipment_terminus st ON st.shipment_id = sh.shipment_id
    JOIN container ct ON sh.shipment_id = ct.shipment_id 
    JOIN received_hist rh on rh.container_id = ct.container_id
    JOIN received_hist_epc rhe on rh.received_hist_id = rhe.received_hist_id
    WHERE
      sh.business_unit_id = :business_unit_id
      AND st.terminus_id in unnest(:terminus_ids)
      AND st.terminus_type='DESTINATION'
      AND sh.date_created > :lower_bound_timestamp 
   """

   return execute_query_return_dataframe(sql, engine, params=params, schema={})

def load_config(env):
    with open("config.yml", "r") as f:
        config = yaml.safe_load(f);
    return config[env]

def get_engine(project_id, instance_name, database_name):
    return create_engine(
        f"spanner+spanner:///projects/{project_id}/instances/{instance_name}/databases/{database_name}"
    )

def execute_query_return_dataframe(query: str, engine, params: dict, schema:dict = {}):
    with engine.connect().execution_options(read_only=True) as connection:
        result = connection.execute(text(query), parameters=params)
        if schema:
            return pl.DataFrame(result.fetchall(), schema=schema)
        else:
            return pl.DataFrame(result.fetchall(), schema=result.keys())

def batch_execute_query_return_dataframe(
        query: str,
        engine,
        params: dict,
        schema: dict = None,
        batch_size: int = 100_000
):

    dfs = []
    total_rows = 0
    batch_number = 0

    logger.debug("Starting query execution")

    with engine.connect().execution_options(
            read_only=True,
            stream_results=True
    ) as connection:

        result = connection.execute(text(query), parameters=params)

        columns = list(result.keys())

        logger.debug("Query execution started, fetching rows in batches")

        while True:
            rows = result.fetchmany(batch_size)

            if not rows:
                break

            batch_number += 1
            batch_row_count = len(rows)
            total_rows += batch_row_count

            logger.debug(
                "Processing batch %s | batch_rows=%s | total_rows=%s",
                batch_number,
                batch_row_count,
                total_rows
            )

            batch_df = pl.DataFrame(
                rows,
                schema=schema if schema else columns
            )

            dfs.append(batch_df)

        logger.debug(
            "Finished fetching all batches | total_batches=%s | total_rows=%s",
            batch_number,
            total_rows
        )

    logger.debug("Concatenating %s batch dataframes", len(dfs))

    if dfs:
        final_df = pl.concat(dfs, rechunk=True)
        logger.debug(
            "Final dataframe created | rows=%s | columns=%s",
            final_df.height,
            final_df.width
        )
    else:
        logger.warning("No rows return from query batches")
        final_df = pl.DataFrame(schema=schema)

    return final_df

def parse_utc_timestamp(value):
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))

        if dt.tzinfo is None:
            raise ValueError("Timestamp must contain a timezone")

        return dt.astimezone(timezone.utc)

    except ValueError as e:
        raise argparse.ArgumentTypeError(
            f"Invalid UTC timestamp: {value}. "
            f"Expected format: 2026-07-08T00:00:00Z"
        ) from e

def parse_args():
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
        type=uuid.UUID,
        required=True,
        help="Business Unit ID (UUID).",
    )

    parser.add_argument(
        "-s", "--inbound-site-ids",
        nargs="+",
        type=uuid.UUID,
        required=True,
        help="List of site id separated by spaces.",
    )


    parser.add_argument(
        "-lb", "--lower-bound-timestamp",
        type=parse_utc_timestamp,
        required=True,
        help="Lower bound timestamp in ISO-8601 format, e.g. 2026-07-08T00:00:00Z"
    )

    return parser.parse_args()

def main():
    args = parse_args()
    buid = str(args.buid)
    site_ids = [str(site_id) for site_id in args.inbound_site_ids]
    info = f"""
        env: {args.env}
        buid: {buid}
        inbound_site_ids: {site_ids}
        lower_bound: {args.lower_bound_timestamp}
    """
    logger.info(f"Args:\n{info}")
    config = load_config(args.env)

    logger.info(config)

    PROJECT_ID = config['PROJECT_ID']
    INSTANCE_NAME = config['INSTANCE_NAME']

    shipment_engine = get_engine(PROJECT_ID, INSTANCE_NAME, SHIPMENT_DATABASE)
    facility_engine = get_engine(PROJECT_ID, INSTANCE_NAME, FACILITY_DATABASE)

    params={
        "business_unit_id": buid,
    }
    sites_df = get_all_active_sites(facility_engine, params=params)

    params.update({
        "terminus_ids": site_ids,
        "lower_bound_timestamp": args.lower_bound_timestamp
    })

    receive_df = get_shipment_receive_data(shipment_engine, params)
    receive_data_df = receive_df.join(sites_df, on=['site_id', 'zone_id'], how='inner')
    output_dir = Path(execution_time) / buid
    output_dir.mkdir(parents=True, exist_ok=True)

    for site_id in site_ids:
        receive_data_site_df = receive_data_df.filter(pl.col("site_id") == site_id)
        receive_data_site_df = (receive_data_site_df
                           .select(
            pl.col('site_code'),
            pl.col('site_name'),
            pl.col('zone_name'),
            pl.col('received_hist_id'),
            pl.col('shipment_barcode_id'),
            pl.col('container_barcode_id'),
            pl.col('product_code').alias('sku'),
            pl.col('epc').alias('epc_hex'),
            pl.col('event_time').alias('read_date')
        ).sort('site_code'))
        site_code = receive_data_site_df.unique(pl.col("site_code")).select(pl.col('site_code')).item()
        if not receive_data_site_df.is_empty():
            receive_data_site_df.write_csv(f"{output_dir}/receive_data_{site_code}.csv")


if __name__ == '__main__':
    main()