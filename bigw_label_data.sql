WITH
  item_label_info AS (
    SELECT *
    FROM `tvc-prod-core.tvc_item_label.item_label`
    WHERE
      business_unit_id = '11c73b36-89e1-407b-80ce-79d3a69eaca4'
      AND label_name = 'ItemOnLayby'
  ),
  label_hist AS (
    SELECT *
    FROM
      `tvc-prod-core.tvc_item_label.epc_item_label_history_11c73b36-89e1-407b-80ce-79d3a69eaca4`
    WHERE
      event_time >= TIMESTAMP_SUB(current_timestamp(), INTERVAL 1 DAY)
      AND business_unit_id = '11c73b36-89e1-407b-80ce-79d3a69eaca4'
      AND (
        item_label_id_added IN (SELECT item_label_id FROM item_label_info)
        OR item_label_id_removed IN (SELECT item_label_id FROM item_label_info))
  )
SELECT
  label_hist.site_id,
  site.site_name,
  site.site_code,
  label_hist.product_code,
  UPPER(to_hex(from_base64(label_hist.epc))) EPC,
  label_hist.item_label_id_added,
  label_hist.item_label_id_removed,
  item_label_info.label_name,
  label_hist.workflow,
  label_hist.event_time
FROM label_hist
LEFT JOIN `tvc-prod-core.tvc_facility.site` site
  ON site.site_id = label_hist.site_id
JOIN item_label_info
  ON
    item_label_info.item_label_id = label_hist.item_label_id_added
    OR item_label_info.item_label_id = label_hist.item_label_id_removed
ORDER BY event_time DESC
