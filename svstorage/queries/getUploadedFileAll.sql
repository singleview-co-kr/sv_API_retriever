SELECT `id`, `owner_id`, `source_filename`, `file_ext`, `simple_desc`, `status`, `regdt`
FROM `uploaded_file`
WHERE `deleted` = 0