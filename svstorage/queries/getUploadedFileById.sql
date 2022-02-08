SELECT `id`, `owner_id`, `source_filename`, `file_ext`, `secured_filename`
FROM `uploaded_file`
WHERE `id` = %s