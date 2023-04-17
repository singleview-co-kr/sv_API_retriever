SELECT `log_srl`, `url`
FROM `scraper_site_log`
WHERE `scrape_date` is null or `scrape_date` < %s
ORDER BY `log_srl` ASC