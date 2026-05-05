# tecdoc-upload

TecDoc data sync and ETL tooling.


restore:

PGPASSWORD='JH{K%ws*=c9#oCawi5' PGSSLMODE=require bash -c "
gunzip -c tecdoc.sql.gz | \
grep -v 'transaction_timeout' | \
perl -pe 'BEGIN { \$o = \"tecdoc\"; \$n = \"tecdoc_ghe\"; } s/\\Q\$o\\E/\$n/g' | \
docker run --rm -i -e PGPASSWORD='JH{K%ws*=c9#oCawi5' -e PGSSLMODE=require postgres:16 \
psql -h air12p-rds-ereliable.cbao66w2oetu.eu-central-1.rds.amazonaws.com \
-p 5432 -U pguserrec -d ereliable -v ON_ERROR_STOP=1
" > restore.log 2>&1 &



UPDATE tecdoc_gheorghe.t200 set artnr_raw = regexp_replace(artnr, '^0{1,}||[^a-zA-Z0-9]+', '','g') where dlnr='6358';
UPDATE tecdoc_gheorghe.t200 set artnr_raw_vectors = to_tsvector(artnr_raw) where dlnr='6358';