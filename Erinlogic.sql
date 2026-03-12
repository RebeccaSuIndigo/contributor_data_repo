/////////////////////////////// RESETS
/*
create or replace table ci_dev.data_engineering.isni_authors clone lake2528_cleansed_dev.prod.isni_authors;
create or replace table ci_dev.data_engineering.isni_isbns clone lake2528_cleansed_dev.prod.isni_isbns;
create or replace table ci_dev.data_engineering.matched_author_isbn13 clone lake2528_cleansed_dev.prod.stg_matched_author_isbn13;
create or replace table ci_dev.data_engineering.stg_matched_author_isbn13 clone lake2528_raw_staging_dev.prod.stg_matched_author_isbn13;
create or replace table ci_dev.data_engineering.stg_matched_author_isbn13_engine_output clone lake2528_raw_staging_dev.prod.stg_matched_author_isbn13_engine_output;

create or replace table ci_dev.data_engineering.stg_matched_author_isbn13 (
	S_ID NUMBER(38,0) autoincrement start 1 increment 1 order,
	ISBN13 VARCHAR(13),
	INDIGO_AUTHOR_ID NUMBER(38,0),
	TRADE_DB_CONTRIBUTOR_ID NUMBER(38,0),
	TRADE_DB_CONTRIBUTOR_ROLE VARCHAR(16777216),
	TRADE_DB_CONTRIBUTOR_ORDER NUMBER(38,0),
	MATCHED_ISNI VARCHAR(16777216),
	NORMAILIZED_AUTHOR_NAME VARCHAR(16777216),
	TRADE_DB_AUTHOR_NAME VARCHAR(16777216),
	ID_SOURCE VARCHAR(16777216),
	DMC_SYNC_HASH_ID NUMBER(38,0),
	DMC_SYNCED BOOLEAN,
	TRANSFORMATION_ID NUMBER(38,0),
	ETL_DATE_INSERTED TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(),
	ETL_UPDATED_DATE TIMESTAMP_NTZ(9)
);

insert into ci_dev.data_engineering.stg_matched_author_isbn13
(
    ISBN13,
    INDIGO_AUTHOR_ID,
    TRADE_DB_CONTRIBUTOR_ID,
    TRADE_DB_CONTRIBUTOR_ROLE,
    TRADE_DB_CONTRIBUTOR_ORDER,
    MATCHED_ISNI,
    NORMAILIZED_AUTHOR_NAME,
    TRADE_DB_AUTHOR_NAME,
    ID_SOURCE,
    DMC_SYNC_HASH_ID,
    DMC_SYNCED,
    TRANSFORMATION_ID
)
select distinct
    isbn13,
    indigo_contr_id as INDIGO_AUTHOR_ID,
    trade_db_contributor_id,
    contributor_role as TRADE_DB_CONTRIBUTOR_ROLE,
    contributor_order as TRADE_DB_CONTRIBUTOR_ORDER,
    matched_isni,
    indigo_contr_nm as NORMAILIZED_AUTHOR_NAME,
    author_nm as TRADE_DB_AUTHOR_NAME,
    ref_to as ID_SOURCE,
    hash(
        trade_db_contributor_id,
        indigo_contr_id,
        indigo_contr_nm,
        matched_isni) 
    as dmc_sync_hash_id,
    true as dmc_synced,
    42 as transformation_id
from ci_dev.data_science.indigo_contributor_book_ref_20260125;

update ci_dev.data_engineering.stg_matched_author_isbn13 set matched_isni = null where matched_isni ilike 'trdb%';
delete from ci_dev.data_engineering.stg_matched_author_isbn13 where indigo_author_id is null or normailized_author_name is null;
/*

/////////////////////////////// TABLE MAPPINGS
indigo_contributor_book_ref_with_flag_202512              -> lake2266_cleansed_dev.prod.matched_author_isbn13
--PRE
ISBN_AUTHORNM_FULL_20260125                               -> stg_new_trade_db_contributors_tmp
FULL_LIST_TITLE_AUTHORS_20260125                          -> stg_new_trade_db_contributor_names_tmp
ISNI_CONVERTED_20260125                                   -> stg_isni_authors_tmp
--POST
contributor_engine_output_unique_20260125                 -> stg_contributor_engine_output_unique_tmp
contributor_engine_output_dups_20260125                   -> stg_contributor_engine_output_dupes_tmp
isbn_contributor_isni_20260125                            -> stg_isbn_contributor_isni_sourced_tmp
ISBN_AUTHORNM_FULL_20260125_excluded_1                    -> stg_isbn_contributor_not_isni_sourced_tmp
ISNI_FULL_AUTHORS_20251201                                -> stg_isni_full_authors_tmp
ISBN_ISNI_AUTHOR_20251201                                 -> stg_isni_full_isbn_authors_tmp
final_isbn_contributor_20260125                           -> stg_final_isbn_contributor_tmp
isbn_contributor_isni_prefilled_isni_20260125             -> stg_isbn_contributor_isni_prefilled_isni_tmp
isbn_contributor_isni_prefilled_isni_20260125_final       -> stg_isbn_contributor_isni_prefilled_isni_final_tmp
ISBN_AUTHORNM_FULL_20260125_excluded_2                    -> stg_isbn_contributor_not_isni_or_prefilled_isni_sourced_tmp
isbn_contributor_isni_20260125_combined_1                 -> stg_isbn_contributor_isni_and_prefilled_isni_sourced_tmp
isbn_contributor_isni_20260125_combined_1_combined_base   -> stg_isbn_contributor_isni_and_prefilled_isni_sourced_base_combined_tmp
unique_isni                                               -> stg_has_unique_isni_tmp
isbn_contributor_isni_tradedbid_20260125                  -> stg_trade_contributors_and_isni_sourced_tmp
isbn_authornm_full_20260125_excluded_3                    -> stg_new_contributors_not_flagged_tmp
isbn_contributor_isni_20260125_combined_2                 -> stg_trade_contributors_prefilled_isni_and_isni_sourced_tmp
isbn_contributor_tradedbid_20260125                       -> stg_trade_contributor_sourced_tmp
isbn_contributor_isni_20260125_combined_3                 -> stg_combined_sources_with_flag_tmp
isbn_contributor_isni_20260125_combined_final             -> stg_matched_author_isbn13

/////////////////////////////// COLUMN MAPPINGS
ISBN13                   -> ISBN13
INDIGO_CONTR_ID          -> INDIGO_AUTHOR_ID
TRADE_DB_CONTRIBUTOR_ID  -> TRADE_DB_CONTRIBUTOR_ID
CONTRIBUTOR_ROLE         -> TRADE_DB_CONTRIBUTOR_ROLE
CONTRIBUTOR_ORDER        -> TRADE_DB_CONTRIBUTOR_ORDER
MATCHED_ISNI             -> MATCHED_ISNI
INDIGO_CONTR_NM          -> NORMAILIZED_AUTHOR_NAME
AUTHOR_NM                -> TRADE_DB_AUTHOR_NAME
REF_TO                   -> ID_SOURCE

*/

use role data_science;
use warehouse data_science;

/********************************************************/
/****************** PRE ENGINE **************************/
/********************************************************/

use schema ci_dev.data_engineering;


/* Get new records from CLEANSED.PROD.TRADE_CONTRIBUTORS */
create or replace temp table stg_new_trade_db_contributors_tmp as
    with
        -- Null out entries such as (Not availble), (Not Supplied), Unknown, Unknown Author, etc
        pre_cleansed as (
            select distinct
                trade_db_contributor_id,
                isbn13,
                contributor_role,
                contributor_order,
                -- Null out entries such as (Not availble), (Not Supplied), Unknown, Unknown Author, etc
                case
                    when contributor_first_name ilike '%(not %)%' then null
                    when contributor_first_name ilike 'unknown' then null
                    when contributor_first_name ilike 'author' then null
                    else contributor_first_name
                end as contributor_first_name,
                case
                    when contributor_last_name ilike '%(not %)%' then null
                    when contributor_last_name ilike 'unknown' then null
                    when contributor_last_name ilike 'author' then null
                    else contributor_last_name
                end as contributor_last_name,
            from cleansed.prod.trade_contributors
        ),
        concatinated as (
            select distinct
                trade_db_contributor_id,
                isbn13,
                contributor_role,
                contributor_order,
                -- Concatinate first and last names
                case
                    -- If both null
                    when contributor_first_name is null and contributor_last_name is null then null
                    -- Snowflake can be unpredictable with nulls, subing in empty strings as a guard
                    when ifnull(contributor_first_name, '') = ifnull(contributor_last_name, '') then contributor_first_name
                    -- If the first or last name is null (empty string) the added space will need to be trimmed
                    else trim(concat(ifnull(contributor_first_name, ''), ' ', ifnull(contributor_last_name, '')))
                end as author_nm
            from pre_cleansed
            where author_nm is not null
        )
        -- Get new records that are not in CLEANSED.PROD.MATCHED_AUTHOR_ISBN13
        select distinct
            trade_db_contributor_id,
            isbn13,
            author_nm,
            contributor_role,
            contributor_order
        from concatinated con
        where not exists (
            select 1
            from  ci_dev.data_engineering.matched_author_isbn13 mai
            where mai.isbn13 = con.isbn13
            and   mai.trade_db_contributor_id = con.trade_db_contributor_id
            and   mai.trade_db_author_name = con.author_nm
        );

/* Get distinct concatinated names to be fed into _PROD_AUTHOR_MATCHING_SP */
create or replace temp table stg_new_trade_db_contributor_names_tmp as
    select distinct author_nm from stg_new_trade_db_contributors_tmp;
    
/* Get INSIs and concatinated names to be fed into _PROD_AUTHOR_MATCHING_SP */
create or replace temp table stg_isni_authors_tmp as
    select distinct
        isni,
        first_name,
        last_name,
        case
            -- If null
            when first_name is null and last_name is null then null
            -- Snowflake can be unpredictable with nulls, subing in empty strings as a guard
            when ifnull(first_name, '') = ifnull(last_name, '') then first_name
            -- If the first or last name is null (empty string) the added space will need to be trimmed
            else trim(concat(ifnull(first_name, ''), ' ', ifnull(last_name, '')))
        end as author
    from ci_dev.data_engineering.isni_authors;

/********************************************************/
/****************** RUN ENGINE **************************/
/********************************************************/

/* import data into the staging table */
/*snowflake.execute({ sqlText:   `call `+v_config_db+`.public._prod_author_matching_sp('`+v_config_db+`',                           --ETL_META DATABASE
             ' ',                           --Stage Location
             'stg_isni_authors_tmp',                      --ISNI Input table
             'stg_new_trade_db_contributor_names_tmp',    --TRADE_CONTRIBUTORS Input table
             'ci_dev.data_engineering.stg_matched_author_isbn13_engine_output',   --Engine output table
             '42',                        --Transformation ID
             'dead-beef-1701',                 --Pipeline Run ID
             null)--Parameter used for testing
             ;*/

/********************************************************/
/****************** POST ENGINE *************************/
/********************************************************/


/* Get authors flagged as unique from matching engine output */
create or replace temp table stg_contributor_engine_output_unique_tmp as
    select * from ci_dev.data_engineering.stg_matched_author_isbn13_engine_output
    where dup_name_flag is null;

/* Get authors flagged as having duplicates from matching engine output */
create or replace temp table stg_contributor_engine_output_dupes_tmp as
    select * from ci_dev.data_engineering.stg_matched_author_isbn13_engine_output
    where dup_name_flag = 'Y';

/* Get and Flag records that will refrence ISNI for Indigo Contributor ID */
create or replace temp table stg_isbn_contributor_isni_sourced_tmp as
    select 
        eout.*, 
        eunq.matched_isni, 
        case 
            when eunq.matched_isni is not null then 'ISNI' 
            else null 
        end as ref_to
    from stg_new_trade_db_contributors_tmp eout
    left join stg_contributor_engine_output_unique_tmp eunq on eout.author_nm = eunq.author_nm;

/* Get reocrds that will not refrence ISNI for Indigo Contributor ID */
create or replace temp table stg_isbn_contributor_not_isni_sourced_tmp as
    select * from stg_new_trade_db_contributors_tmp
    where ISBN13||'_'||AUTHOR_NM not in 
        (select ISBN13||'_'||AUTHOR_NM 
        from stg_isbn_contributor_isni_sourced_tmp 
        where ref_to is not null);

/* Get the authors from CLEANSED.PROD.ISNI_AUTHORS */
create or replace temp table stg_isni_full_authors_tmp as 
    select distinct 
        isni,
        case 
            when first_name is not null and 
                 last_name is not null and 
                 first_name <> last_name 
            then first_name||' '||last_name
            when first_name is not null 
            then first_name
            when last_name is not null 
            then last_name
            else 'UNIDENTIFIED' 
        end as full_name
    from ci_dev.data_engineering.isni_authors;

/* Add in ISBNS */
create or replace temp table stg_isni_full_isbn_authors_tmp as
    select 
        iis.isbn13, 
        iis.isni, 
        ifa.full_name
    -- Distinct out potential dupes
    from (select distinct 
            isbn13, 
            isni 
        from ci_dev.data_engineering.isni_isbns
        ) iis
    inner join stg_isni_full_authors_tmp ifa on iis.isni = ifa.isni;

/* Map ISBNs and contributors */
create or replace temp table stg_final_isbn_contributor_tmp as
    select
        nis.*,
        -- iia.isni is from isni web and eod.matched_isni is from matching engine, and we take the isni from website as higher priority
        coalesce(iia.isni, eod.matched_isni) as final_isni,  
        case 
            when iia.isni is not null and iia.isni = eod.matched_isni then 'Unique Match'
            when iia.isni is not null and iia.isni <> eod.matched_isni then 'Conflict'
            when iia.isni is null then 'No Match Found'
            else 'Undetermined'
        end as match_status
    from stg_isbn_contributor_not_isni_sourced_tmp nis
    -- this table is the table from isni web after manipulation (including isni, full name and isbn13)
    left join stg_isni_full_isbn_authors_tmp iia  
        on  nis.isbn13 = iia.isbn13 
        and nis.author_nm = iia.full_name
    left join stg_contributor_engine_output_dupes_tmp eod
        on  nis.author_nm = eod.author_nm 
        and iia.isni = eod.matched_isni;

/* Get flags for when sourced from Pre-filled ISNI records */
create or replace temp table stg_isbn_contributor_isni_prefilled_isni_tmp as
    select 
        trade_db_contributor_id, 
        isbn13, contributor_role, 
        contributor_order, 
        author_nm, 
        final_isni, 
        'ISNI&PREFILLISNI' as ref_to
    from stg_final_isbn_contributor_tmp
    where match_status = 'Unique Match';

/* De-dupe stg_isbn_contributor_isni_prefilled_isni_tmp */
create or replace temp table stg_isbn_contributor_isni_prefilled_isni_final_tmp as
    select *
    from stg_isbn_contributor_isni_prefilled_isni_tmp
    where (isbn13, contributor_role, contributor_order, author_nm) 
        in (
            select 
                isbn13, 
                contributor_role, 
                contributor_order, 
                author_nm
            from stg_isbn_contributor_isni_prefilled_isni_tmp
            group by isbn13, contributor_role, contributor_order, author_nm
            having count(*) = 1);

/* Get records not sourced from ISNI or Prefilled ISNI */
create or replace temp table stg_isbn_contributor_not_isni_or_prefilled_isni_sourced_tmp as 
    select * from stg_new_trade_db_contributors_tmp
    where isbn13||'_'||author_nm not in (select isbn13||'_'||author_nm from stg_isbn_contributor_isni_sourced_tmp where ref_to is not null)
    and   isbn13||'_'||author_nm not in (select isbn13||'_'||author_nm from stg_isbn_contributor_isni_prefilled_isni_final_tmp where ref_to is not null);

/* Combine ISNI and Prefilled ISNI sourced */
create or replace temp table stg_isbn_contributor_isni_and_prefilled_isni_sourced_tmp as 
    select 
        trade_db_contributor_id, 
        isbn13, 
        contributor_role, 
        contributor_order, 
        author_nm, 
        matched_isni, 
        ref_to 
    from stg_isbn_contributor_isni_sourced_tmp where ref_to is not null
    union
    select 
        trade_db_contributor_id, 
        isbn13, 
        contributor_role, 
        contributor_order, 
        author_nm, 
        final_isni as matched_isni, 
        ref_to
    from stg_isbn_contributor_isni_prefilled_isni_final_tmp where ref_to is not null;

/* Combine ISNI and Prefilled ISNI sourced with CLEANSED.PROD.MATCHED_AUTHOR_ISBN13 */
create or replace temp table stg_isbn_contributor_isni_and_prefilled_isni_sourced_base_combined_tmp as
    select 
        trade_db_contributor_id, 
        isbn13, 
        contributor_role, 
        contributor_order, 
        author_nm, 
        matched_isni, 
        ref_to
    from stg_isbn_contributor_isni_and_prefilled_isni_sourced_tmp
    union 
    select 
        trade_db_contributor_id, 
        isbn13, 
        trade_db_contributor_role as contributor_role, 
        trade_db_contributor_order as contributor_order, 
        trade_db_author_name as author_nm, 
        matched_isni, 
        id_source as ref_to 
        from  ci_dev.data_engineering.matched_author_isbn13
    where ref_to in ('ISNI', 'ISNI&PREFILLISNI');

/* Get unique TRADE_DB_CONTRIBUTOR_ID and ISNI pairs */
create or replace temp table stg_has_unique_isni_tmp as
    select 
        trade_db_contributor_id, 
        max(matched_isni) as matched_isni
    from stg_isbn_contributor_isni_and_prefilled_isni_sourced_base_combined_tmp
    group by trade_db_contributor_id
    having count(distinct matched_isni) = 1;

/* Get records that sourced from TRADE_CONTRIBUTORS and ISNI */
create or replace temp table stg_trade_contributors_and_isni_sourced_tmp as
    select 
        ini.*,
        unq.matched_isni,
        case 
            when unq.matched_isni is not null then 'TRADEDBID&ISNI' 
            else null 
        end as ref_to
    from stg_isbn_contributor_not_isni_or_prefilled_isni_sourced_tmp ini
    left join stg_has_unique_isni_tmp unq on ini.trade_db_contributor_id = unq.trade_db_contributor_id;

/* Get records from new TRADE_CONTRIBUTORS that haven't been flagged yet */
create or replace temp table stg_new_contributors_not_flagged_tmp as 
    select * from stg_new_trade_db_contributors_tmp
        where isbn13||'_'||author_nm not in (select isbn13||'_'||author_nm 
    from stg_isbn_contributor_isni_sourced_tmp 
    where ref_to is not null)
        and   isbn13||'_'||author_nm not in (select isbn13||'_'||author_nm 
    from stg_isbn_contributor_isni_prefilled_isni_final_tmp 
    where ref_to is not null)
        and  isbn13||'_'||author_nm not in (select isbn13||'_'||author_nm 
    from stg_trade_contributors_and_isni_sourced_tmp 
    where ref_to is not null);

/* Combine ISNI, Prefilled ISNI and TRADE_CONTRIBUTORS sourced */
create or replace temp table stg_trade_contributors_prefilled_isni_and_isni_sourced_tmp as 
    select  
        trade_db_contributor_id,
        isbn13,
        contributor_role,
        contributor_order,
        author_nm,
        matched_isni,
        ref_to
    from stg_isbn_contributor_isni_and_prefilled_isni_sourced_tmp
    union
    select
        trade_db_contributor_id,
        isbn13,
        contributor_role,
        contributor_order,
        author_nm,
        matched_isni,
        ref_to
    from stg_trade_contributors_and_isni_sourced_tmp where ref_to is not null;

/* Get flags for records that sourced from TRADE_CONTRIBUTORS */
create or replace temp table stg_trade_contributor_sourced_tmp as
    select 
        trade_db_contributor_id,
        isbn13,
        author_nm,
        contributor_role,
        contributor_order, 
        'trdb'||''||trade_db_contributor_id as matched_isni, 
        'TRADEDBID' as ref_to
    from stg_new_contributors_not_flagged_tmp;

/* Combine all records with a source flag */
create or replace temp table stg_combined_sources_with_flag_tmp as 
    select 
        trade_db_contributor_id,
        isbn13,
        contributor_role,
        contributor_order,
        author_nm,
        matched_isni,
        ref_to
    from stg_trade_contributors_prefilled_isni_and_isni_sourced_tmp
    union
    select 
        trade_db_contributor_id,
        isbn13,
        contributor_role,
        contributor_order,
        author_nm,
        matched_isni,
        ref_to
    from stg_trade_contributor_sourced_tmp 
    where ref_to is not null;

/********************************************************/
/****************** TIDY UP *****************************/
/********************************************************/ 
       
/* truncate the staging table that you should have already created in Snowflake */
truncate table ci_dev.data_engineering.stg_matched_author_isbn13;

/********************************************************/
/****************** STAGE DATA **************************/
/********************************************************/

/* insert data into the staging table */
insert into ci_dev.data_engineering.stg_matched_author_isbn13 (
    isbn13,
    indigo_author_id,
    trade_db_contributor_id,
    trade_db_contributor_role,
    trade_db_contributor_order,
    matched_isni,
    normailized_author_name,
    trade_db_author_name,
    id_source,
    transformation_id,
    pipeline_run_id,
    source_filename
)
select distinct
    isbn13,
    null as indigo_author_id,
    trade_db_contributor_id,
    contributor_role as trade_db_contributor_role,
    contributor_order as trade_db_contributor_order,
    matched_isni,
    null as normailized_author_name,
    author_nm as trade_db_author_name,
    ref_to as id_source,
    42,
    'dead-beef-1701',
    ''
from stg_combined_sources_with_flag_tmp;

/********************************************************/
/****************** TRANSFORMATIONS *********************/
/********************************************************/

/* Backfill INDIGO_AUTHOR_ID from CLEANSED.PROD.MATCHED_AUTHOR_ISBN13 */
update ci_dev.data_engineering.stg_matched_author_isbn13 stg
    set indigo_author_id = cln.indigo_author_id
    from (
        select
            -- The original logic had trdb concatinated with the TRADE_DB_CONTRIBUTOR_ID as a placeholder
            -- when there was no matched ISNI for this to work.
            -- In production this is nulled out of for the cleanesed table, so we recreate it here
            ifnull(matched_isni, concat('trdb', trade_db_contributor_id)) as matched_isni, 
            indigo_author_id
        from ci_dev.data_engineering.matched_author_isbn13
        qualify row_number() over (partition by ifnull(matched_isni, concat('trdb', trade_db_contributor_id)) order by matched_isni) = 1
    ) cln
    where stg.matched_isni = cln.matched_isni;

/* Create new INDIGO_AUTHOR_ID where authors didn't have one before */
update ci_dev.data_engineering.stg_matched_author_isbn13 stg
    set indigo_author_id = new.new_indigo_author_id
    from (
        with max_id as (
            select max(indigo_author_id) as max_indigo_author_id
            from (
                select indigo_author_id from  ci_dev.data_engineering.matched_author_isbn13
                union all
                select indigo_author_id from ci_dev.data_engineering.stg_matched_author_isbn13
            )
        ),
        new_isni as (
            select distinct matched_isni
            from ci_dev.data_engineering.stg_matched_author_isbn13
            where indigo_author_id is null
        )
        select
            nis.matched_isni,
            mid.max_indigo_author_id
            + row_number() over (order by nis.matched_isni) as new_indigo_author_id
        from new_isni nis
        cross join max_id mid
    ) new
    where stg.matched_isni = new.matched_isni
    and stg.indigo_author_id is null;

/* Populate Consolidated Author Names */
update ci_dev.data_engineering.stg_matched_author_isbn13 as stg
    set normailized_author_name = ctl.trade_db_author_name
    from (
    select indigo_author_id, trade_db_author_name
    from (
        select
        indigo_author_id,
        trade_db_author_name,
        count(*) as frequency,
        row_number() over (
            partition by indigo_author_id
            order by
            frequency desc,                    -- most frequent first
            length(trade_db_author_name) desc,  -- tie-breaker 1: longer name wins
            trade_db_author_name asc            -- tie-breaker 2: alphabetical
        ) as rn
        from ci_dev.data_engineering.stg_matched_author_isbn13
        where indigo_author_id is not null
        and trade_db_author_name  is not null
        group by indigo_author_id, trade_db_author_name
    )
    where rn = 1
    ) as ctl
    where stg.indigo_author_id = ctl.indigo_author_id;

/* Generate DMC_SYNC_HASH_IDs */
update ci_dev.data_engineering.stg_matched_author_isbn13
    set dmc_sync_hash_id = hash(
        trade_db_contributor_id,
        indigo_author_id,
        normailized_author_name,
        matched_isni
    );

/* Clear out ISNI palceholders now that all hashes have been calculated */
update ci_dev.data_engineering.stg_matched_author_isbn13
    set matched_isni = null
    where matched_isni ilike 'trdb%';

/* There may be cases where the INDIGO_AUTHOR_ID or the NORMALIZED_AUTHOR_NAME is null. */
delete from ci_dev.data_engineering.stg_matched_author_isbn13 
where indigo_author_id is null 
or    normailized_author_name is null;

/********************************************************/
/****************** DATA LOAD ***************************/
/********************************************************/

/* Get the existing DMC_SYNC_HASH_IDs from CLEANSED to identify changed records after the merge */
create or replace temp table ci_dev.data_engineering.stg_matched_author_isbn13_old_hashes_tmp as
    select dmc_sync_hash_id from  ci_dev.data_engineering.matched_author_isbn13
;

merge into ci_dev.data_engineering.matched_author_isbn13 t using ci_dev.data_engineering.stg_matched_author_isbn13 s on
t.isbn13 = s.isbn13 and
t.indigo_author_id = s.indigo_author_id and
t.trade_db_contributor_id = s.trade_db_contributor_id and
t.trade_db_contributor_order = s.trade_db_contributor_order and
t.trade_db_contributor_role = s.trade_db_contributor_role
when not matched then insert
(    
    isbn13,
    indigo_author_id,
    trade_db_contributor_id,
    trade_db_contributor_role,
    trade_db_contributor_order,
    matched_isni,
    normailized_author_name,
    trade_db_author_name,
    id_source,
    dmc_sync_hash_id,
    transformation_id
)
values
(    
    s.isbn13,
    s.indigo_author_id,
    s.trade_db_contributor_id,
    s.trade_db_contributor_role,
    s.trade_db_contributor_order,
    s.matched_isni,
    s.normailized_author_name,
    s.trade_db_author_name,
    s.id_source,
    s.dmc_sync_hash_id,
    42
)
when matched then update
set
    t.isbn13 = s.isbn13,
    t.indigo_author_id = s.indigo_author_id,
    t.trade_db_contributor_id = s.trade_db_contributor_id,
    t.trade_db_contributor_role = s.trade_db_contributor_role,
    t.trade_db_contributor_order = s.trade_db_contributor_order,
    t.matched_isni = s.matched_isni,
    t.normailized_author_name = s.normailized_author_name,
    t.trade_db_author_name = s.trade_db_author_name,
    t.id_source = s.id_source,
    t.dmc_sync_hash_id = s.dmc_sync_hash_id,
    t.etl_updated_date = current_timestamp,
    t.transformation_id = 42;

/* Set the DMC_SYNCED = FALSE where there are new or changed records */
update ci_dev.data_engineering.matched_author_isbn13 dst
set dmc_synced = false 
where not exists (
    select 1
    from ci_dev.data_engineering.stg_matched_author_isbn13_old_hashes_tmp old
    where dst.dmc_sync_hash_id = old.dmc_sync_hash_id
    );