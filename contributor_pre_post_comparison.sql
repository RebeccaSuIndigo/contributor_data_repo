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


/* Original - Get new records from CLEANSED.PROD.TRADE_CONTRIBUTORS 
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
*/

-- [UPDATED: stg_new_trade_db_contributors_tmp]
-- Original logic built this as a single CTE that excluded records already present in
-- matched_author_isbn13 (treating everything as NEW with no delta awareness).
-- New logic splits this into two steps:
--   1. stg_new_trade_db_contributors_with_delta_tmp (NEW table): classifies every
--      source record as NEW, CHANGED, or SAME against the base table, using a
--      normalized author name (uppercased, whitespace-collapsed) to avoid false
--      CHANGED signals from casing or whitespace differences.
--   2. stg_new_trade_db_contributors_tmp now simply filters the delta table to
--      NEW and CHANGED, keeping logic clean and enabling the two-phase UPDATE pattern
--      in TIDY UP that Original Logic did not have.
-- [NEW] Step 1: Build full contributor list with delta classification.
create or replace temp table stg_new_trade_db_contributors_with_delta_tmp as
    with
        src as (
            select
                trade_db_contributor_id,
                isbn13,
                contributor_role,
                contributor_order,
                case
                    when contributor_first_name is not null and contributor_last_name is not null
                         and contributor_first_name <> contributor_last_name
                    then contributor_first_name||' '||contributor_last_name
                    when contributor_first_name is not null then contributor_first_name
                    when contributor_last_name is not null then contributor_last_name
                    else 'UNIDENTIFIED'
                end as author_nm,
                regexp_replace(
                    trim(upper(regexp_replace(
                        case
                            when contributor_first_name is not null and contributor_last_name is not null
                                 and contributor_first_name <> contributor_last_name
                            then contributor_first_name||' '||contributor_last_name
                            when contributor_first_name is not null then contributor_first_name
                            when contributor_last_name is not null then contributor_last_name
                            else 'UNIDENTIFIED'
                        end,
                        '[\\r\\n\\t]+',
                        ' '
                    ))),
                    ' +',
                    ' '
                ) as author_nm_norm
            from cleansed.prod.trade_contributors
        ),
        base as (
            select
                trade_db_contributor_id,
                isbn13,
                trade_db_contributor_role as contributor_role,
                trade_db_contributor_order as contributor_order,
                trade_db_author_name as author_nm,
                regexp_replace(
                    trim(upper(regexp_replace(trade_db_author_name, '[\\r\\n\\t]+', ' '))),
                    ' +',
                    ' '
                ) as author_nm_norm
            from ci_dev.data_engineering.matched_author_isbn13
        ),
        base_isbn as (
            select distinct isbn13 from base
        ),
        base_role_order as (
            select distinct isbn13, contributor_role, contributor_order from base
        ),
        base_exact as (
            select distinct
                isbn13,
                contributor_role,
                contributor_order,
                trade_db_contributor_id,
                author_nm_norm
            from base
        )
    select
        s.trade_db_contributor_id,
        s.isbn13,
        s.contributor_role,
        s.contributor_order,
        s.author_nm,
        case
            when bi.isbn13 is null then 'NEW'
            when bro.isbn13 is null then 'NEW'
            when be.trade_db_contributor_id is null then 'CHANGED'
            else 'SAME'
        end as delta_flag
    from src s
    left join base_isbn bi
        on s.isbn13 = bi.isbn13
    left join base_role_order bro
        on  s.isbn13 = bro.isbn13
        and s.contributor_role = bro.contributor_role
        and s.contributor_order = bro.contributor_order
    left join base_exact be
        on  s.isbn13 = be.isbn13
        and s.contributor_role = be.contributor_role
        and s.contributor_order = be.contributor_order
        and s.trade_db_contributor_id = be.trade_db_contributor_id
        and s.author_nm_norm = be.author_nm_norm;
		


-- [UPDATED] Step 2: Filter to actionable records only.
create or replace temp table stg_new_trade_db_contributors_tmp as
    select *
    from stg_new_trade_db_contributors_with_delta_tmp
    where delta_flag in ('NEW', 'CHANGED');
	
	
		
/* Original: Get distinct concatinated names to be fed into _PROD_AUTHOR_MATCHING_SP 
create or replace temp table stg_new_trade_db_contributor_names_tmp as
    select distinct author_nm from stg_new_trade_db_contributors_tmp;
*/    
	
	
-- [UPDATED: stg_new_trade_db_contributor_names_tmp]
-- Adds a filter to exclude UNIDENTIFIED and UNKNOWN author names from the engine
-- input. These are placeholder values that should never be matched against ISNI records,
-- so sending them to the engine wastes compute and risks spurious matches.
create or replace temp table stg_new_trade_db_contributor_names_tmp as
    select distinct author_nm
    from stg_new_trade_db_contributors_tmp
    where upper(author_nm) not in ('UNIDENTIFIED', 'UNKNOWN');


	
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

/* Original: Get reocrds that will not refrence ISNI for Indigo Contributor ID 
create or replace temp table stg_isbn_contributor_not_isni_sourced_tmp as
    select * from stg_new_trade_db_contributors_tmp
    where ISBN13||'_'||AUTHOR_NM not in 
        (select ISBN13||'_'||AUTHOR_NM 
        from stg_isbn_contributor_isni_sourced_tmp 
        where ref_to is not null);
*/

-- [UPDATED: stg_isbn_contributor_not_isni_sourced_tmp]
-- Exclusion key widened from 2-part (isbn13||'_'||author_nm) to 5-part
-- (trade_db_contributor_id||'_'||isbn13||'_'||contributor_role||'_'||contributor_order||'_'||author_nm).
-- The 2-part key incorrectly excluded records when the same isbn13+author_nm existed
-- for a different contributor_id, role, or order. Since CHANGED records are now included
-- (not just NEW), the stricter 5-part key is required to avoid over-exclusion.
create or replace temp table stg_isbn_contributor_not_isni_sourced_tmp as
    select *
    from stg_new_trade_db_contributors_tmp
    where trade_db_contributor_id||'_'||isbn13||'_'||contributor_role||'_'||contributor_order||'_'||author_nm
        not in (
            select trade_db_contributor_id||'_'||isbn13||'_'||contributor_role||'_'||contributor_order||'_'||author_nm
            from stg_isbn_contributor_isni_sourced_tmp
            where ref_to is not null
        );


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

/* Original: Map ISBNs and contributors 
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

*/
-- [UPDATED: stg_final_isbn_contributor_tmp]
-- New Logic adds iia.isni and eod.matched_isni as explicit separate output columns,
-- in addition to the coalesced final_isni. This makes the web-ISNI and engine-ISNI
-- independently visible for debugging and for the match_status CASE expression to
-- reference each source clearly without ambiguity.
create or replace temp table stg_final_isbn_contributor_tmp as
    select
        nis.*,
        iia.isni,
        eod.matched_isni as engine_matched_isni,
        -- iia.isni is from ISNI web; eod.matched_isni is from matching engine; web takes priority
        coalesce(iia.isni, eod.matched_isni) as final_isni,
        case
            when iia.isni is not null and iia.isni = eod.matched_isni then 'Unique Match'
            when iia.isni is not null and iia.isni <> eod.matched_isni then 'Conflict'
            when iia.isni is null then 'No Match Found'
            else 'Undetermined'
        end as match_status
    from stg_isbn_contributor_not_isni_sourced_tmp nis
    -- this table is from ISNI web after manipulation (isbn13, isni, full_name)
    left join stg_isni_full_isbn_authors_tmp iia
        on  nis.isbn13 = iia.isbn13
        and nis.author_nm = iia.full_name
    left join stg_contributor_engine_output_dupes_tmp eod
        on  nis.author_nm = eod.author_nm
        and iia.isni = eod.matched_isni;



/* Original: Get flags for when sourced from Pre-filled ISNI records 
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
*/

-- [UPDATED: stg_isbn_contributor_isni_prefilled_isni_tmp]
-- Adds delta_flag to the output. This column is required downstream so the two-phase
-- UPDATE logic in TIDY UP can distinguish CHANGED records (update base table) from
-- NEW records (staged for insert). Without delta_flag, the CHANGED/NEW split is lost.

create or replace temp table stg_isbn_contributor_isni_prefilled_isni_tmp as
    select
        trade_db_contributor_id,
        isbn13,
        contributor_role,
        contributor_order,
        author_nm,
        delta_flag,
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

/* Original: Get records not sourced from ISNI or Prefilled ISNI 
create or replace temp table stg_isbn_contributor_not_isni_or_prefilled_isni_sourced_tmp as 
    select * from stg_new_trade_db_contributors_tmp
    where isbn13||'_'||author_nm not in (select isbn13||'_'||author_nm from stg_isbn_contributor_isni_sourced_tmp where ref_to is not null)
    and   isbn13||'_'||author_nm not in (select isbn13||'_'||author_nm from stg_isbn_contributor_isni_prefilled_isni_final_tmp where ref_to is not null);
*/

-- [UPDATED: stg_isbn_contributor_not_isni_or_prefilled_isni_sourced_tmp]
-- Same rationale as stg_isbn_contributor_not_isni_sourced_tmp above: widened from
-- 2-part to 5-part exclusion key to correctly handle CHANGED records where the same
-- isbn13+author_nm may appear under a different contributor_id, role, or order.
create or replace temp table stg_isbn_contributor_not_isni_or_prefilled_isni_sourced_tmp as
    select *
    from stg_new_trade_db_contributors_tmp
    where trade_db_contributor_id||'_'||isbn13||'_'||contributor_role||'_'||contributor_order||'_'||author_nm
        not in (
            select trade_db_contributor_id||'_'||isbn13||'_'||contributor_role||'_'||contributor_order||'_'||author_nm
            from stg_isbn_contributor_isni_sourced_tmp
            where ref_to is not null
        )
      and trade_db_contributor_id||'_'||isbn13||'_'||contributor_role||'_'||contributor_order||'_'||author_nm
        not in (
            select trade_db_contributor_id||'_'||isbn13||'_'||contributor_role||'_'||contributor_order||'_'||author_nm
            from stg_isbn_contributor_isni_prefilled_isni_final_tmp
            where ref_to is not null
        );

/* Original: Combine ISNI and Prefilled ISNI sourced 
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
	
*/

-- [UPDATED: stg_isbn_contributor_isni_and_prefilled_isni_sourced_tmp]
-- Adds delta_flag as an explicit output column in both halves of the UNION.
-- This propagates the NEW/CHANGED classification forward into all downstream tables
-- that reference this combined set, enabling the UPDATE-vs-INSERT split in TIDY UP
-- and STAGE DATA. Without delta_flag here, no downstream table can distinguish the two.
create or replace temp table stg_isbn_contributor_isni_and_prefilled_isni_sourced_tmp as
    select
        trade_db_contributor_id,
        isbn13,
        contributor_role,
        contributor_order,
        author_nm,
        delta_flag,
        matched_isni,
        ref_to
    from stg_isbn_contributor_isni_sourced_tmp
    where ref_to is not null
    union
    select
        trade_db_contributor_id,
        isbn13,
        contributor_role,
        contributor_order,
        author_nm,
        delta_flag,
        final_isni as matched_isni,
        ref_to
    from stg_isbn_contributor_isni_prefilled_isni_final_tmp
    where ref_to is not null;
	
	
	
-- [NEW] First UPDATE: apply ISNI/ISNI&PREFILLISNI CHANGED rows to the
-- base table BEFORE deriving unique ISNIs.
-- Reason: stg_has_unique_isni_tmp (below) reads from the base table to include all
-- previously known ISNI assignments. For CHANGED contributors the base table still
-- holds the old ISNI. If not corrected first, unique_isni will reflect the stale ISNI,
-- causing incorrect TRADEDBID&ISNI assignments in the next step. This UPDATE fires only
-- for CHANGED records in the ISNI/ISNI&PREFILLISNI bucket; it nulls indigo_author_id
-- and normailized_author_name so they are re-assigned cleanly downstream.
update ci_dev.data_engineering.matched_author_isbn13 tgt
    set
        tgt.trade_db_contributor_id = src.trade_db_contributor_id,
        tgt.trade_db_author_name    = src.author_nm,
        tgt.matched_isni            = src.matched_isni,
        tgt.id_source               = src.ref_to,
        tgt.indigo_author_id        = null,
        tgt.normailized_author_name = null
    from stg_isbn_contributor_isni_and_prefilled_isni_sourced_tmp src
    where src.delta_flag = 'CHANGED'
      and tgt.isbn13 = src.isbn13
      and tgt.trade_db_contributor_role = src.contributor_role
      and tgt.trade_db_contributor_order = src.contributor_order;
	  
	  
	  
/* Original: Combine ISNI and Prefilled ISNI sourced with CLEANSED.PROD.MATCHED_AUTHOR_ISBN13 
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
*/

-- [UPDATED: stg_isbn_contributor_isni_and_prefilled_isni_sourced_base_combined_tmp]
-- Three changes from Original Logic:
-- 1. The new-delivery side is filtered to delta_flag = 'NEW' only. CHANGED records were
--    already written to the base table by the UPDATE above, so the base-table half of
--    this UNION already reflects them. Including CHANGED here would double-count ISNIs.
-- 2. Reads from matched_author_isbn13 (now already updated) rather than the staging
--    temp table alone, capturing ISNI assignments from all past deliveries.
-- 3. Adds 'TRADEDBID&ISNI' to the base-table filter. Original Logic only included 'ISNI'
--    and 'ISNI&PREFILLISNI'; adding 'TRADEDBID&ISNI' ensures contributors that
--    previously received an ISNI via cross-contributor propagation are also considered
--    when evaluating whether a contributor_id maps to a single unique ISNI.

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
    where delta_flag = 'NEW'
    union
    select
        trade_db_contributor_id,
        isbn13,
        trade_db_contributor_role as contributor_role,
        trade_db_contributor_order as contributor_order,
        trade_db_author_name as author_nm,
        matched_isni,
        id_source as ref_to
    from ci_dev.data_engineering.matched_author_isbn13
    where id_source in ('ISNI', 'ISNI&PREFILLISNI', 'TRADEDBID&ISNI');


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

/* Original: Get records from new TRADE_CONTRIBUTORS that haven't been flagged yet 
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
*/

-- [UPDATED: stg_new_contributors_not_flagged_tmp]
-- Same rationale as other exclusion tables: widened from 2-part to 5-part key to
-- correctly handle CHANGED records and avoid over-exclusion when the same
-- isbn13+author_nm appears for a different contributor_id, role, or order.

create or replace temp table stg_new_contributors_not_flagged_tmp as
    select *
    from stg_new_trade_db_contributors_tmp
    where trade_db_contributor_id||'_'||isbn13||'_'||contributor_role||'_'||contributor_order||'_'||author_nm
        not in (
            select trade_db_contributor_id||'_'||isbn13||'_'||contributor_role||'_'||contributor_order||'_'||author_nm
            from stg_isbn_contributor_isni_sourced_tmp
            where ref_to is not null
        )
      and trade_db_contributor_id||'_'||isbn13||'_'||contributor_role||'_'||contributor_order||'_'||author_nm
        not in (
            select trade_db_contributor_id||'_'||isbn13||'_'||contributor_role||'_'||contributor_order||'_'||author_nm
            from stg_isbn_contributor_isni_prefilled_isni_final_tmp
            where ref_to is not null
        )
      and trade_db_contributor_id||'_'||isbn13||'_'||contributor_role||'_'||contributor_order||'_'||author_nm
        not in (
            select trade_db_contributor_id||'_'||isbn13||'_'||contributor_role||'_'||contributor_order||'_'||author_nm
            from stg_trade_contributors_and_isni_sourced_tmp
            where ref_to is not null
        );
		
		
/* Original: Combine ISNI, Prefilled ISNI and TRADE_CONTRIBUTORS sourced 
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
*/

-- [UPDATED: stg_trade_contributors_prefilled_isni_and_isni_sourced_tmp]
-- Original Logic explicitly listed 7 columns (excluding delta_flag). New Logic uses
-- select * so that delta_flag is automatically propagated from both upstream tables.
-- The column list is otherwise identical; this change is purely to carry delta_flag
-- forward to stg_combined_sources_with_flag_tmp for the UPDATE/INSERT split.

create or replace temp table stg_trade_contributors_prefilled_isni_and_isni_sourced_tmp as
    select *
    from stg_isbn_contributor_isni_and_prefilled_isni_sourced_tmp
    union
    select *
    from stg_trade_contributors_and_isni_sourced_tmp
    where ref_to is not null;
	

/* Original: Get flags for records that sourced from TRADE_CONTRIBUTORS
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
 */
 
-- [UPDATED: stg_trade_contributor_sourced_tmp]
-- Adds delta_flag to the SELECT list. Original Logic omitted it because there was no
-- downstream logic that needed it (everything was treated as NEW). New Logic requires
-- delta_flag here so stg_combined_sources_with_flag_tmp can route CHANGED records to
-- the base-table UPDATE and NEW records to the staging insert.
create or replace temp table stg_trade_contributor_sourced_tmp as
    select
        trade_db_contributor_id,
        isbn13,
        contributor_role,
        contributor_order,
        author_nm,
        delta_flag,
        'trdb'||''||trade_db_contributor_id as matched_isni,
        'TRADEDBID' as ref_to
    from stg_new_contributors_not_flagged_tmp;

/* Original: Combine all records with a source flag 
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
*/

-- [UPDATED: stg_combined_sources_with_flag_tmp]
-- Same rationale as stg_trade_contributors_prefilled_isni_and_isni_sourced_tmp above:
-- changed from explicit column list (without delta_flag) to select * so delta_flag
-- is propagated through for the TIDY UP and STAGE DATA sections to consume.

create or replace temp table stg_combined_sources_with_flag_tmp as
    select *
    from stg_trade_contributors_prefilled_isni_and_isni_sourced_tmp
    union
    select *
    from stg_trade_contributor_sourced_tmp
    where ref_to is not null;

/********************************************************/
/****************** TIDY UP *****************************/
/********************************************************/ 
       
-- [REMOVED] Original Logic had: truncate table ci_dev.data_engineering.stg_matched_author_isbn13;
-- Reason: The truncate + insert into stg_matched_author_isbn13 now happens at the END of
-- TRANSFORMATIONS (after full ID allocation and canonical name derivation via the combined
-- temp table approach). Truncating here would clear the table before the data is ready.

-- [NEW] Second UPDATE: apply the full CHANGED set (all ref_to buckets)
-- to the base table.
-- Reason: The first UPDATE (above) only covered CHANGED records in the ISNI and
-- ISNI&PREFILLISNI buckets because those were needed before unique_isni derivation.
-- CHANGED records in TRADEDBID&ISNI and TRADEDBID buckets were not yet assigned at
-- that point. Now that stg_combined_sources_with_flag_tmp is complete (all buckets),
-- this second UPDATE refreshes all remaining CHANGED rows, nulling their Indigo IDs
-- for re-assignment downstream.
update ci_dev.data_engineering.matched_author_isbn13 tgt
    set
        tgt.trade_db_contributor_id = src.trade_db_contributor_id,
        tgt.trade_db_author_name    = src.author_nm,
        tgt.matched_isni            = src.matched_isni,
        tgt.id_source               = src.ref_to,
        tgt.indigo_author_id        = null,
        tgt.normailized_author_name = null
    from stg_combined_sources_with_flag_tmp src
    where src.delta_flag = 'CHANGED'
      and tgt.isbn13 = src.isbn13
      and tgt.trade_db_contributor_role = src.contributor_role
      and tgt.trade_db_contributor_order = src.contributor_order;

-- [NEW] Backfill INDIGO_AUTHOR_ID and NORMAILIZED_AUTHOR_NAME for
-- CHANGED rows that share a matched_isni with other rows that still have valid IDs.
-- Reason: The UPDATE above nulled indigo_author_id for CHANGED rows. Some of those
-- rows share a matched_isni with other base-table rows that were not CHANGED and
-- still have their Indigo IDs intact. Re-populating here before the staging step
-- prevents unnecessary new-ID allocation for ISNIs that already have known IDs.
update ci_dev.data_engineering.matched_author_isbn13 tgt
    set
        tgt.indigo_author_id        = src.indigo_author_id,
        tgt.normailized_author_name = src.normailized_author_name
    from (
        select
            matched_isni,
            max(indigo_author_id)        as indigo_author_id,
            max(normailized_author_name) as normailized_author_name
        from ci_dev.data_engineering.matched_author_isbn13
        where matched_isni is not null
          and indigo_author_id is not null
        group by matched_isni
    ) src
    where tgt.indigo_author_id is null
      and tgt.matched_isni = src.matched_isni;
	  

/********************************************************/
/****************** STAGE DATA **************************/
/********************************************************/
-- [REMOVED] Original Logic inserted all records from stg_combined_sources_with_flag_tmp
-- directly into stg_matched_author_isbn13 with null indigo_author_id and
-- normailized_author_name. New Logic replaces this with a richer pattern:
-- it stages only NEW records into a temp table (below), backfills known Indigo IDs,
-- then in TRANSFORMATIONS builds a combined base+new table for full ID allocation
-- and canonical name derivation. The final load into stg_matched_author_isbn13
-- happens in DATA LOAD after everything is resolved.
/* Original[REMOVED] insert data into the staging table 
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
*/

-- [NEW] Stage NEW records only for Indigo ID assignment and MERGE.
-- CHANGED records are already applied to the base table via the UPDATEs above;
-- they do not need a separate insert row.
create or replace temp table stg_matched_author_isbn13_new_tmp as
    select
        *,
        null::bigint as indigo_contr_id
    from stg_combined_sources_with_flag_tmp
    where delta_flag = 'NEW';

-- [NEW] Backfill Indigo IDs for NEW records whose matched_isni already
-- exists in the base table. Avoids allocating a new Indigo ID for a known ISNI.
update stg_matched_author_isbn13_new_tmp n
    set indigo_contr_id = b.indigo_author_id
    from (
        select
            matched_isni,
            indigo_author_id
        from ci_dev.data_engineering.matched_author_isbn13
        where indigo_author_id is not null
        qualify row_number() over (partition by matched_isni order by matched_isni) = 1
    ) b
    where n.matched_isni = b.matched_isni;
	
	
/********************************************************/
/****************** TRANSFORMATIONS *********************/
/********************************************************/
-- [REMOVED] All Original Logic TRANSFORMATIONS (backfill indigo_author_id from base,
-- allocate new IDs via cross-join, populate normailized_author_name, generate hash,
-- clear trdb placeholders, delete null rows) are replaced by the combined-table
-- approach below.
-- Reason: New Logic unifies the base table and new-record staging table into a single
-- combined temp table, then performs ID allocation and canonical name derivation across
-- the full combined set in one consistent pass. This handles edge cases (e.g., ISNIs
-- shared across base and new records) more reliably than operating on stg_matched_author_isbn13
-- alone. Hash generation and trdb-placeholder cleanup are also moved into the INSERT in
-- DATA LOAD, keeping the staging table as a clean intermediate.
/*
-------Backfill INDIGO_AUTHOR_ID from CLEANSED.PROD.MATCHED_AUTHOR_ISBN13 
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

------Create new INDIGO_AUTHOR_ID where authors didn't have one before 
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

----Populate Consolidated Author Names 
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

-- -- Generate DMC_SYNC_HASH_IDs 
update ci_dev.data_engineering.stg_matched_author_isbn13
    set dmc_sync_hash_id = hash(
        trade_db_contributor_id,
        indigo_author_id,
        normailized_author_name,
        matched_isni
    );

-- -- Clear out ISNI palceholders now that all hashes have been calculated 
update ci_dev.data_engineering.stg_matched_author_isbn13
    set matched_isni = null
    where matched_isni ilike 'trdb%';

-- -- There may be cases where the INDIGO_AUTHOR_ID or the NORMALIZED_AUTHOR_NAME is null.
delete from ci_dev.data_engineering.stg_matched_author_isbn13 
where indigo_author_id is null 
or    normailized_author_name is null;
 */
 
/* Build combined table: full base (previous delivery) + newly staged NEW records.
   [NEW] Replaces Original Logic's direct operation on stg_matched_author_isbn13;
   by unifying base + new here, the three Original Logic-style UPDATE steps below operate on
   the full combined set rather than just the staging table, ensuring IDs and names
   are consistent across old and new records. UNION ALL is intentional as the same ISNI
   can appear under different contributor IDs in each set. */
create or replace temp table stg_indigo_contributor_book_ref_combined_tmp as
    select
        trade_db_contributor_id,
        isbn13,
        trade_db_contributor_role as contributor_role,
        trade_db_contributor_order as contributor_order,
        trade_db_author_name as author_nm,
        matched_isni,
        id_source as ref_to,
        indigo_author_id as indigo_contr_id
    from ci_dev.data_engineering.matched_author_isbn13
    union all
    select
        trade_db_contributor_id,
        isbn13,
        contributor_role,
        contributor_order,
        author_nm,
        matched_isni,
        ref_to,
        indigo_contr_id
    from stg_matched_author_isbn13_new_tmp;

/* Backfill INDIGO_CONTR_ID from matched_author_isbn13 where a matching ISNI already exists.
   Adapted from Original Logic step 1 (Backfill INDIGO_AUTHOR_ID from CLEANSED.PROD.MATCHED_AUTHOR_ISBN13);
   targets stg_indigo_contributor_book_ref_combined_tmp instead of stg_matched_author_isbn13. */
update stg_indigo_contributor_book_ref_combined_tmp stg
    set indigo_contr_id = cln.indigo_author_id
    from (
        select
            matched_isni,
            indigo_author_id
        from ci_dev.data_engineering.matched_author_isbn13
        qualify row_number() over (partition by matched_isni order by matched_isni) = 1
    ) cln
    where stg.indigo_contr_id is null
      and stg.matched_isni = cln.matched_isni;

/* Create new INDIGO_CONTR_ID where ISNIs still have none after backfill.
   Adapted from Original Logic step 2 (Create new INDIGO_AUTHOR_ID where authors didn't have one before);
   targets stg_indigo_contributor_book_ref_combined_tmp and uses indigo_contr_id column name. */
update stg_indigo_contributor_book_ref_combined_tmp stg
    set indigo_contr_id = new.new_indigo_contr_id
    from (
        with max_id as (
            select max(indigo_author_id) as max_indigo_author_id
            from (
                select indigo_author_id from ci_dev.data_engineering.matched_author_isbn13
                union all
                select indigo_contr_id   from stg_indigo_contributor_book_ref_combined_tmp
            )
        ),
        new_isni as (
            select distinct matched_isni
            from stg_indigo_contributor_book_ref_combined_tmp
            where indigo_contr_id is null
              and matched_isni is not null
        )
        select
            nis.matched_isni,
            mid.max_indigo_author_id
            + row_number() over (order by nis.matched_isni) as new_indigo_contr_id
        from new_isni nis
        cross join max_id mid
    ) new
    where stg.matched_isni = new.matched_isni
      and stg.indigo_contr_id is null;

/* Null out ISNI, ref_to, and Indigo ID for placeholder author names.
   [NEW] These records carry no real author identity and should not be
   persisted with ISNI or Indigo ID assignments that would pollute the contributor master. */
update stg_indigo_contributor_book_ref_combined_tmp
    set
        matched_isni    = null,
        ref_to          = null,
        indigo_contr_id = null
    where upper(author_nm) in ('UNIDENTIFIED', 'UNKNOWN', 'ANONYMOUS');

/* Populate INDIGO_CONTR_NM: derive canonical author name per Indigo ID.
   Adapted from Original Logic step 3 (Populate Consolidated Author Names); same frequency/length/alpha
   tie-breaker logic, but now operates across the full combined table (base + new) instead of
   only the new-record staging set, producing a more stable canonical name. */
create or replace temp table stg_indigo_contributor_book_ref_final_tmp as
    select
        t.trade_db_contributor_id,
        t.isbn13,
        t.contributor_role,
        t.contributor_order,
        t.author_nm,
        t.matched_isni,
        t.ref_to,
        t.indigo_contr_id,
        c.indigo_contr_nm
    from stg_indigo_contributor_book_ref_combined_tmp t
    left join (
        select indigo_contr_id, author_nm as indigo_contr_nm
        from (
            select
                indigo_contr_id,
                author_nm,
                count(*) as cnt,
                row_number() over (
                    partition by indigo_contr_id
                    order by
                        cnt desc,
                        length(author_nm) desc,
                        author_nm asc
                ) as rn
            from stg_indigo_contributor_book_ref_combined_tmp
            where indigo_contr_id is not null
              and author_nm is not null
            group by indigo_contr_id, author_nm
        )
        where rn = 1
    ) c on t.indigo_contr_id = c.indigo_contr_id;

/* Load fully resolved records into stg_matched_author_isbn13, bridging to DATA LOAD.
   Replaces Original Logic's steps 4-6 (generate hash, clear trdb%, delete nulls) with a single
   inline truncate + insert: hash is computed here, trdb% placeholders are excluded via
   WHERE clause, and null indigo_contr_id rows (UNIDENTIFIED/UNKNOWN/ANONYMOUS) are excluded
   by the WHERE clause — matching the net effect of those three Original Logic UPDATE/DELETE steps. */
truncate table ci_dev.data_engineering.stg_matched_author_isbn13;

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
    dmc_sync_hash_id,
    dmc_synced,
    transformation_id
)
select distinct
    isbn13,
    indigo_contr_id as indigo_author_id,
    trade_db_contributor_id,
    contributor_role  as trade_db_contributor_role,
    contributor_order as trade_db_contributor_order,
    matched_isni,
    indigo_contr_nm   as normailized_author_name,
    author_nm         as trade_db_author_name,
    ref_to            as id_source,
    hash(trade_db_contributor_id, indigo_contr_id, indigo_contr_nm, matched_isni) as dmc_sync_hash_id,
    true              as dmc_synced,
    42                as transformation_id
from stg_indigo_contributor_book_ref_final_tmp
where indigo_contr_id       is not null
  and indigo_contr_nm        is not null
  and (matched_isni is null or matched_isni not ilike 'trdb%');
 
 
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