Metrics :

People Changing from B/I and vice versa.
People Changing Tax Information
Verification of KYC
Deleted / Ghosted


  with base_population
  as
  (
  SELECT distinct profile_id, person_id,
    full_name,  date_of_birth,  phone,  email,  
  nationality,    country_of_residence, tax_id, gender
  FROM db_exports_pii.cider_production_profiles_v01 
  WHERE  ds =  '2023-07-02'
  and is_latest =1
  )
  ,

  profiles AS 
  (
  SELECT distinct profile_id, person_id,full_name,  date_of_birth,  phone,  email,  
  nationality,    country_of_residence, tax_id, gender
  FROM db_exports_pii.cider_production_profiles_v01 
  WHERE  ds =  '{{presto.latest_partition('db_exports_pii.cider_production_profiles_v01')}}'
  and is_latest =1
  )
  ,

  persons AS 
  (
  select * 
  from db_exports.cider_production_persons_v01 
  where  ds ='{{ presto.latest_partition('db_exports.cider_production_persons_v01')}}'
  )
  ,

  business_lat AS 
  (
  SELECT
        new.entity_id                                               AS id_user
      , COALESCE(legacy.dim_business_type, new.dim_business_type)   AS dim_kyc_business_type
      FROM
        (
          SELECT 
          CASE WHEN  entity_id='' THEN NULL ELSE CAST(entity_id AS BIGINT) END entity_id 
          , business_type AS dim_business_type
          , ROW_NUMBER() OVER(PARTITION BY entity_id ORDER BY created_at DESC) AS Rank -- Latest data 
            FROM db_exports.cider_production_businesses_v01
            WHERE ds = '{{presto.latest_partition('db_exports.cider_production_businesses_v01')}}'
              AND source_type = 'KYC'
              AND entity_type = 'ACCOUNT'
              AND deleted_at IS NULL
              AND entity_id not in ('')
            ) as new 
        LEFT JOIN 
        (
            SELECT 
            user_id
            , CASE WHEN business_type is NULL THEN 'INDIVIDUAL'
              WHEN business_type in ('NOT_REGISTERED_PARTNERSHIP', 'REGISTERED_PARTNERSHIP', 'REGISTERED_PARTNERSHIP', 'SOLE_PROPRIETORSHIP', 'PRIVATE_COMPANY',
              'PUBLIC_COMPANY', 'GOVERNMENT_OWNED_COMPANY') THEN business_type
              ELSE NULL END AS dim_business_type
            FROM db_exports.oyster_production_businessaccount_deserialized_v03
            WHERE ds = '{{presto.latest_partition('db_exports.oyster_production_businessaccount_deserialized_v03')}}'
            ) legacy on new.entity_id=legacy.user_id 
    where Rank=1
  )
  ,

  business_pop AS 
  (
  SELECT
        new.entity_id                                               AS id_user
      , COALESCE(legacy.dim_business_type, new.dim_business_type)   AS dim_kyc_business_type
      FROM
        (
          SELECT 
          CASE WHEN  entity_id='' THEN NULL ELSE CAST(entity_id AS BIGINT) END entity_id 
          , business_type AS dim_business_type
          , ROW_NUMBER() OVER(PARTITION BY entity_id ORDER BY created_at DESC) AS Rank -- Latest data 
            FROM db_exports.cider_production_businesses_v01
            WHERE ds = '2023-07-02'
              AND source_type = 'KYC'
              AND entity_type = 'ACCOUNT'
              AND deleted_at IS NULL
              AND entity_id not in ('')
            ) as new 
        LEFT JOIN 
        (
            SELECT 
            user_id
            , CASE WHEN business_type is NULL THEN 'INDIVIDUAL'
              WHEN business_type in ('NOT_REGISTERED_PARTNERSHIP', 'REGISTERED_PARTNERSHIP', 'REGISTERED_PARTNERSHIP', 'SOLE_PROPRIETORSHIP', 'PRIVATE_COMPANY',
              'PUBLIC_COMPANY', 'GOVERNMENT_OWNED_COMPANY') THEN business_type
              ELSE NULL END AS dim_business_type
            FROM db_exports.oyster_production_businessaccount_deserialized_v03
            WHERE ds = '2023-07-02'
            ) legacy on new.entity_id=legacy.user_id 
    where Rank=1
  )



  select a.*,
  b.full_name as curr_full_name,
  b.date_of_birth as curr_date_of_birth,
  b.phone as curr_phone,
  b.email as curr_email,
  b.nationality as curr_nationality, 
  b.country_of_residence as curr_residence,  
  b.tax_id   as curr_tax_id,
  b.gender as curr_gender,
  CAST(c.context_id AS BIGINT) as user_id,
  bp.dim_kyc_business_type as business,
  bt.dim_kyc_business_type as curr_business,
  Case when a.full_name <> b.full_name then 'Yes' else 'No' end Change_FullName,
  case when a.date_of_birth <> b.date_of_birth then 'Yes' else 'No' end  Change_Birth,
  case when a.phone <> b.phone then  'Yes' else 'No' end Change_Phone,
  case when a.email <> b.email then  'Yes' else 'No' end Change_Email,  
  case when a.nationality <> b.nationality then 'Yes' else 'No' end Change_Nationality,    
  case when a.country_of_residence <> b.country_of_residence then  'Yes' else 'No' end Change_CountryResidence,
  case when a.tax_id <> b.tax_id then 'Yes' else 'No' end Change_taxID,
  case when a.gender <> b.gender then  'Yes' else 'No' end Change_Gender,
  case when bp.dim_kyc_business_type <> bt.dim_kyc_business_type then 'Yes' else 'No' end as Change_business
  from base_population a 
  join profiles b on a.profile_id = b.profile_id and a.person_id = b.person_id
  join persons c on a.person_id = c.person_id
  join business_pop bp on CAST(c.context_id AS BIGINT)  = bp.id_user 
  join business_lat bt on CAST(c.context_id AS BIGINT) = bt.id_user
  where a.profile_id = '87c28f01988f403cb1aedc9d8443b41a'


WITH tpi as 
(SELECT *
FROM db_exports.oyster_production_taxpayerinformation_deserialized_v01
WHERE
ds = '{{ presto.latest_partition('db_exports.oyster_production_taxpayerinformation_deserialized_v01') }}'
AND deleted_at is null)

, relate as
(SELECT *
FROM db_exports.oyster_production_taxpayerinformationrelationship_deserialized_v01
WHERE
ds = '{{ presto.latest_partition('db_exports.oyster_production_taxpayerinformationrelationship_deserialized_v01') }}'
AND deleted_at is null)

, authority as 
(SELECT *
FROM tax_platform.global_taxpayer_tax_authority_data)

, type as
(SELECT *
FROM tax_platform.global_taxpayer_tax_relationship_type)

SELECT tpi.*
    , relate.entity_id as id_listing
    , relate.created_at as tpi_on_listing_created_at
    , authority.name as authority_name
    , authority.full_name as authority_name_full
    , authority.level as authority_level
    , type.name as tax_type
FROM tpi
JOIN relate --excludes hosts w/non-assigned TPI
ON tpi.id = relate.tpi_id
JOIN authority
ON tpi.tax_authority_id = authority.id
JOIN type
ON tpi.relationship_type = type.id

