class AccessionAllAccessions < AbstractReport
  register_report(
    params: []
  )

  def query
    results = db.fetch(query_string)
    info[:total_count] = results.count
    results
  end

  def query_string
  	<<~SOME_SQL
    select concat('https://archivesspace.library.yale.edu/accessions/', a.id) as 'Staff URL'
    , replace(replace(replace(replace(replace(a.identifier, ',', '.'), '"', ''), ']', ''), '[', ''), '.null', '') as 'Accession number'
    , SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING_INDEX(REPLACE(a.identifier, '"', ''), ',', 2), ',', 2), ',', -1) as 'curatorial unit'
    , a.title  as Title
    , a.accession_date as Date
    , ev4.value as 'Accession type'
    , ud.text_1 as 'Call Number'
    , a.content_description as 'Content Description'
    , a.condition_description as 'Condition Description'
    , a.provenance as 'Provenance Note'
    , a.general_note as 'General Note'
    , a.access_restrictions_note as 'Access note'
    , a.use_restrictions_note as 'Use note'
    , a.inventory
    , a.publish
    , ud.string_1 as 'ACQ number'
    , ud.string_2 as 'Voyager BIB ID'
    , ud.string_3 as 'Place of Publication'
    , ud.date_1 as 'Review date'
    , ud.date_2 as 'Completed date'
    , ev7.value as 'Order type'
    , ev8.value as 'Acquisition Capacity Budget'
    , ev9.value as 'Curator'
     
    , TRIM(both '; ' from CONCAT(
    GROUP_CONCAT(distinct
    coalesce(case when la.role_id = '880' and np.authorized = '1' then np.sort_name end, '')
    , 
    coalesce(case when la.role_id = '880' and nce.authorized = '1' then nce.sort_name end, '')
    , 
    coalesce(case when la.role_id = '880' and nf.authorized = '1' then nf.sort_name end, '')
     separator '; ')))
     
         as 'Creator(s)'
    ,
    TRIM(both '; ' from CONCAT(GROUP_CONCAT(distinct 
    coalesce(case when la.role_id = '881' and np.authorized = '1' then np.sort_name end, '')
    , 
    coalesce(case when la.role_id = '881' and nce.authorized = '1'  then nce.sort_name end, '')
    , 
    coalesce(case when la.role_id = '881' and nf.authorized = '1'  then nf.sort_name end, '')
     separator '; ')))
         as 'Source(s)'
         
    , GROUP_CONCAT(distinct (concat(extent.number, ' ', ev2.value)) separator '; ') as 'Extent'
                            
    , GROUP_CONCAT(distinct (extent.container_summary) separator '; ') as 'Additional Extent'
    , GROUP_CONCAT(distinct (extent.dimensions) separator '; ') as 'Extent Dimensions'
    , GROUP_CONCAT(distinct (extent.physical_details) separator '; ') as 'Physical Details'
                           
    , GROUP_CONCAT(distinct evf.value SEPARATOR '; ') as 'Fund(s)'

    , GROUP_CONCAT(distinct concat(ev3.value, ': ', coalesce(eventdate.expression, eventdate.begin)) SEPARATOR '; ') as 'Event(s)'

    , cm.processors
    , cm.processing_funding_source
    , ev6.value as 'processing status'
    , cm.processing_total_extent
    , cm.processing_total_extent_type_id
    , cm.processing_hours_per_foot_estimate
    , cm.processing_hours_total
    , cm.processing_plan
    , cm.processing_priority_id

    , group_concat(DISTINCT 
        CASE WHEN LOWER(ev2.value) like '%linear%' THEN CONVERT(extent.number, DECIMAL(10,2)) END
      ORDER BY 1 SEPARATOR '; ') as 'linear-footage'
      
    , group_concat(DISTINCT 
        case when extent_type_id = 119805 then CONVERT(extent.number, DECIMAL(10,2)) end
      ORDER BY 1 SEPARATOR '; ') as 'Manuscript Items'
      
    , group_concat(DISTINCT 
         case when extent_type_id = 119808 then CONVERT(extent.number, DECIMAL(10,2)) end 
      ORDER BY 1 SEPARATOR '; ') as 'Non-book formats' 


    , GROUP_CONCAT(distinct replace(replace(replace(replace(replace(linkeda.identifier, ',', '.'), '"', ''), ']', ''), '[', ''), '.null', '') SEPARATOR '; ') as 'Linked accessions'
    , GROUP_CONCAT(distinct ev5.value separator '; ') as 'Linked accession types'

    , mt.works_of_art, mt.audiovisual_materials, mt.books, mt.electronic_documents, mt.games, mt.microforms, mt.maps, mt.manuscripts, mt.photographs, mt.realia, mt.serials
    , a.create_time

    , GROUP_CONCAT(distinct IF(date.date_type_id in (903, 905), date.expression, NULL) SEPARATOR '; ') as 'date expressions'

    , GROUP_CONCAT(distinct IF(date.date_type_id = 904, CONCAT(date.begin, '-', date.end), NULL) SEPARATOR '; ') as 'bulk dates'

    , GROUP_CONCAT(distinct IF(date.date_type_id in (903, 905), date.begin, NULL) SEPARATOR '; ') as 'normalized begin dates'

    , GROUP_CONCAT(distinct IF(date.date_type_id in (903, 905), date.end, NULL) SEPARATOR '; ') as 'normalized end dates'

    , location.title as 'accession_location'

    , ud.boolean_2 as 'Record reviewed?'
    , a.suppressed as 'Suppressed?'
    , repo.name as 'Repository'



    from accession a
    join repository repo 
    on a.repo_id = repo.id

    left join extent
    on a.id = extent.accession_id

    left join linked_agents_rlshp la
    on a.id = la.accession_id
    left join name_person np
    on np.agent_person_id = la.agent_person_id
    left join name_corporate_entity nce
    on nce.agent_corporate_entity_id = la.agent_corporate_entity_id
    left join name_family nf
    on nf.agent_family_id = la.agent_family_id

    left join enumeration_value 
    on a.acquisition_type_id = enumeration_value.id
    left join enumeration_value ev2
    on extent.extent_type_id = ev2.id
    left join payment_summary ps
    on ps.accession_id = a.id
    left join payment 
    on payment.payment_summary_id = ps.id
    left join enumeration_value evp
    on evp.id = ps.currency_id
    left join enumeration_value evf
    on evf.id = payment.fund_code_id
    left join user_defined ud
    on ud.accession_id = a.id

    left join event_link_rlshp elr
    on elr.accession_id = a.id
    left join event 
    on elr.event_id = event.id
    left join enumeration_value ev3
    on ev3.id = event.event_type_id
    left join date eventdate
    on eventdate.event_id = event.id

    left join collection_management cm
    on cm.accession_id = a.id
    left join enumeration_value ev4
    on ev4.id = a.acquisition_type_id

    left join material_types mt
    on mt.accession_id = a.id

    left join related_accession_rlshp rar
    on a.id = rar.accession_id_0

    left join accession linkeda
    on linkeda.id = rar.accession_id_1

    left join enumeration_value ev5
    on rar.relator_type_id = ev5.id

    left join enumeration_value ev6
    on ev6.id = cm.processing_status_id

    left join enumeration_value ev7
    on ev7.id = ud.enum_1_id 

    left join enumeration_value ev8
    on ev8.id = ud.enum_2_id 

    left join enumeration_value ev9
    on ev9.id = ud.enum_3_id 


    left join date 
    on date.accession_id = a.id

    left join instance
    on instance.accession_id = a.id
    left join sub_container sc
    on sc.instance_id = instance.id
    left join top_container_link_rlshp tcl
    on tcl.sub_container_id = sc.id
    left join top_container tc
    on tc.id = tcl.top_container_id
    left join top_container_housed_at_rlshp tch
    on tch.top_container_id = tc.id
    left join location 
    on tch.location_id = location.id

    WHERE a.repo_id = #{db.literal(@repo_id)}

    group by a.id

    order by Date desc;
    SOME_SQL
  end

  def page_break
    false
  end
end