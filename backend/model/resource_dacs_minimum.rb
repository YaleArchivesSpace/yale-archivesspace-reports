class ResourceDacsMinimum < AbstractReport
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
      SELECT DISTINCT CONCAT('/repositories/', resource.repo_id, '/resources/', resource.id) as resource_uri  
          , CONCAT(repository.name, ' (', repository.org_code, '; ', ev.value, '; ', rl.latitude, ' ', rl.longitude, ')') as `repository_info (DACS 2.1/2.2)`
          , replace(replace(replace(replace(replace(resource.identifier, ',', ''), '"', ''), ']', ''), '[', ''), 'null', '') AS `resource_identifier (DACS 2.1)`
          , resource.title as `resource_title (DACS 2.3)`
          , date_statements.date_value as `date_info (DACS 2.4)`
          , extent_statements.extent_value as `extent_info (DACS 2.5)`
          , creators.creator_names as `name_of_creator (DACS 2.6)`
          , bioghist_notes.notes as `bioghist (DACS 2.7)`
          , scope_notes.notes as `scope_notes (DACS 3.1)`
          , arrangement_notes.notes as `arrangement_notes (DACS 3.2)`
          , access_notes.notes as `access_notes (DACS 4.1)`
          , use_notes.notes as `use_notes (DACS 4.4)`
          , CONCAT(IFNULL(language_data.language_code, 'NULL'),
                '/ ',
               IFNULL(language_data.script_code, 'NULL'),
                '/ ',
               IFNULL(language_data.note_text, 'NULL')) as `lang_material (DACS 4.5)`
          , acq_notes.notes as `immediate_source_acq (DACS 5.2)`
          , prefercite_notes.notes as `preferred_citation (DACS 7.1)`
          , processinfo_notes.notes as `processing_info (DACS 7.1)`
          , CONCAT(IFNULL(resource.finding_aid_date, 'NULL'),
                '; ',
               IFNULL(resource.finding_aid_author, 'NULL'),
                '; ',
               IFNULL(ev2.value, 'NULL'),
                '; ',
               IFNULL(resource.finding_aid_note, 'NULL')) as `description_control (DACS 8.1)`
          , all_points.acc_points as `access points`
        FROM resource
        JOIN repository on resource.repo_id = repository.id 
        JOIN repository_location rl on rl.repository_id = repository.id
        LEFT JOIN enumeration_value ev on ev.id = repository.country_id
        LEFT JOIN enumeration_value ev2 on ev2.id = resource.finding_aid_description_rules_id
        LEFT JOIN (SELECT GROUP_CONCAT(DISTINCT CONCAT(date.begin, '-', IFNULL(date.end, 'NULL'), ' (', ev2.value, ' ', ev3.value, ')') SEPARATOR '; ') as date_value
                , resource.id as resource_id
              FROM resource
              LEFT JOIN date on date.resource_id = resource.id
              LEFT JOIN enumeration_value ev2 on ev2.id = date.date_type_id
              LEFT JOIN enumeration_value ev3 on ev3.id = date.label_id
              WHERE resource.repo_id = #{db.literal(@repo_id)}
                GROUP BY resource.id) as date_statements on date_statements.resource_id = resource.id
        LEFT JOIN (SELECT GROUP_CONCAT(DISTINCT CONCAT(extent.number, ' ', ev4.value, ' (', ev5.value, ')') SEPARATOR '; ') as extent_value
                , resource.id as resource_id
              FROM resource
              LEFT JOIN extent on extent.resource_id = resource.id
              LEFT JOIN enumeration_value ev4 on ev4.id = extent.extent_type_id
              LEFT JOIN enumeration_value ev5 on ev5.id = extent.portion_id
              WHERE resource.repo_id = #{db.literal(@repo_id)}
              GROUP BY resource.id) as extent_statements on extent_statements.resource_id = resource.id
        LEFT JOIN (SELECT note.resource_id
                , GROUP_CONCAT(DISTINCT JSON_UNQUOTE(JSON_EXTRACT(CAST(CONVERT(note.notes using utf8) as json), '$.subnotes[0].content')) SEPARATOR '; ') as notes
                FROM note
                JOIN resource on resource.id = note.resource_id
                WHERE resource.repo_id = #{db.literal(@repo_id)}
                AND note.notes like '%scopecontent%'
                GROUP BY resource.id) as scope_notes on scope_notes.resource_id = resource.id
        LEFT JOIN (SELECT note.resource_id
                , GROUP_CONCAT(DISTINCT JSON_UNQUOTE(JSON_EXTRACT(CAST(CONVERT(note.notes using utf8) as json), '$.subnotes[0].content')) SEPARATOR '; ') as notes
                FROM note
                JOIN resource on resource.id = note.resource_id
                WHERE resource.repo_id = #{db.literal(@repo_id)}
                AND note.notes like '%"type":"arrangement"%'
                GROUP BY resource.id) as arrangement_notes on arrangement_notes.resource_id = resource.id
        LEFT JOIN (SELECT note.resource_id
                  , GROUP_CONCAT(DISTINCT JSON_UNQUOTE(JSON_EXTRACT(CAST(CONVERT(note.notes using utf8) as json), '$.subnotes[0].content')) SEPARATOR '; ') as notes
              FROM note
              JOIN resource on resource.id = note.resource_id
              WHERE resource.repo_id = #{db.literal(@repo_id)}
              AND note.notes like '%accessrestrict%'
              GROUP BY resource.id) as access_notes on access_notes.resource_id = resource.id
        LEFT JOIN (SELECT note.resource_id
                  , GROUP_CONCAT(DISTINCT JSON_UNQUOTE(JSON_EXTRACT(CAST(CONVERT(note.notes using utf8) as json), '$.subnotes[0].content')) SEPARATOR '; ') as notes
              FROM note
              JOIN resource on resource.id = note.resource_id
              WHERE resource.repo_id = #{db.literal(@repo_id)}
              AND note.notes like '%userestrict%'
              GROUP BY resource.id) as use_notes on use_notes.resource_id = resource.id
        LEFT JOIN (SELECT note.resource_id
                  , GROUP_CONCAT(DISTINCT JSON_UNQUOTE(JSON_EXTRACT(CAST(CONVERT(note.notes using utf8) as json), '$.subnotes[0].content')) SEPARATOR '; ') as notes
              FROM note
              JOIN resource on resource.id = note.resource_id
              WHERE resource.repo_id = #{db.literal(@repo_id)}
              AND note.notes like '%acqinfo%'
              GROUP BY resource.id) as acq_notes on acq_notes.resource_id = resource.id
        LEFT JOIN (SELECT note.resource_id
                  , GROUP_CONCAT(DISTINCT JSON_UNQUOTE(JSON_EXTRACT(CAST(CONVERT(note.notes using utf8) as json), '$.subnotes[0].content')) SEPARATOR '; ') as notes
              FROM note
              JOIN resource on resource.id = note.resource_id
              WHERE resource.repo_id = #{db.literal(@repo_id)}
              AND note.notes like '%prefercite%'
              GROUP BY resource.id) as prefercite_notes on prefercite_notes.resource_id = resource.id
        LEFT JOIN (SELECT note.resource_id
                  , GROUP_CONCAT(DISTINCT JSON_UNQUOTE(JSON_EXTRACT(CAST(CONVERT(note.notes using utf8) as json), '$.subnotes[0].content')) SEPARATOR '; ') as notes
              FROM note
              JOIN resource on resource.id = note.resource_id
              WHERE resource.repo_id = #{db.literal(@repo_id)}
              AND note.notes like '%processinfo%'
              GROUP BY resource.id) as processinfo_notes on processinfo_notes.resource_id = resource.id
        LEFT JOIN (SELECT note.resource_id
                  , GROUP_CONCAT(DISTINCT JSON_UNQUOTE(JSON_EXTRACT(CAST(CONVERT(note.notes using utf8) as json), '$.subnotes[0].content')) SEPARATOR '; ') as notes
              FROM note
              JOIN resource on resource.id = note.resource_id
              WHERE resource.repo_id = #{db.literal(@repo_id)}
              AND note.notes like '%bioghist%'
              GROUP BY resource.id) as bioghist_notes on bioghist_notes.resource_id = resource.id
        LEFT JOIN (SELECT resource.id as resource_id
                , GROUP_CONCAT(DISTINCT lang_struct.lang SEPARATOR '; ') as language_code
                , GROUP_CONCAT(DISTINCT lang_struct.script SEPARATOR '; ') as script_code
                , GROUP_CONCAT(DISTINCT lang_notes.note_text SEPARATOR '; ') as note_text
              FROM resource
              LEFT JOIN (SELECT lang_material_id as lmi
                    , resource.id as resource_id
                    , JSON_UNQUOTE(JSON_EXTRACT(CAST(CONVERT(note.notes using utf8) as json), '$.content[0]')) as note_text 
                     FROM note
                     JOIN lang_material lm on lm.id = note.lang_material_id
                     JOIN resource on resource.id = lm.resource_id
                     WHERE resource.repo_id = #{db.literal(@repo_id)}) 
                     as lang_notes on lang_notes.resource_id = resource.id
              LEFT JOIN (SELECT resource.id as resource_id
                    , lm.id as lmi
                    , ev.value as lang
                    , ev2.value as script
                     FROM lang_material lm
                     JOIN language_and_script las on las.lang_material_id = lm.id
                     JOIN resource on resource.id = lm.resource_id
                     LEFT JOIN enumeration_value ev on ev.id = las.language_id
                     LEFT JOIN enumeration_value ev2 on ev2.id = las.script_id
                     WHERE resource.repo_id = #{db.literal(@repo_id)}
                     AND las.language_id is not null) as lang_struct on lang_struct.resource_id = resource.id
              WHERE resource.repo_id = #{db.literal(@repo_id)}
              GROUP BY resource.id) as language_data on language_data.resource_id = resource.id
        LEFT JOIN (SELECT GROUP_CONCAT(DISTINCT CONCAT(agent_name, ' (', IFNULL(notes, 'NULL'), ')') SEPARATOR '; ') as creator_names
                , resource_id
              FROM
              (SELECT resource.id as resource_id
                , JSON_UNQUOTE(JSON_EXTRACT(CAST(CONVERT(note.notes using utf8) as json), '$.subnotes[0].content')) as notes
                , np.sort_name as agent_name
              FROM linked_agents_rlshp lar
              JOIN resource on resource.id = lar.resource_id
              JOIN name_person np on np.agent_person_id = lar.agent_person_id
              JOIN agent_person ap on ap.id = np.agent_person_id
              LEFT JOIN enumeration_value ev on ev.id = lar.role_id
              LEFT JOIN note on note.agent_person_id = ap.id
              WHERE resource.repo_id = #{db.literal(@repo_id)}
              AND np.is_display_name is not null
              AND ev.value like 'creator'
              UNION ALL
              SELECT resource.id as resource_id
                , JSON_UNQUOTE(JSON_EXTRACT(CAST(CONVERT(note.notes using utf8) as json), '$.subnotes[0].content')) as notes
                , nf.sort_name as agent_name
              FROM linked_agents_rlshp lar
              JOIN resource on resource.id = lar.resource_id
              JOIN name_family nf on nf.agent_family_id = lar.agent_family_id
              JOIN agent_family af on af.id = nf.agent_family_id
              LEFT JOIN enumeration_value ev on ev.id = lar.role_id
              LEFT JOIN note on note.agent_family_id = af.id
              WHERE resource.repo_id = #{db.literal(@repo_id)}
              AND nf.is_display_name is not null
              AND ev.value like 'creator'
              UNION ALL
              SELECT resource.id as resource_id
                , JSON_UNQUOTE(JSON_EXTRACT(CAST(CONVERT(note.notes using utf8) as json), '$.subnotes[0].content')) as notes
                , nce.sort_name as agent_name
              FROM linked_agents_rlshp lar
              JOIN resource on resource.id = lar.resource_id
              JOIN name_corporate_entity nce on nce.agent_corporate_entity_id = lar.agent_corporate_entity_id
              JOIN agent_corporate_entity ace on ace.id = nce.agent_corporate_entity_id
              LEFT JOIN enumeration_value ev on ev.id = lar.role_id
              LEFT JOIN note on note.agent_corporate_entity_id = ace.id
              WHERE resource.repo_id = #{db.literal(@repo_id)}
              AND nce.is_display_name is not null
              AND ev.value like 'creator') as agent_creators
              GROUP BY resource_id) as creators on creators.resource_id = resource.id
        LEFT JOIN (SELECT GROUP_CONCAT(DISTINCT IFNULL(access_point, 'NULL') SEPARATOR '; ') as acc_points
                , resource_id
              FROM
              (SELECT resource.id as resource_id
                  , np.sort_name as access_point
              FROM linked_agents_rlshp lar
              JOIN resource on resource.id = lar.resource_id
              JOIN name_person np on np.agent_person_id = lar.agent_person_id
              LEFT JOIN enumeration_value ev on ev.id = lar.role_id
              WHERE resource.repo_id = #{db.literal(@repo_id)}
              AND np.is_display_name is not null
              AND ev.value like 'subject'
              UNION ALL
              SELECT resource.id as resource_id
                  , nce.sort_name as access_point
              FROM linked_agents_rlshp lar
              JOIN resource on resource.id = lar.resource_id
              JOIN name_corporate_entity nce on nce.agent_corporate_entity_id = lar.agent_corporate_entity_id
              LEFT JOIN enumeration_value ev on ev.id = lar.role_id
              WHERE resource.repo_id = #{db.literal(@repo_id)}
              AND nce.is_display_name is not null
              AND ev.value like 'subject'
              UNION ALL
              SELECT resource.id as resource_id
                  , nf.sort_name as access_point
              FROM linked_agents_rlshp lar
              JOIN resource on resource.id = lar.resource_id
              JOIN name_family nf on nf.agent_family_id = lar.agent_family_id
              LEFT JOIN enumeration_value ev on ev.id = lar.role_id
              WHERE resource.repo_id = #{db.literal(@repo_id)}
              AND nf.is_display_name is not null
              AND ev.value like 'subject'
              UNION ALL
              SELECT resource.id as resource_id
                , subject.title as access_point
              FROM subject_rlshp sr
              JOIN resource on resource.id = sr.resource_id
              JOIN subject on subject.id = sr.subject_id
              WHERE resource.repo_id = #{db.literal(@repo_id)}) as access_points
              GROUP BY resource_id) as all_points on all_points.resource_id = resource.id
        WHERE resource.repo_id = #{db.literal(@repo_id)}
      SOME_SQL
  end

  def page_break
    false
  end
end