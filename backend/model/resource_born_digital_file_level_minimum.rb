class ResourceBornDigitalFileLevelMinimum < AbstractReport
  register_report(
    params: [["call_number", "callnumber", "The resource identifier"]]
  )

  def initialize(params, job, db)
    super

    @call_number = params["call_number"].to_s

    #info[:scoped_by_date_range] = "#{@from} & #{@to}"
  end

  def query
    results = db.fetch(query_string)
    info[:total_count] = results.count
    results
  end

  def query_string
  	<<~SOME_SQL
    SELECT CONCAT('/repositories/', ao.repo_id) as repository
      , CONCAT('/repositories/', ao.repo_id, '/resources/', ao.root_record_id) as resource
      , CONCAT('/repositories/', ao.repo_id, '/archival_objects/', ao.id) as archival_object_uri
      , ao.title as title
      , GROUP_CONCAT(extent.number SEPARATOR '; ') as extent_number
      , GROUP_CONCAT(ev2.value SEPARATOR '; ') as extent_type
      , GROUP_CONCAT(ev3.value SEPARATOR '; ') as extent_portion
      , GROUP_CONCAT(extent.container_summary SEPARATOR '; ') as extent_container_summary
      , GROUP_CONCAT(byte_extents.bytes SEPARATOR '; ') as byte_size
      , GROUP_CONCAT(byte_extents.byte_summary SEPARATOR '; ') as byte_size_container_summary
      , GROUP_CONCAT(date.expression SEPARATOR '; ') as date_expression
      , GROUP_CONCAT(date.begin SEPARATOR '; ') as date_begin
      , GROUP_CONCAT(date.end SEPARATOR '; ') as date_end
      , GROUP_CONCAT(ev4.value SEPARATOR '; ') as date_type
      , GROUP_CONCAT(ev5.value SEPARATOR '; ') as date_label
      , GROUP_CONCAT(scope_notes.notes) as scope_content
      , NULL as use_standard_access_note
      , GROUP_CONCAT(access_notes.notes) as access_restrict
      , GROUP_CONCAT(access_notes.machine_actionable_restriction_type) as machine_actionable_restriction_type
      , GROUP_CONCAT(access_notes.timebound_restriction_begin_date) as timebound_restriction_begin_date
      , GROUP_CONCAT(access_notes.timebound_restriction_end_date) as timebound_restriction_end_date
      , GROUP_CONCAT(process_notes.notes) as process_info
      , GROUP_CONCAT(otherfindaid_notes.notes) as otherfind_aid
      , GROUP_CONCAT(arrangement_notes.notes) as arrangement
    FROM archival_object ao
    LEFT JOIN enumeration_value ev on ev.id = ao.level_id
    LEFT JOIN extent on extent.archival_object_id = ao.id
    LEFT JOIN enumeration_value ev2 on ev2.id = extent.extent_type_id
    LEFT JOIN enumeration_value ev3 on ev3.id = extent.portion_id
    LEFT JOIN date on date.archival_object_id = ao.id
    LEFT JOIN enumeration_value ev4 on ev4.id = date.date_type_id
    LEFT JOIN enumeration_value ev5 on ev5.id = date.label_id
    JOIN resource on resource.id = ao.root_record_id
    LEFT JOIN (SELECT note.archival_object_id
          , JSON_UNQUOTE(JSON_EXTRACT(CAST(CONVERT(note.notes using utf8) as json), '$.subnotes[0].content')) as notes
         FROM note
         JOIN archival_object ao on ao.id = note.archival_object_id
         WHERE ao.repo_id = 12
         AND note.notes like '%scopecontent%') as scope_notes on scope_notes.archival_object_id = ao.id
    LEFT JOIN (SELECT note.archival_object_id
          , JSON_UNQUOTE(JSON_EXTRACT(CAST(CONVERT(note.notes using utf8) as json), '$.subnotes[0].content')) as notes
         FROM note
         JOIN archival_object ao on ao.id = note.archival_object_id
         WHERE ao.repo_id = 12
         AND note.notes like '%processinfo%') as process_notes on process_notes.archival_object_id = ao.id
    LEFT JOIN (SELECT note.archival_object_id
          , JSON_UNQUOTE(JSON_EXTRACT(CAST(CONVERT(note.notes using utf8) as json), '$.subnotes[0].content')) as notes
         FROM note
         JOIN archival_object ao on ao.id = note.archival_object_id
         WHERE ao.repo_id = 12
         AND note.notes like '%otherfindaid%') as otherfindaid_notes on otherfindaid_notes.archival_object_id = ao.id
    LEFT JOIN (SELECT note.archival_object_id
          , JSON_UNQUOTE(JSON_EXTRACT(CAST(CONVERT(note.notes using utf8) as json), '$.subnotes[0].content')) as notes
         FROM note
         JOIN archival_object ao on ao.id = note.archival_object_id
         WHERE ao.repo_id = 12
         AND note.notes like '%"type":"arrangement"%') as arrangement_notes on arrangement_notes.archival_object_id = ao.id
    LEFT JOIN (SELECT note.archival_object_id
              , JSON_UNQUOTE(JSON_EXTRACT(CAST(CONVERT(note.notes using utf8) as json), '$.subnotes[0].content')) as notes
              , replace(replace(replace(JSON_UNQUOTE(JSON_EXTRACT(CAST(CONVERT(note.notes using utf8) as json),           '$.rights_restriction.local_access_restriction_type')), '[', ''), ']', ''), '"', '') as machine_actionable_restriction_type
              , JSON_UNQUOTE(JSON_EXTRACT(CAST(CONVERT(note.notes using utf8) as json), '$.rights_restriction.begin')) as timebound_restriction_begin_date
              , JSON_UNQUOTE(JSON_EXTRACT(CAST(CONVERT(note.notes using utf8) as json), '$.rights_restriction.end')) as timebound_restriction_end_date
           FROM note
           JOIN archival_object ao on ao.id = note.archival_object_id
           WHERE ao.repo_id = #{db.literal(@repo_id)}
             AND note.notes like '%accessrestrict%') as access_notes on access_notes.archival_object_id = ao.id
    LEFT JOIN (SELECT extent.archival_object_id
            , ev.value as bytes
            , extent.container_summary as byte_summary
          FROM extent
          LEFT JOIN enumeration_value ev on ev.id = extent.extent_type_id
          JOIN archival_object ao on ao.id = extent.archival_object_id
          WHERE ev.value = 'bytes'
          AND ao.repo_id = #{db.literal(@repo_id)}) as byte_extents on byte_extents.archival_object_id = ao.id
    WHERE resource.repo_id = #{db.literal(@repo_id)}
    AND replace(replace(replace(replace(replace(resource.identifier, \',\', \'\'), \'\"\', \'\'), \']\', \'\'), \'[\', \'\'), \'null\', \'\') = #{db.literal(@call_number)}
    GROUP BY ao.id
    ORDER BY ao.parent_id, ao.position
    SOME_SQL
  end

  def page_break
    false
  end
end