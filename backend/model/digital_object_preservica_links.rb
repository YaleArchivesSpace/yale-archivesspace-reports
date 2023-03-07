class DigitalObjectPreservicaLinks < AbstractReport
  register_report(
    params: [["call_number", "callnumber", "The resource identifier(s)"]]
  )

  def initialize(params, job, db)
    super

    @call_number = params["call_number"].to_s
    @call_number = @call_number.split(', ')

  end

  def query
    results = db.fetch(query_string)
    info[:total_count] = results.count
    results
  end

  def query_string
    <<~SOME_SQL
    SELECT NULL as to_download
      , CONCAT('https://archives.yale.edu/repositories/', ao.repo_id, '/archival_objects/', ao.id) as aay_url
      , JSON_UNQUOTE(JSON_EXTRACT(resource.identifier, '$[0]')) as call_number
      , resource.title as collection_title
      , ao.display_string as object_title
      , LTRIM(physical_containers.container_statement) as container_statement
      , fv.file_size_bytes as size_in_bytes
      , fv.file_size_bytes/1000000 as size_in_mb
      , ev.value as file_type
      , do.title as do_title
      , do.digital_object_id as deliverable_unit
      , CONCAT(replace(do.title, '[Preservica] ', ''), '.', ev.value) as filename
      , CONCAT('/repositories/', do.repo_id, '/digital_objects/', do.id) as do_uri
    FROM file_version fv
    JOIN digital_object do on do.id = fv.digital_object_id
    LEFT JOIN enumeration_value ev on ev.id = fv.file_format_name_id
    JOIN instance_do_link_rlshp idlr on idlr.digital_object_id = do.id
    JOIN instance on idlr.instance_id = instance.id
    JOIN archival_object ao on ao.id = instance.archival_object_id
    JOIN resource on resource.id = ao.root_record_id
    LEFT JOIN (SELECT ao.id as ao_id
                  , GROUP_CONCAT(DISTINCT CONCAT(IFNULL(ev.value, '')
                                                      , IF(tc.indicator IS NOT NULL, ' ', '')
                                                      , IFNULL(tc.indicator, '')
                                                      , IF(cp.name IS NOT NULL, ' [', '')
                                                      , IFNULL(cp.name, '')
                                                      , IF(cp.name IS NOT NULL, '] ', ' ')
                                                      , IFNULL(ev2.value, '')
                                                      , IF(sc.indicator_2 IS NOT NULL, ' ', '')
                                                      , IFNULL(sc.indicator_2, ''))
                  ORDER BY CAST(tc.indicator as UNSIGNED), CAST(sc.indicator_2 as UNSIGNED)
                  SEPARATOR '; ') as container_statement
                FROM archival_object ao
                JOIN resource on resource.id = ao.root_record_id
                JOIN instance on instance.archival_object_id = ao.id
                JOIN sub_container sc on sc.instance_id = instance.id
                JOIN top_container_link_rlshp tclr on tclr.sub_container_id = sc.id
                JOIN top_container tc on tc.id = tclr.top_container_id
                LEFT JOIN top_container_profile_rlshp tcpr on tcpr.top_container_id = tc.id
                LEFT JOIN container_profile cp on tcpr.container_profile_id = cp.id
                LEFT JOIN enumeration_value ev on ev.id = tc.type_id
                LEFT JOIN enumeration_value ev2 on ev2.id = sc.type_2_id
                LEFT JOIN enumeration_value ev3 on ev3.id = instance.instance_type_id
                WHERE ao.repo_id = #{db.literal(@repo_id)}
                AND JSON_UNQUOTE(JSON_EXTRACT(resource.identifier, '$[0]')) in #{db.literal(@call_number)}
                AND (ev3.value is NULL or ev3.value != 'digital_object')
                GROUP BY ao.id) as physical_containers on physical_containers.ao_id = ao.id
    where file_uri like '%preservica.library.yale.edu/explorer/%'
    AND resource.repo_id = #{db.literal(@repo_id)}
    AND JSON_UNQUOTE(JSON_EXTRACT(resource.identifier, '$[0]')) in #{db.literal(@call_number)}
    ORDER BY do.title
    SOME_SQL
  end

  def page_break
    false
  end
end