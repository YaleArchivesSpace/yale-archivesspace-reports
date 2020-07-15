class ResourceBornDigitalMaterials < AbstractReport
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
      SELECT DISTINCT CONCAT('https://archives.yale.edu/repositories/', ao.repo_id, '/archival_objects/', ao.id) as aay_url
        , CONCAT('https://archivesspace.library.yale.edu/resources/', ao.root_record_id, '#tree::archival_object_', ao.id) as staff_url
        , CONCAT('/repositories/', ao.repo_id, '/archival_objects/', ao.id) as uri
        , replace(replace(replace(replace(replace(identifier, ',', ''), '"', ''), ']', ''), '[', ''), 'null', '') AS call_number
        , replace(resource.title, '"', "'") as resource_title
        , replace(replace(ao.display_string, '"', "'"), ",", "\,") as ao_title
        , ANY_VALUE(CONCAT(
                    IF(ev.value is not NULL,
                       ev.value,
                       "NULL"),
                                       ', ', 
                                  IF(cp.name is not NULL,
                       cp.name,
                       "NULL"))
                                  ) as extent_type_container_profile
        , ANY_VALUE(do.title) as do_title
        , GROUP_CONCAT(fv.file_uri SEPARATOR ', ') as file_uris
      FROM archival_object ao
      JOIN resource on resource.id = ao.root_record_id
      LEFT JOIN extent on extent.archival_object_id = ao.id
      LEFT JOIN enumeration_value ev on ev.id = extent.extent_type_id
      LEFT JOIN instance on instance.archival_object_id = ao.id
      LEFT JOIN instance_do_link_rlshp idlr on instance.id = idlr.instance_id
      LEFT JOIN sub_container sc on sc.instance_id = instance.id
      LEFT JOIN top_container_link_rlshp tclr on tclr.sub_container_id = sc.id
      LEFT JOIN top_container tc on tclr.top_container_id = tc.id
      LEFT JOIN top_container_profile_rlshp tcpr on tcpr.top_container_id = tc.id
      LEFT JOIN container_profile cp on tcpr.container_profile_id = cp.id
      LEFT JOIN digital_object do on idlr.digital_object_id = do.id
      LEFT JOIN enumeration_value ev2 on ev2.id = instance.instance_type_id
      LEFT JOIN file_version fv on fv.digital_object_id = do.id
      LEFT JOIN enumeration_value ev3 on ev3.id = tc.type_id
      LEFT JOIN enumeration_value ev4 on ev4.id = sc.type_2_id
      WHERE (ev.value like '%CD%'
          OR ev.value like '%DVD%'
          OR ev.value like '%disk%'
          OR ev.value like '%disc%'
          OR ev.value like '%bytes%'
          OR ev.value like '%drives%'
          OR ev.value like '%computer%'
          OR ev2.value like '%computer%'
          OR cp.name like '%CD%' 
          or cp.name like '%compact%' 
          or cp.name like '%DV%' 
          or cp.name like '%disc%'
          or resource.title like '%disc%'
          OR ao.display_string like '%digital %'
          OR ao.display_string like '%digitized%'
          OR ao.display_string like '%computer files%'
          or ao.display_string like '%disk%'
          or ao.display_string like '%disc%')
      AND (fv.file_uri not like '%https://aviaryplatform.com/images/audio-default.png%'
          OR fv.file_uri is NULL)
      AND ao.repo_id = #{db.literal(@repo_id)}
      GROUP BY ao.id
      SOME_SQL
  end

  def page_break
    false
  end
end