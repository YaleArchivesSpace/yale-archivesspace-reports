class ResourcePreservicaMaterials < AbstractReport
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
      SELECT CONCAT('https://archives.yale.edu/repositories/', ao.repo_id, '/archival_objects/', ao.id) as aay_url
        , CONCAT('https://archivesspace.library.yale.edu/resources/', ao.root_record_id, '#tree::archival_object_', ao.id) as staff_url
        , CONCAT('/repositories/', ao.repo_id, '/archival_objects/', ao.id) as uri
        , replace(replace(replace(replace(replace(identifier, ',', ''), '"', ''), ']', ''), '[', ''), 'null', '') AS call_number
        , replace(replace(replace(replace(resource.title, '"', "'"), '</', ''), '<', ''), '>', '') as resource_title
          , replace(replace(replace(replace(ao.title, '"', "'"), '</', ''), '<', ''), '>', '') as ao_title
        , ANY_VALUE(CONCAT(
                    IF(ev.value is not NULL,
                       ev.value,
                       "NULL"),
                                       ', ', 
                                  IF(cp.name is not NULL,
                       cp.name,
                       "NULL"))
                                  ) as category
        , ao.create_time
        , ANY_VALUE(do.create_time) as do_create_time
        , ANY_VALUE(do.created_by) as do_created_by
          , GROUP_CONCAT(do.title SEPARATOR ', ') as preservica_title
        , GROUP_CONCAT(fv.file_uri SEPARATOR ', ') as file_uri
      FROM digital_object do
      JOIN file_version fv on fv.digital_object_id = do.id
      LEFT JOIN instance_do_link_rlshp idlr on idlr.digital_object_id = do.id
      LEFT JOIN instance on idlr.instance_id = instance.id
      LEFT JOIN sub_container sc on sc.instance_id = instance.id
      LEFT JOIN top_container_link_rlshp tclr on tclr.sub_container_id = sc.id
      LEFT JOIN top_container tc on tclr.top_container_id = tc.id
      LEFT JOIN top_container_profile_rlshp tcpr on tcpr.top_container_id = tc.id
      LEFT JOIN container_profile cp on tcpr.container_profile_id = cp.id
      LEFT JOIN archival_object ao on instance.archival_object_id = ao.id
      LEFT JOIN extent on extent.archival_object_id = ao.id
      LEFT JOIN enumeration_value ev on ev.id = extent.extent_type_id
      LEFT JOIN archival_object ao2 on ao2.id = ao.parent_id
      LEFT JOIN resource on ao.root_record_id = resource.id
      WHERE do.repo_id = 12
      AND ao.id is not null
      AND fv.file_uri like '%preservica%'
      GROUP BY ao.id
      SOME_SQL
  end

  def page_break
    false
  end
end