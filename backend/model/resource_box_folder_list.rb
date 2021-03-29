class ResourceBoxFolderList < AbstractReport
  register_report(
    params: [["call_number", "callnumber", "The resource identifier"]]
  )

  def initialize(params, job, db)
    super

    @call_number = params["call_number"].to_s

  end

  def query
    results = db.fetch(query_string)
    info[:total_count] = results.count
    results
  end

  def query_string
    <<~SOME_SQL
    SELECT CONCAT('/repositories/', tc.repo_id, '/top_containers/', tc.id) as tc_uri
      , CONCAT('/repositories/', resource.repo_id, '/resources/', resource.id) as resource_uri
      , CONCAT('/repositories/', resource.repo_id) as repo_uri
      , CONCAT('/repositories/', ao.repo_id, '/archival_objects/', ao.id) as ao_uri
      , replace(replace(replace(replace(replace(identifier, ',', ''), '"', ''), ']', ''), '[', ''), 'null', '') AS call_number
      , resource.title AS resource_title
      , ao.display_string AS ao_title
      , ev2.value AS level
      , tc.barcode AS barcode
      , cp.name AS container_profile
      , tc.indicator AS container_num
      , ev.value AS sc_type
      , sc.indicator_2 AS sc_num
    from sub_container sc
    LEFT JOIN enumeration_value ev on ev.id = sc.type_2_id
    JOIN top_container_link_rlshp tclr on tclr.sub_container_id = sc.id
    JOIN top_container tc on tclr.top_container_id = tc.id
    LEFT JOIN top_container_profile_rlshp tcpr on tcpr.top_container_id = tc.id
    LEFT JOIN container_profile cp on cp.id = tcpr.container_profile_id
    LEFT JOIN top_container_housed_at_rlshp tchar on tchar.top_container_id = tc.id
    JOIN instance on sc.instance_id = instance.id
    JOIN archival_object ao on instance.archival_object_id = ao.id
    JOIN resource on ao.root_record_id = resource.id
    LEFT JOIN enumeration_value ev2 on ev2.id = ao.level_id
    WHERE resource.repo_id = #{db.literal(@repo_id)}
    AND replace(replace(replace(replace(replace(resource.identifier, \',\', \'\'), \'\"\', \'\'), \']\', \'\'), \'[\', \'\'), \'null\', \'\') = #{db.literal(@call_number)}
    SOME_SQL
  end

  def page_break
    false
  end
end