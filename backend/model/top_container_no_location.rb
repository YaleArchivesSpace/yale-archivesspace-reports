class TopContainerNoLocation < AbstractReport
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
    select CONCAT('/repositories/', tc.repo_id, '/top_containers/', tc.id) as uri
        , tc.barcode
        , tc.indicator
        , resource.title
    from top_container tc
    LEFT JOIN top_container_housed_at_rlshp tchar on tchar.top_container_id = tc.id
    LEFT JOIN location on tchar.location_id = location.id
    LEFT JOIN top_container_link_rlshp tclr on tclr.top_container_id = tc.id
    LEFT JOIN sub_container sc on tclr.sub_container_id = sc.id
    LEFT JOIN instance on sc.instance_id = instance.id
    LEFT JOIN archival_object ao on instance.archival_object_id = ao.id
    LEFT JOIN resource on resource.id = ao.root_record_id
    where location.id is null
    and tc.repo_id = #{db.literal(@repo_id)}
    UNION
    select CONCAT('/repositories/', tc.repo_id, '/top_containers/', tc.id) as uri
        , tc.barcode
        , tc.indicator
        , resource.title
    from top_container tc
    RIGHT JOIN top_container_housed_at_rlshp tchar on tchar.top_container_id = tc.id
    RIGHT JOIN location on tchar.location_id = location.id
    RIGHT JOIN top_container_link_rlshp tclr on tclr.top_container_id = tc.id
    RIGHT JOIN sub_container sc on tclr.sub_container_id = sc.id
    RIGHT JOIN instance on sc.instance_id = instance.id
    RIGHT JOIN archival_object ao on instance.archival_object_id = ao.id
    RIGHT JOIN resource on resource.id = ao.root_record_id
    where location.id is null
    and tc.repo_id = #{db.literal(@repo_id)}
    SOME_SQL
  end

  def page_break
    false
  end
end