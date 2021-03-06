class TopContainerUnassociatedContainers < AbstractReport
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
        , tc.created_by
        , tc.create_time
        , tc.last_modified_by
        , tc.user_mtime
      from sub_container sc
      left join top_container_link_rlshp tclr on tclr.sub_container_id = sc.id
      left join top_container tc on tc.id = tclr.top_container_id
      where sc.instance_id is null
      and tc.repo_id = #{db.literal(@repo_id)}
      UNION
      select CONCAT('/repositories/', tc.repo_id, '/top_containers/', tc.id) as uri
        , tc.barcode
        , tc.indicator
        , tc.created_by
        , tc.create_time
        , tc.last_modified_by
        , tc.user_mtime
      from sub_container sc
      right join top_container_link_rlshp tclr on tclr.sub_container_id = sc.id
      right join top_container tc on tc.id = tclr.top_container_id
      where sc.instance_id is null
      and tc.repo_id = #{db.literal(@repo_id)}
    SOME_SQL
  end

  def page_break
    false
  end
end