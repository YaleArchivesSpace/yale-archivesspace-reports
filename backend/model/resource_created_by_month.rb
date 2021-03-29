class ResourceCreatedByMonth < AbstractReport
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
      SELECT left(rtrim(resource.create_time), 7) as `yyyy_mm`
        , count(left(rtrim(resource.create_time), 7)) as `resources_created`
      FROM resource
      WHERE resource.repo_id = #{db.literal(@repo_id)}
      GROUP BY left(rtrim(resource.create_time), 7)
      SOME_SQL
  end

  def page_break
    false
  end
end