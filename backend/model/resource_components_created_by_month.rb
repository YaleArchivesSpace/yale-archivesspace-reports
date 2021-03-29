class ResourceComponentsCreatedByMonth < AbstractReport
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
        SELECT left(rtrim(ao.create_time), 7) as `yyyy_mm`
          , count(left(rtrim(ao.create_time), 7)) as `archival_objects_created`
        FROM archival_object ao
        WHERE ao.repo_id = #{db.literal(@repo_id)}
        GROUP BY left(rtrim(ao.create_time), 7)
      SOME_SQL
  end

  def page_break
    false
  end
end