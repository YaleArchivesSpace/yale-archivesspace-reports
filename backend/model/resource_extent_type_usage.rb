class ResourceExtentTypeUsage < AbstractReport
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
      SELECT ev.value as extent_type
        , COUNT(*) as count
      FROM extent
      LEFT JOIN enumeration_value ev on ev.id = extent.extent_type_id
      GROUP BY ev.value
      ORDER BY ev.value
      SOME_SQL
  end

  def page_break
    false
  end
end