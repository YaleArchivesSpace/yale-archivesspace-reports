class DigitalObjectPreservicaLinks < AbstractReport
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
    SELECT CONCAT('/repositories/', ao.repo_id, '/archival_objects/', ao.id) as archival_object_uri
      , CONCAT('/repositories/', ao.repo_id, '/resources/', ao.root_record_id) as resource_uri
      , CONCAT('/repositories/', ao.repo_id) as repo_uri
      , CONCAT('/repositories/', ao2.repo_id, '/archival_objects/', ao2.id) as parent_uri
      , ao.title as archival_object_title
      , ev.value as archival_object_level
    FROM archival_object ao
    LEFT JOIN enumeration_value ev on ev.id = ao.level_id
    LEFT JOIN archival_object ao2 on ao2.id = ao.parent_id
    WHERE resource.repo_id = #{db.literal(@repo_id)}
    AND replace(replace(replace(replace(replace(resource.identifier, \',\', \'\'), \'\"\', \'\'), \']\', \'\'), \'[\', \'\'), \'null\', \'\') = #{db.literal(@call_number)}
    SOME_SQL
  end

  def page_break
    false
  end
end