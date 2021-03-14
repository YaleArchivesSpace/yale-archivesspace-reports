class ResourceDistinctBoxList < AbstractReport
  register_report(
    params: [["call_number", "callnumber", "The resource identifier"]]
  )

  def initialize(params, job, db)
    super

    @call_number = params["call_number"].to_s

    puts @call_number

    #info[:scoped_by_date_range] = "#{@from} & #{@to}"
  end

  def query
    results = db.fetch(query_string)
    info[:total_count] = results.count
    results
  end

  def query_string
	"SELECT DISTINCT CONCAT('/repositories/', resource.repo_id, '/resources/', resource.id) as resource_uri
		, resource.title AS resource_title
		, replace(replace(replace(replace(replace(resource.identifier, \',\', \'\'), \'\"\', \'\'), \']\', \'\'), \'[\', \'\'), \'null\', \'\') AS call_number
		, CONCAT('/repositories/', tc.repo_id, '/top_containers/', tc.id) as top_container_uri
		, tc.indicator AS container_number
	from sub_container sc
	LEFT JOIN top_container_link_rlshp tclr on tclr.sub_container_id = sc.id
	LEFT JOIN top_container tc on tclr.top_container_id = tc.id
	JOIN instance on sc.instance_id = instance.id
	JOIN archival_object ao on instance.archival_object_id = ao.id
	JOIN resource on ao.root_record_id = resource.id
	WHERE replace(replace(replace(replace(replace(resource.identifier, \',\', \'\'), \'\"\', \'\'), \']\', \'\'), \'[\', \'\'), \'null\', \'\') = #{db.literal(@call_number)}
	AND resource.repo_id = #{db.literal(@repo_id)}
  ORDER BY CAST(tc.indicator as unsigned)"
  end

  def page_break
    false
  end
end