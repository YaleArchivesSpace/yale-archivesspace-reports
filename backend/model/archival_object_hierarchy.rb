class ArchivalObjectHierarchy < AbstractReport
  register_report(
    params: [["call_number", "callnumber", "The resource identifier(s)"]]
  )

  def initialize(params, job, db)
    super

    @call_number = params["call_number"].to_s
    @call_number = @call_number.split(', ')

  end

  def query
    results = db.fetch(query_string)
    info[:total_count] = results.count
    results
  end

  def query_string
  	<<~SOME_SQL
    WITH RECURSIVE hierarchies
    AS (SELECT ao.id as id
      , ao.display_string as display_string
      , ao.parent_id as parent_id
      , ao.root_record_id as root_record_id
      , replace(resource.title, '"', "'") as resource_title
      , JSON_UNQUOTE(JSON_EXTRACT(resource.identifier, '$[0]')) AS call_number
      , ao.repo_id as repo_id
      , 0 as lvl
      , IF(ev.value = 'series', ao.component_id, NULL) as series_cuid
      , IF(ev.value = 'series', ao.display_string, NULL) as series_title
      , IF(ev.value = 'series', ao.id, NULL) as series_id
      , CONCAT(ao.display_string, ' (', CONCAT(UPPER(SUBSTRING(ev.value,1,1)),LOWER(SUBSTRING(ev.value,2))), ' ', IF(ao.component_id is not NULL, CAST(ao.component_id as CHAR), "N/A"), ')') as path
    FROM archival_object ao
    LEFT JOIN enumeration_value ev on ev.id = ao.level_id
    JOIN resource on resource.id  = ao.root_record_id
    WHERE ao.parent_id is NULL
    AND JSON_UNQUOTE(JSON_EXTRACT(resource.identifier, '$[0]')) in #{db.literal(@call_number)}
    UNION ALL
    SELECT ao.id as id
      , ao.display_string as display_string
      , ao.parent_id as parent_id
      , ao.root_record_id as root_record_id
      , replace(resource.title, '"', "'") as resource_title
      , JSON_UNQUOTE(JSON_EXTRACT(resource.identifier, '$[0]')) AS call_number
      , ao.repo_id as repo_id
      , h.lvl + 1 as lvl
      , h.series_cuid
      , h.series_title
      , h.series_id
      , CONCAT(h.path ,' > ', CONCAT(ao.display_string, ' (', CONCAT(UPPER(SUBSTRING(ev.value,1,1)),LOWER(SUBSTRING(ev.value,2))), ' ', IF(ao.component_id is not NULL, CAST(ao.component_id as CHAR), "N/A"), ')')) AS path
    FROM hierarchies h
    JOIN archival_object ao on h.id = ao.parent_id
    JOIN resource on resource.id  = ao.root_record_id
    LEFT JOIN enumeration_value ev on ev.id = ao.level_id
    WHERE JSON_UNQUOTE(JSON_EXTRACT(resource.identifier, '$[0]')) in #{db.literal(@call_number)}
    SELECT id
      , parent_id
      , series_id
      , root_record_id
      , repo_id
      , lvl
      , call_number
      , resource_title
      , series_cuid
      , series_title
      , display_string
      , CONCAT(resource_title, ' (Resource ', call_number, ') > ', path) as full_path
    FROM hierarchies;
    SOME_SQL
  end

  def page_break
    false
  end
end