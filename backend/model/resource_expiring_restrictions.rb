class ResourceExpiringRestrictions < AbstractReport
  register_report(
    params: [['end', Date, 'The end of the report range']]
  )

  def initialize(params, job, db)
    super

    end_date = params['end'] || Time.now.to_s

    @from = DateTime.parse(end_date).to_time.strftime('%Y-%m-%d %H:%M:%S')

  end

  def query_string
    <<~SOME_SQL
      SELECT ev.value as rights_restriction_type
           , rr.begin
           , rr.end
           , replace(replace(replace(replace(replace(r2.identifier, ',', ''), '"', ''), ']', ''), '[', ''), 'null', '') AS call_number
           , replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(ao.display_string, '<', ''), '>', ''), '/', ''), '=', ''), '"', ''), 'emph', ''), 'render', ''), 'title', ''), ' xlink:typesimple italic', ''), 'italic', ''), 'underline', ''), 'smcaps', '') as child_title
           , r2.title as parent_title
           , CONCAT('/repositories/', ao.repo_id, '/archival_objects/', ao.id) as uri
           , ao.repo_id
      FROM rights_restriction rr
      JOIN rights_restriction_type rrt on rr.id = rrt.rights_restriction_id
      JOIN archival_object ao on ao.id = rr.archival_object_id
      JOIN resource r2 on r2.id = ao.root_record_id
      LEFT JOIN enumeration_value ev on ev.id = rrt.restriction_type_id
      JOIN note on note.archival_object_id = ao.id
      WHERE rr.end is not null
      and r2.identifier like '%RU%'
      and rr.end < #{db.literal(@from.split(' ')[0].gsub('-', ''))}
      UNION ALL
      SELECT ev.value as rights_restriction_type
           , rr.begin
           , rr.end
          , replace(replace(replace(replace(replace(r.identifier, ',', ''), '"', ''), ']', ''), '[', ''), 'null', '') AS call_number
          , NULL as child_title
          , r.title as parent_title
          , CONCAT('/repositories/', r.repo_id, '/resources/', r.id) as uri
          , r.repo_id
      FROM rights_restriction rr
      JOIN rights_restriction_type rrt on rr.id = rrt.rights_restriction_id
      JOIN resource r on r.id = rr.resource_id
      LEFT JOIN enumeration_value ev on ev.id = rrt.restriction_type_id
      WHERE rr.end is not null
      and r.identifier like '%RU%'
      and rr.end < #{db.literal(@from.split(' ')[0].gsub('-', ''))}%
    SOME_SQL
  end

  def page_break
    false
  end
end