class DigitalObjectPreservicaLinksMultiple < AbstractReport
  register_report(
    params: [["call_number", "callnumber", "The resource identifier"]]
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
    SELECT CONCAT('https://archives.yale.edu/repositories/', ao.repo_id, '/archival_objects/', ao.id) as aay_url
      , replace(replace(replace(replace(replace(identifier, ',', ''), '"', ''), ']', ''), '[', ''), 'null', '') AS call_number
      , resource.title as collection_title
      # , (SELECT GROUP_CONCAT(CONCAT(display_string, ' (', ao_level, ')') SEPARATOR ' < ') as parent_path
      #       FROM (SELECT T2.display_string as display_string
      #             , ev.value as ao_level
      #           FROM (SELECT @r AS _id
      #               , @p := @r AS previous
      #               , (SELECT @r := parent_id FROM archival_object WHERE id = _id) AS parent_id
      #               , @l := @l + 1 AS lvl
      #                 FROM ((SELECT @r := ao.id, @p := 0, @l := 0) AS vars,
      #                       archival_object h)
      #                      WHERE @r <> 0 AND @r <> @p) AS T1
      #           JOIN archival_object T2 ON T1._id = T2.id
      #           LEFT JOIN enumeration_value ev on ev.id = T2.level_id
      #           WHERE T2.id != ao.id
      #           ORDER BY T1.lvl DESC) as all_parents) as p_path
      , ao.display_string as object_title
      , fv.file_size_bytes/1000000 as size_in_mb
      , ev.value as file_type
      , file_uri as direct_download_link
      , CONCAT(replace(fv.file_uri, 
              "https://preservica.library.yale.edu/api/entity/digitalFileContents/", 
                "https://preservica.library.yale.edu/explorer/explorer.html#render:10&"), "&0") as render_in_preservica_link

      , do.title as do_title
      , CONCAT('/repositories/', do.repo_id, '/digital_objects/', do.id) as do_uri
    FROM file_version fv
    JOIN digital_object do on do.id = fv.digital_object_id
    LEFT JOIN enumeration_value ev on ev.id = fv.file_format_name_id
    JOIN instance_do_link_rlshp idlr on idlr.digital_object_id = do.id
    JOIN instance on idlr.instance_id = instance.id
    JOIN archival_object ao on ao.id = instance.archival_object_id
    JOIN resource on resource.id = ao.root_record_id
    where file_uri like '%preservica.library.yale.edu/api/entity/digitalFileContents%'
    AND resource.repo_id = #{db.literal(@repo_id)}
    #how to have multiple call numbers....
    AND replace(replace(replace(replace(replace(resource.identifier, \',\', \'\'), \'\"\', \'\'), \']\', \'\'), \'[\', \'\'), \'null\', \'\') 
    in #{db.literal(@call_number)}
    ORDER BY resource.identifier
    SOME_SQL
  end

  def page_break
    false
  end
end