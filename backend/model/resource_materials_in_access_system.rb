class ResourceMaterialsInAccessSystem < AbstractReport
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
        SELECT CONCAT('/repositories/', ao.repo_id, '/archival_objects/', ao.id) as uri
          , ao.display_string as title
          , replace(replace(replace(replace(replace(identifier, ',', ''), '"', ''), ']', ''), '[', ''), 'null', '') AS call_number
          , fv.file_uri
        FROM digital_object do
        JOIN file_version fv on fv.digital_object_id = do.id
        LEFT JOIN instance_do_link_rlshp idlr on idlr.digital_object_id = do.id
        LEFT JOIN instance on idlr.instance_id = instance.id
        LEFT JOIN archival_object ao on instance.archival_object_id = ao.id
        LEFT JOIN archival_object ao2 on ao2.id = ao.parent_id
        LEFT JOIN resource on ao.root_record_id = resource.id
        WHERE fv.id is not null
        AND do.repo_id = 12
        AND ao.id is not null
        AND fv.file_uri not like '%preservica%'
        AND fv.file_uri not like '%https://libweb.library.yale.edu/pui-assets/access_thumb.jpg%'
        AND fv.file_uri not like '%/collection_resource_files/thumbnails/%'
        AND fv.file_uri not like '%/images/audio-default.png%'
        AND fv.file_uri not like '%imageserver.library.yale.edu/digcoll%'
        AND fv.file_uri not like '%130.132.21.20%'
        AND fv.file_uri not like '%gemini%'
        AND fv.file_uri not like '%triton%'
        AND fv.file_uri not like '%thumbnail%'
        AND fv.file_uri not like '%kaltura%'
        AND resource.title not like '%kissinger%'
      SOME_SQL
  end

  def page_break
    false
  end
end