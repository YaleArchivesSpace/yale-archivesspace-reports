class TopContainerRooms < AbstractReport
    register_report(
      params: [["building", "building"], ["room_name", "roomname", "The resource identifier"]]
    )
    def initialize(params, job, db)
        super
    
        @building = params["building"].to_s
        @roomname = params["roomname"].to_s
    
        #info[:scoped_by_date_range] = "#{@from} & #{@to}"
      end
  
    def query
      results = db.fetch(query_string)
      info[:total_count] = results.count
      results
    end
  
    def query_string
      query = <<~SOME_SQL
        SELECT CONCAT('/repositories/', tc.repo_id, '/top_containers/', tc.id) AS uri
       , tc.barcode
       , tc.indicator
       , l.title
       , l.building
       , l.room
       , tc.created_by
       , tc.create_time
       , tc.last_modified_by
       , tc.user_mtime
        FROM sub_container sc
        LEFT JOIN top_container_link_rlshp tclr ON tclr.sub_container_id = sc.id
        LEFT JOIN top_container tc ON tc.id = tclr.top_container_id
        left join top_container_housed_at_rlshp tchar on tchar.top_container_id = tc.id
        left join location l on l.id = tchar.location_id 
        where tc.repo_id = #{db.literal(@repo_id)}
      SOME_SQL

        if @building.present?
        query += " AND l.building LIKE #{db.literal("%#{@building}%")}"
        end
        if @roomname.present?
            query += " AND l.room = #{db.literal(@roomname)}"
        end

        query += <<~SOME_SQL
        UNION ALL

        SELECT CONCAT('/repositories/', tc.repo_id, '/top_containers/', tc.id) AS uri
       , tc.barcode
       , tc.indicator
       , l.title
       , l.building
       , l.room
       , tc.created_by
       , tc.create_time
       , tc.last_modified_by
       , tc.user_mtime
        FROM sub_container sc
        RIGHT JOIN top_container_link_rlshp tclr ON tclr.sub_container_id = sc.id
        RIGHT JOIN top_container tc ON tc.id = tclr.top_container_id
        right join top_container_housed_at_rlshp tchar on tchar.top_container_id = tc.id
        right join location l on l.id = tchar.location_id 
        WHERE sc.id IS NULL
        AND tc.repo_id = #{db.literal(@repo_id)}
      SOME_SQL
      
      if @building.present?
        query += " AND l.building LIKE #{db.literal("%#{@building}%")}"
      end
      if @roomname.present?
        query += " AND l.room = #{db.literal(@roomname)}"
      end
      query
    end
  
    def page_break
      false
    end
  end
