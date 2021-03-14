module ReportManager

  @@registered_reports ||= {}

  ALLOWED_REPORT_FORMATS = ["json", "csv", "html", "pdf", "rtf", "xlsx"]

  def self.allowed_report_formats
    ALLOWED_REPORT_FORMATS
  end

  def self.register_report(report_class, opts)
    opts[:code] = report_class.code
    opts[:model] = report_class
    opts[:params] ||= []

    Log.warn("Report with code '#{opts[:code]}' already registered") if @@registered_reports.has_key?(opts[:code])

    @@registered_reports[opts[:code]] = opts
    #Hacky thing to remove some or all of the native reports from the interface via the plugin
    @@registered_reports.delete_if { |key, _| ['accession_receipt_report', 'accession_deaccessions_list_report', 'accession_inventory_report', 'accession_subjects_names_classifications_list_report', 'accession_unprocessed_report', 'assessment_list_report', 'assessment_rating_report', 'created_accessions_report', 'agent_list_report', 'accession_rights_transferred_report', 'accession_report', 'digital_object_file_versions_report', 'digital_object_list_table_report', 'location_holdings_report', 'location_report', 'resource_deaccessions_list_report', 'resource_instances_list_report', 'resource_locations_list_report', 'resource_restrictions_list_report', 'resources_list_report', 'shelflist_report', 'subject_list_report', 'user_groups_report'].include? key }
  end

end